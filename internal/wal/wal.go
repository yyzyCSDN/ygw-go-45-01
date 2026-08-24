package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const recordSize = 32

// checkpointName holds the persisted replay watermark file name.
const checkpointName = "checkpoint"

// Record is one persisted mutation in the write-ahead log.
type Record struct {
	Seq      uint64
	SeriesID uint64
	TS       int64
	Value    float64
}

// Segment is a single append-only WAL file.
type Segment struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	first  uint64
	last   uint64
	size   int64
}

// WAL appends records to the current segment and truncates old ones once the
// memtable has been flushed past a sequence number.
type WAL struct {
	mu         sync.Mutex
	dir        string
	segments   map[int]*Segment
	nextSeq    uint64
	nextReplay uint64
	maxSize    int64
}

// Open creates or reopens the WAL directory.
func Open(dir string, maxSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, segments: make(map[int]*Segment), maxSize: maxSize}
	if err := w.loadSegments(); err != nil {
		return nil, err
	}
	return w, nil
}

func segmentIndex(name string) (int, error) {
	base := strings.TrimPrefix(name, "wal-")
	base = strings.TrimSuffix(base, ".seg")
	n, err := strconv.Atoi(base)
	if err != nil {
		return 0, fmt.Errorf("bad segment name %q: %w", name, err)
	}
	return n, nil
}

func (w *WAL) loadSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	names := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".seg") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		idx, err := segmentIndex(name)
		if err != nil {
			return err
		}
		seg, err := w.openSegment(filepath.Join(w.dir, name), idx)
		if err != nil {
			return err
		}
		w.segments[idx] = seg
		if seg.last >= w.nextSeq {
			w.nextSeq = seg.last + 1
		}
	}
	if err := w.loadCheckpoint(); err != nil {
		return err
	}
	if len(w.segments) == 0 {
		return w.rotate()
	}
	return nil
}

func (w *WAL) openSegment(path string, idx int) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	seg := &Segment{path: path, file: f, size: info.Size(), first: ^uint64(0)}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) < recordSize+crcSize {
			continue
		}
		if !verifyChecksum(line) {
			continue
		}
		seq := binary.LittleEndian.Uint64(line[0:8])
		if seq < seg.first {
			seg.first = seq
		}
		if seq > seg.last {
			seg.last = seq
		}
	}
	if err := scanner.Err(); err != nil {
		f.Close()
		return nil, err
	}
	if seg.first == ^uint64(0) {
		seg.first = 0
	}
	seg.writer = bufio.NewWriterSize(f, 64*1024)
	return seg, nil
}

func (w *WAL) rotate() error {
	idx := 0
	for i := range w.segments {
		if i > idx {
			idx = i
		}
	}
	idx++
	path := filepath.Join(w.dir, fmt.Sprintf("wal-%06d.seg", idx))
	seg, err := w.openSegment(path, idx)
	if err != nil {
		return err
	}
	w.segments[idx] = seg
	return nil
}

// Append writes a record to the WAL and returns its sequence number.
func (w *WAL) Append(rec Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rec.Seq = w.nextSeq
	seg := w.currentLocked()
	if seg.size >= w.maxSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
		seg = w.currentLocked()
	}
	if err := seg.write(rec); err != nil {
		return 0, err
	}
	w.nextSeq++
	w.nextReplay = rec.Seq + 1
	_ = w.persistReplayLocked()
	return rec.Seq, nil
}

func (w *WAL) currentLocked() *Segment {
	idx := 0
	for i := range w.segments {
		if i > idx {
			idx = i
		}
	}
	return w.segments[idx]
}

func (w *WAL) rotateLocked() error {
	if err := w.currentLocked().flush(); err != nil {
		return err
	}
	return w.rotate()
}

func encodeRecord(rec Record) []byte {
	buf := make([]byte, 0, recordSize+crcSize+1)
	buf = binary.LittleEndian.AppendUint64(buf, rec.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, rec.SeriesID)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.TS))
	buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(rec.Value))
	buf = appendChecksum(buf)
	buf = append(buf, '\n')
	return buf
}

func (s *Segment) write(rec Record) error {
	buf := encodeRecord(rec)
	if _, err := s.writer.Write(buf); err != nil {
		return err
	}
	s.size += int64(len(buf))
	if rec.Seq < s.first || s.first == 0 {
		s.first = rec.Seq
	}
	if rec.Seq > s.last {
		s.last = rec.Seq
	}
	return nil
}

func (s *Segment) flush() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Sync flushes the current segment to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLocked().flush()
}

// TruncateBefore removes every segment whose last sequence number is strictly
// smaller than seq, i.e. all records in those segments were flushed to the
// memtable/disk already.
func (w *WAL) TruncateBefore(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for idx, seg := range w.segments {
		if seg.last < seq {
			if err := seg.flush(); err != nil {
				return err
			}
			if err := seg.file.Close(); err != nil {
				return err
			}
			if err := os.Remove(seg.path); err != nil {
				return err
			}
			delete(w.segments, idx)
		}
	}
	if len(w.segments) == 0 {
		return w.rotate()
	}
	return nil
}

// Replay iterates over every record with Seq >= from in ascending order.
func (w *WAL) Replay(from uint64, fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	indices := make([]int, 0, len(w.segments))
	for idx := range w.segments {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	for _, idx := range indices {
		seg := w.segments[idx]
		if seg.last < from {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			return err
		}
		err = scanRecords(f, from, fn)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func scanRecords(r io.Reader, from uint64, fn func(Record) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) < recordSize+crcSize {
			continue
		}
		if !verifyChecksum(line) {
			continue
		}
		rec := Record{
			Seq:      binary.LittleEndian.Uint64(line[0:8]),
			SeriesID: binary.LittleEndian.Uint64(line[8:16]),
			TS:       int64(binary.LittleEndian.Uint64(line[16:24])),
			Value:    math.Float64frombits(binary.LittleEndian.Uint64(line[24:32])),
		}
		if rec.Seq < from {
			continue
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// NextSeq returns the next sequence number that will be assigned.
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// SetReplayStart records the first sequence number that must be replayed after
// a restart and persists it so the watermark survives crashes. Records with a
// smaller sequence were already flushed to blocks.
func (w *WAL) SetReplayStart(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq > w.nextReplay {
		w.nextReplay = seq
	}
	return w.persistReplayLocked()
}

// persistReplayLocked writes the replay watermark to the checkpoint file.
func (w *WAL) persistReplayLocked() error {
	path := filepath.Join(w.dir, checkpointName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(w.nextReplay, 10)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadCheckpoint reads the persisted replay watermark, if present.
func (w *WAL) loadCheckpoint() error {
	path := filepath.Join(w.dir, checkpointName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	value, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return err
	}
	w.nextReplay = value
	return nil
}

// ReplayStart returns the first sequence number that would be replayed.
func (w *WAL) ReplayStart() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextReplay
}

// ReplayUnflushed replays every record not yet covered by a flush checkpoint.
func (w *WAL) ReplayUnflushed(fn func(Record) error) error {
	return w.Replay(w.nextReplay, fn)
}

// TruncateTo physically removes every record with Seq <= seq from all
// segments. It is only safe to call after the affected records were flushed
// to storage blocks.
func (w *WAL) TruncateTo(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for idx, seg := range w.segments {
		if seg.last <= seq {
			if err := seg.flush(); err != nil {
				return err
			}
			if err := seg.file.Close(); err != nil {
				return err
			}
			if err := os.Remove(seg.path); err != nil {
				return err
			}
			delete(w.segments, idx)
		}
	}
	if len(w.segments) == 0 {
		return w.rotate()
	}
	cur := w.currentLocked()
	keep := make([]Record, 0)
	err := scanRecords(openOrFail(cur.path), seq+1, func(rec Record) error {
		keep = append(keep, rec)
		return nil
	})
	if err != nil {
		return err
	}
	tmp := cur.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, 64*1024)
	for _, rec := range keep {
		if _, err := writer.Write(encodeRecord(rec)); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := cur.flush(); err != nil {
		return err
	}
	if err := cur.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, cur.path); err != nil {
		return err
	}
	reopened, err := w.openSegment(cur.path, segmentIndexFromPath(cur.path))
	if err != nil {
		return err
	}
	w.segments[segmentIndexFromPath(cur.path)] = reopened
	return nil
}

// openOrFail opens a file for scanning, panicking only in the impossible case
// of a concurrently removed segment (TruncateTo runs under the WAL lock).
func openOrFail(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return f
}

func segmentIndexFromPath(path string) int {
	base := strings.TrimPrefix(filepath.Base(path), "wal-")
	base = strings.TrimSuffix(base, ".seg")
	n, err := strconv.Atoi(base)
	if err != nil {
		return 0
	}
	return n
}

// Close flushes and closes all segments.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var firstErr error
	for idx, seg := range w.segments {
		if err := seg.flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(w.segments, idx)
	}
	return firstErr
}

