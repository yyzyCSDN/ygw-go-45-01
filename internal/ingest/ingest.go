package ingest

import (
	"errors"
	"fmt"
	"sync"

	"metricstore/internal/memtable"
	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/series"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
	"metricstore/internal/wal"
)

// ErrBackpressure is returned when the write pipeline is saturated.
var ErrBackpressure = errors.New("ingest backpressure")

// Ingest is the write path: series registration, WAL, memtable, shard routing
// and periodic flush into storage blocks.
type Ingest struct {
	mu             sync.Mutex
	idx            *series.Index
	wal            *wal.WAL
	mem            *memtable.MemTable
	shards         *shard.Manager
	store          *storage.Store
	reg            *metric.Registry
	flushThreshold int
	lastFlushSeq   uint64
	slots          chan struct{}
}

// Options configures the ingest pipeline.
type Options struct {
	Index          *series.Index
	WAL            *wal.WAL
	Shards         *shard.Manager
	Store          *storage.Store
	Metrics        *metric.Registry
	FlushThreshold int
	Concurrency    int
}

// New creates the ingest pipeline.
func New(opts Options) (*Ingest, error) {
	if opts.Index == nil || opts.WAL == nil || opts.Shards == nil || opts.Store == nil {
		return nil, errors.New("ingest requires index, wal, shards and store")
	}
	if opts.Metrics == nil {
		opts.Metrics = metric.New()
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = 4096
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 16
	}
	in := &Ingest{
		idx:            opts.Index,
		wal:            opts.WAL,
		mem:            memtable.New(),
		shards:         opts.Shards,
		store:          opts.Store,
		reg:            opts.Metrics,
		flushThreshold: opts.FlushThreshold,
		slots:          make(chan struct{}, opts.Concurrency),
	}
	return in, nil
}

// SeriesOf resolves or registers a series by name and labels.
func (in *Ingest) SeriesOf(name string, labels map[string]string) (model.SeriesMeta, error) {
	if !series.ValidName(name) {
		return model.SeriesMeta{}, errors.New("invalid metric name")
	}
	clean, err := series.SanitizeLabels(labels)
	if err != nil {
		return model.SeriesMeta{}, err
	}
	labels = clean
	meta, ok := in.idx.Lookup(name, labels)
	if !ok {
		registered, created := in.idx.Register(model.SeriesMeta{Name: name, Labels: labels})
		meta = registered
		if created {
			in.reg.Counter("series_registered").Inc(1)
		}
	}
	return meta, nil
}

// Append writes one sample synchronously. When the pipeline is saturated the
// call blocks until a slot frees up.
func (in *Ingest) Append(name string, labels map[string]string, ts int64, value float64) error {
	in.slots <- struct{}{}
	defer func() { <-in.slots }()
	meta, err := in.SeriesOf(name, labels)
	if err != nil {
		return err
	}
	rec := wal.Record{SeriesID: meta.ID, TS: ts, Value: value}
	seq, err := in.wal.Append(rec)
	if err != nil {
		return err
	}
	if err := in.wal.TruncateTo(seq); err != nil {
		return err
	}
	pt := model.Point{TS: ts, Value: value, SeriesID: meta.ID}
	in.mu.Lock()
	defer in.mu.Unlock()
	if _, err := in.shards.Route(ts); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	in.mem.Write(pt)
	in.reg.Counter("points_ingested").Inc(1)
	if in.mem.Len() >= in.flushThreshold {
		return in.flushLocked(seq)
	}
	return nil
}

// Flush seals the current memtable into a storage block and advances the WAL
// truncation point.
func (in *Ingest) Flush() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	lastSeq := in.wal.NextSeq()
	if lastSeq == 0 {
		return nil
	}
	return in.flushLocked(lastSeq - 1)
}

func (in *Ingest) flushLocked(seq uint64) error {
	if in.mem.Len() == 0 {
		return nil
	}
	snap, err := in.mem.Flush()
	if err != nil {
		return err
	}
	defer snap.Release()
	groups := in.groupByShard(snap.RawCopy())
	for _, group := range groups {
		minTS, maxTS := groupMinMax(group)
		block, err := in.store.Create(group, minTS, maxTS)
		if err != nil {
			return err
		}
		block.Seal()
		sh, err := in.shards.Route(minTS)
		if err != nil {
			return err
		}
		sh.AddBlock(block.ID())
	}
	in.lastFlushSeq = seq
	in.reg.Counter("blocks_flushed").Inc(int64(len(groups)))
	if err := in.wal.SetReplayStart(seq + 1); err != nil {
		return err
	}
	return in.wal.TruncateTo(seq)
}

// groupByShard splits a snapshot into one point set per time shard so a block
// is always owned by the shard covering its data.
func (in *Ingest) groupByShard(byID map[uint64][]model.Point) []map[uint64][]model.Point {
	type groupKey struct {
		start int64
		end   int64
	}
	order := make([]groupKey, 0)
	groups := make(map[groupKey]map[uint64][]model.Point)
	for seriesID, pts := range byID {
		for _, pt := range pts {
			sh, err := in.shards.Route(pt.TS)
			if err != nil {
				continue
			}
			key := groupKey{start: sh.Start(), end: sh.End()}
			g, ok := groups[key]
			if !ok {
				g = make(map[uint64][]model.Point)
				groups[key] = g
				order = append(order, key)
			}
			g[seriesID] = append(g[seriesID], pt)
		}
	}
	out := make([]map[uint64][]model.Point, 0, len(order))
	for _, key := range order {
		out = append(out, groups[key])
	}
	return out
}

func groupMinMax(group map[uint64][]model.Point) (int64, int64) {
	var minTS, maxTS int64
	first := true
	for _, pts := range group {
		for _, pt := range pts {
			if first || pt.TS < minTS {
				minTS = pt.TS
			}
			if first || pt.TS > maxTS {
				maxTS = pt.TS
			}
			first = false
		}
	}
	return minTS, maxTS
}

// FlushedSeq returns the last sequence number flushed to disk.
func (in *Ingest) FlushedSeq() uint64 {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.lastFlushSeq
}

// BufferLen returns the current memtable size.
func (in *Ingest) BufferLen() int {
	return in.mem.Len()
}

// Mem exposes the live memtable for query reads.
func (in *Ingest) Mem() *memtable.MemTable { return in.mem }

// Index exposes the series index (read-only use).
func (in *Ingest) Index() *series.Index { return in.idx }

// Store exposes the storage store.
func (in *Ingest) Store() *storage.Store { return in.store }

// Shards exposes the shard manager.
func (in *Ingest) Shards() *shard.Manager { return in.shards }

// WAL exposes the write-ahead log.
func (in *Ingest) WAL() *wal.WAL { return in.wal }

// Metrics exposes the metric registry.
func (in *Ingest) Metrics() *metric.Registry { return in.reg }


// Recover replays the WAL into the live memtable after startup so acknowledged
// writes are not lost across restarts.
func (in *Ingest) Recover() error {
	err := in.wal.ReplayUnflushed(func(rec wal.Record) error {
		in.mu.Lock()
		defer in.mu.Unlock()
		in.mem.Write(model.Point{TS: rec.TS, Value: rec.Value, SeriesID: rec.SeriesID})
		in.reg.Counter("points_recovered").Inc(1)
		return nil
	})
	if err != nil {
		return err
	}
	return in.Flush()
}
