package memtable

import (
	"sort"
	"sync"

	"metricstore/internal/model"
)

// Snapshot is an immutable copy of the memtable contents. Readers hold a
// reference via Acquire/Release so the underlying slices stay alive even after
// the live table moves on to a newer snapshot.
type Snapshot struct {
	mu    sync.Mutex
	refs  int
	byID  map[uint64][]model.Point
	minTS int64
	maxTS int64
}

// Acquire increments the reference count of the snapshot.
func (s *Snapshot) Acquire() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
}

// Release decrements the reference count.
func (s *Snapshot) Release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	s.mu.Unlock()
}

// Range returns the points of one series within the given time range.
func (s *Snapshot) Range(seriesID uint64, rng model.QueryRange) []model.Point {
	if s == nil {
		return nil
	}
	pts := s.byID[seriesID]
	lo := sort.Search(len(pts), func(i int) bool { return pts[i].TS >= rng.Start })
	hi := sort.Search(len(pts), func(i int) bool { return pts[i].TS >= rng.End })
	out := make([]model.Point, 0, hi-lo)
	out = append(out, pts[lo:hi]...)
	return out
}

// MinMax returns the covered time range.
func (s *Snapshot) MinMax() (int64, int64) {
	if s == nil {
		return 0, 0
	}
	return s.minTS, s.maxTS
}

// RawCopy returns a deep copy of the snapshot contents for hand-off into a
// storage block.
func (s *Snapshot) RawCopy() map[uint64][]model.Point {
	if s == nil {
		return nil
	}
	out := make(map[uint64][]model.Point, len(s.byID))
	for id, pts := range s.byID {
		copied := make([]model.Point, len(pts))
		copy(copied, pts)
		out[id] = copied
	}
	return out
}

// MemTable buffers recent writes before they are flushed into storage blocks.
// Reads go through Latest() which pins the current snapshot for the duration
// of the read.
type MemTable struct {
	mu      sync.RWMutex
	current *Snapshot
	points  int
}

// New creates an empty memtable.
func New() *MemTable {
	return &MemTable{
		current: &Snapshot{refs: 1, byID: make(map[uint64][]model.Point)},
	}
}

// Write inserts a point keeping per-series order by timestamp.
func (m *MemTable) Write(p model.Point) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pts := m.current.byID[p.SeriesID]
	idx := sort.Search(len(pts), func(i int) bool { return pts[i].TS >= p.TS })
	if idx < len(pts) && pts[idx].TS == p.TS {
		pts[idx].Value = p.Value
	} else {
		pts = append(pts, model.Point{})
		copy(pts[idx+1:], pts[idx:])
		pts[idx] = p
		m.current.byID[p.SeriesID] = pts
	}
	if m.points == 0 || p.TS < m.current.minTS {
		m.current.minTS = p.TS
	}
	if m.points == 0 || p.TS > m.current.maxTS {
		m.current.maxTS = p.TS
	}
	m.points++
}

// Latest pins the current snapshot. The caller must call Release.
func (m *MemTable) Latest() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.current.Acquire()
	return m.current
}

// Flush swaps in a fresh snapshot and returns the old one for hand-off into a
// storage block. The caller must Release the returned snapshot.
func (m *MemTable) Flush() (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.current
	m.current = &Snapshot{refs: 1, byID: make(map[uint64][]model.Point)}
	old.byID = nil
	m.points = 0
	return old, nil
}

// Len returns the number of buffered points.
func (m *MemTable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.points
}

// Read returns points of a series inside the range by pinning the live
// snapshot for the duration of the read.
func (m *MemTable) Read(seriesID uint64, rng model.QueryRange) []model.Point {
	snap := m.Latest()
	defer snap.Release()
	return snap.Range(seriesID, rng)
}
