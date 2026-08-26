package query

import (
	"fmt"
	"sort"
	"sync"

	"metricstore/internal/memtable"
	"metricstore/internal/model"
	"metricstore/internal/series"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
)

// AggFunc aggregates a set of points into one value.
type AggFunc int

const (
	AggAvg AggFunc = iota
	AggSum
	AggMin
	AggMax
	AggCount
)

// Result is one aggregated sample for a series.
type Result struct {
	TS       int64
	Value    float64
	SeriesID uint64
	Count    int
}

// Engine answers range queries by merging the live memtable with storage
// blocks, taking a consistent shard snapshot first.
type Engine struct {
	mu     sync.RWMutex
	mem    *memtable.MemTable
	shards *shard.Manager
	store  *storage.Store
	idx    *series.Index
}

// New creates a query engine.
func New(mem *memtable.MemTable, shards *shard.Manager, store *storage.Store, idx *series.Index) *Engine {
	return &Engine{mem: mem, shards: shards, store: store, idx: idx}
}

// QueryRange returns aggregated samples for one series in the range.
func (e *Engine) QueryRange(seriesID uint64, rng model.QueryRange, agg AggFunc, step int64) ([]Result, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	points := e.collect(seriesID, rng)
	return aggregate(points, agg, step), nil
}

func (e *Engine) collect(seriesID uint64, rng model.QueryRange) []model.Point {
	var points []model.Point
	points = append(points, e.mem.Read(seriesID, rng)...)
	shards := e.shards.ShardsCovering(rng)
	for _, sh := range shards {
		for _, bid := range sh.Blocks() {
			b, err := e.store.Open(bid)
			if err != nil || b == nil {
				continue
			}
			points = append(points, b.Range(seriesID, rng)...)
			e.store.Close(b)
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].TS != points[j].TS {
			return points[i].TS < points[j].TS
		}
		return points[i].SeriesID < points[j].SeriesID
	})
	return points
}

// SeriesID resolves a series by name and labels.
func (e *Engine) SeriesID(name string, labels map[string]string) (uint64, error) {
	meta, ok := e.idx.Lookup(name, labels)
	if !ok {
		return 0, fmt.Errorf("unknown series %q", name)
	}
	return meta.ID, nil
}

// Stats returns basic engine statistics.
func (e *Engine) Stats() map[string]int {
	return map[string]int{
		"shards": e.shards.Count(),
		"blocks": e.store.Count(),
		"series": e.idx.Len(),
		"buffer": e.mem.Len(),
	}
}

func aggregate(points []model.Point, agg AggFunc, step int64) []Result {
	if step <= 0 {
		step = 60_000_000_000
	}
	type acc struct {
		ts    int64
		sum   float64
		min   float64
		max   float64
		count int
		first bool
	}
	var windows []acc
	for _, p := range points {
		start := p.TS - p.TS%step
		if len(windows) == 0 || windows[len(windows)-1].ts != start {
			windows = append(windows, acc{ts: start, sum: p.Value, min: p.Value, max: p.Value, count: 1, first: false})
		} else {
			last := &windows[len(windows)-1]
			last.sum += p.Value
			if p.Value < last.min {
				last.min = p.Value
			}
			if p.Value > last.max {
				last.max = p.Value
			}
			last.count++
		}
	}
	out := make([]Result, 0, len(windows))
	for _, w := range windows {
		value := w.sum / float64(w.count)
		switch agg {
		case AggSum:
			value = w.sum
		case AggMin:
			value = w.min
		case AggMax:
			value = w.max
		case AggCount:
			value = float64(w.count)
		}
		out = append(out, Result{TS: w.ts, Value: value, Count: w.count})
	}
	if agg == AggDelta {
		values := make([]float64, len(out))
		for i, r := range out {
			values[i] = r.Value
		}
		deltas := deltaOf(values)
		for i := range out {
			out[i].Value = deltas[i]
		}
	}
	return out
}
