package compact

import (
	"sync"

	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
)

// Compactor downsamples sealed blocks into coarser blocks. It holds a
// reference on the source block for the whole run so a concurrent retention
// sweep cannot reclaim it mid-flight.
type Compactor struct {
	mu     sync.Mutex
	store  *storage.Store
	shards *shard.Manager
	reg    *metric.Registry
	policy model.DownsamplePolicy
}

// New creates a compactor.
func New(store *storage.Store, shards *shard.Manager, reg *metric.Registry, policy model.DownsamplePolicy) *Compactor {
	return &Compactor{store: store, shards: shards, reg: reg, policy: policy}
}

// Compact processes one sealed block. It returns the id of the produced block
// or (0, nil) when the block produced no samples.
func (c *Compactor) Compact(id model.BlockID) (model.BlockID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	src, err := c.store.Open(id)
	if err != nil || src == nil {
		return 0, err
	}
	defer c.store.Close(src)
	raw := src.Raw()
	minTS, maxTS := src.MinMax()
	step := c.policy.Window.Nanoseconds()
	if step <= 0 {
		step = 60_000_000_000 // 1 minute default
	}
	byID := make(map[uint64][]model.Point)
	var outMin, outMax int64
	first := true
	for seriesID, pts := range raw {
		agg := aggregateWindow(pts, step)
		if len(agg) == 0 {
			continue
		}
		byID[seriesID] = agg
		for _, p := range agg {
			if first || p.TS < outMin {
				outMin = p.TS
			}
			if first || p.TS > outMax {
				outMax = p.TS
			}
			first = false
		}
	}
	src.MarkCompacting()
	if len(byID) == 0 {
		src.MarkArchived()
		c.store.Reclaim()
		c.reg.Counter("blocks_compacted").Inc(1)
		return 0, nil
	}
	block, err := c.store.Create(byID, outMin, outMax)
	if err != nil {
		return 0, err
	}
	block.Seal()
	sh, err := c.shards.Route(minTS)
	if err != nil {
		return 0, err
	}
	sh.AddBlock(block.ID())
	src.MarkArchived()
	c.store.Reclaim()
	c.reg.Counter("blocks_compacted").Inc(1)
	_ = maxTS
	return block.ID(), nil
}

// aggregateWindow buckets points by window and keeps the window boundary
// inclusive on the left: the point exactly at the window start is included.
func aggregateWindow(pts []model.Point, step int64) []model.Point {
	if len(pts) == 0 {
		return nil
	}
	type acc struct {
		ts    int64
		sum   float64
		count int
	}
	var windows []acc
	for i := 0; i < len(pts)-1; i++ {
		p := pts[i]
		start := p.TS - p.TS%step
		if len(windows) == 0 || windows[len(windows)-1].ts != start {
			windows = append(windows, acc{ts: start, sum: p.Value, count: 1})
		} else {
			last := &windows[len(windows)-1]
			last.sum += p.Value
			last.count++
		}
	}
	out := make([]model.Point, 0, len(windows))
	for _, w := range windows {
		out = append(out, model.Point{TS: w.ts, Value: w.sum / float64(w.count), SeriesID: pts[0].SeriesID})
	}
	return out
}

// CompactAll compacts every immutable, non-compacting block.
func (c *Compactor) CompactAll() (int, error) {
	ids := c.store.List()
	done := 0
	for _, id := range ids {
		b, err := c.store.Open(id)
		if err != nil || b == nil {
			continue
		}
		st := b.State()
		c.store.Close(b)
		if st != model.BlockImmutable {
			continue
		}
		if _, err := c.Compact(id); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// Policy returns the active downsample policy.
func (c *Compactor) Policy() model.DownsamplePolicy {
	return c.policy
}

