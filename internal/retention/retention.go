package retention

import (
	"sync"

	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
)

// Retention sweeps archived blocks that are fully outside the retention
// window. A block is only removed once no reader references it.
type Retention struct {
	mu     sync.Mutex
	store  *storage.Store
	shards *shard.Manager
	reg    *metric.Registry
	policy model.RetentionPolicy
}

// New creates the retention sweeper.
func New(store *storage.Store, shards *shard.Manager, reg *metric.Registry, policy model.RetentionPolicy) *Retention {
	return &Retention{store: store, shards: shards, reg: reg, policy: policy}
}

// Sweep removes data older than now-maxAge and returns how many blocks were
// removed. Blocks that still have active readers are left for a later sweep.
func (r *Retention) Sweep(now int64) model.SweepResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := r.Cutoff(now)
	result := model.SweepResult{}
	archived := 0
	for _, id := range r.store.List() {
		b, err := r.store.Open(id)
		if err != nil || b == nil {
			continue
		}
		_, maxTS := b.MinMax()
		r.store.Close(b)
		result.Scanned++
		if maxTS <= cutoff {
			r.store.MarkArchived(id)
			archived++
		}
	}
	result.Removed = r.store.Reclaim()
	r.shards.ArchiveOlderThan(cutoff)
	r.reg.Counter("retention_sweeps").Inc(1)
	r.reg.Gauge("blocks_retained").Add(int64(-result.Removed))
	return result
}

// Policy returns the retention policy.
func (r *Retention) Policy() model.RetentionPolicy {
	return r.policy
}
