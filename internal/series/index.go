package series

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"

	"metricstore/internal/model"
)

// Index keeps a sharded map of series meta keyed by their label key.
// The bucket table can be expanded at runtime; registration during expansion
// must remain visible on the new bucket table.
type Index struct {
	mu      sync.RWMutex
	buckets []map[string]*model.SeriesMeta
	nextID  uint64
}

// NewIndex creates an index with the given initial bucket count.
func NewIndex(initialBuckets int) *Index {
	if initialBuckets < 1 {
		initialBuckets = 1
	}
	buckets := make([]map[string]*model.SeriesMeta, initialBuckets)
	for i := range buckets {
		buckets[i] = make(map[string]*model.SeriesMeta)
	}
	return &Index{buckets: buckets, nextID: 1}
}

func (ix *Index) bucket(key string) int {
	return int(xxhash.Sum64String(key) % uint64(len(ix.buckets)))
}

// Register inserts a series unless a series with the same label key already
// exists, in which case the existing one is returned.
func (ix *Index) Register(meta model.SeriesMeta) (model.SeriesMeta, bool) {
	key := meta.LabelsKey()
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if existing, ok := ix.buckets[ix.bucket(key)][key]; ok {
		return *existing, false
	}
	meta.ID = ix.nextID
	ix.nextID++
	ix.buckets[ix.bucket(key)][key] = &meta
	return meta, true
}

// Lookup finds a series by name and labels.
func (ix *Index) Lookup(name string, labels map[string]string) (model.SeriesMeta, bool) {
	key := model.SeriesMeta{Name: name, Labels: labels}.LabelsKey()
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	meta, ok := ix.buckets[ix.bucket(key)][key]
	if !ok {
		return model.SeriesMeta{}, false
	}
	return *meta, true
}

// Len reports the total number of registered series.
func (ix *Index) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	total := 0
	for _, b := range ix.buckets {
		total += len(b)
	}
	return total
}

// Expand migrates the index to a larger bucket table. Existing entries are
// rehashed into the new table, so a series registered before the expansion —
// including one whose Register completed while it held the lock just before
// Expand acquired it — stays visible by label lookup on the new table.
func (ix *Index) Expand(newBuckets int) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if newBuckets <= len(ix.buckets) {
		return fmt.Errorf("expand requires more buckets than current %d", len(ix.buckets))
	}
	next := make([]map[string]*model.SeriesMeta, newBuckets)
	for i := range next {
		next[i] = make(map[string]*model.SeriesMeta)
	}
	// Swap the table in first so bucket() resolves against the new width, then
	// rehash every existing entry into its new bucket. Done under the write
	// lock, so concurrent Register/Lookup observe one consistent table.
	old := ix.buckets
	ix.buckets = next
	for _, b := range old {
		for key, meta := range b {
			ix.buckets[ix.bucket(key)][key] = meta
		}
	}
	return nil
}

// BucketCount exposes the current number of buckets (used by tests and probes).
func (ix *Index) BucketCount() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.buckets)
}

// MetaByID resolves a series by its numeric id, scanning the index when the
// id is not directly reachable.
func (ix *Index) MetaByID(id uint64) (model.SeriesMeta, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for _, b := range ix.buckets {
		for _, meta := range b {
			if meta.ID == id {
				return *meta, true
			}
		}
	}
	return model.SeriesMeta{}, false
}
