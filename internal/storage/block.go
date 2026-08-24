package storage

import (
	"fmt"
	"sync"

	"metricstore/internal/model"
)

// Block holds an immutable set of points for one shard. Readers acquire a
// reference before reading; reclaim only happens when the reference count
// reaches zero and the block is marked archived.
type Block struct {
	mu       sync.Mutex
	id       model.BlockID
	state    model.BlockState
	refs     int
	byID     map[uint64][]model.Point
	minTS    int64
	maxTS    int64
	archived bool
}

func newBlock(id model.BlockID, byID map[uint64][]model.Point, minTS, maxTS int64) *Block {
	// refs counts active reader references only; the store itself holds the
	// block by map membership and drops it during reclaim.
	return &Block{id: id, state: model.BlockMutable, refs: 0, byID: byID, minTS: minTS, maxTS: maxTS}
}

// ID returns the block identifier.
func (b *Block) ID() model.BlockID { return b.id }

// State returns the current block state.
func (b *Block) State() model.BlockState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Acquire adds a reader reference. It fails if the block is already archived
// and unreferenced.
func (b *Block) Acquire() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.archived {
		return fmt.Errorf("block %d already archived", b.id)
	}
	b.refs++
	return nil
}

// Release drops a reader reference.
func (b *Block) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.refs > 0 {
		b.refs--
	}
}

// Seal transitions the block to immutable.
func (b *Block) Seal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == model.BlockMutable {
		b.state = model.BlockImmutable
	}
}

// MarkCompacting marks the block as being downsampled.
func (b *Block) MarkCompacting() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == model.BlockImmutable {
		b.state = model.BlockCompacting
	}
}

// MarkArchived marks the block as deletable. Reclaim will only proceed once
// no reader references remain.
func (b *Block) MarkArchived() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.archived = true
}

// Reclaimable reports whether the block can be removed from the store.
func (b *Block) Reclaimable() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.archived && b.refs == 0
}

// Range returns points of a series within the range. The caller must hold a
// reference for the duration of the read.
func (b *Block) Range(seriesID uint64, rng model.QueryRange) []model.Point {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rangeLocked(seriesID, rng)
}

func (b *Block) rangeLocked(seriesID uint64, rng model.QueryRange) []model.Point {
	pts := b.byID[seriesID]
	lo := lowerBound(pts, rng.Start)
	hi := lowerBound(pts, rng.End)
	out := make([]model.Point, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, pts[i])
	}
	return out
}

// MinMax returns the time span covered by the block.
func (b *Block) MinMax() (int64, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.minTS, b.maxTS
}


// Raw returns a deep copy of all points (used by compaction).
func (b *Block) Raw() map[uint64][]model.Point {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[uint64][]model.Point, len(b.byID))
	for id, pts := range b.byID {
		copied := make([]model.Point, len(pts))
		copy(copied, pts)
		out[id] = copied
	}
	return out
}

func lowerBound(pts []model.Point, ts int64) int {
	lo, hi := 0, len(pts)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if pts[mid].TS < ts {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
