package storage

import (
	"sync"

	"metricstore/internal/model"
)

// Store owns all sealed blocks and hands out referenced readers.
type Store struct {
	mu      sync.Mutex
	blocks  map[model.BlockID]*Block
	nextID  model.BlockID
	removed int
}

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{blocks: make(map[model.BlockID]*Block)}
}

// Create registers a new immutable block from a flushed snapshot.
func (s *Store) Create(byID map[uint64][]model.Point, minTS, maxTS int64) (*Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	b := newBlock(s.nextID, byID, minTS, maxTS)
	b.state = model.BlockImmutable
	s.blocks[b.id] = b
	return b, nil
}

// Open returns a referenced handle to a block, or nil if absent.
func (s *Store) Open(id model.BlockID) (*Block, error) {
	s.mu.Lock()
	b, ok := s.blocks[id]
	if !ok {
		s.mu.Unlock()
		return nil, nil
	}
	if err := b.Acquire(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	return b, nil
}

// Close drops the store's own reference to a block (call after Open).
func (s *Store) Close(b *Block) {
	b.Release()
}

// MarkArchived flags a block for reclaim.
func (s *Store) MarkArchived(id model.BlockID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.blocks[id]; ok {
		b.MarkArchived()
	}
}

// Reclaim removes every archived block with no active readers and returns the
// number removed.
func (s *Store) Reclaim() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, b := range s.blocks {
		if b.Reclaimable() {
			delete(s.blocks, id)
			removed++
			s.removed++
		}
	}
	return removed
}

// Count returns the number of blocks currently held.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.blocks)
}

// Removed returns the cumulative number of reclaimed blocks.
func (s *Store) Removed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removed
}

// List returns ids of all held blocks.
func (s *Store) List() []model.BlockID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]model.BlockID, 0, len(s.blocks))
	for id := range s.blocks {
		ids = append(ids, id)
	}
	return ids
}
