package shard

import (
	"fmt"
	"sync"
	"time"

	"metricstore/internal/model"
)

// Shard groups one time window of data.
type Shard struct {
	mu     sync.Mutex
	id     model.ShardID
	start  int64
	end    int64
	state  model.ShardState
	blocks []model.BlockID
}

// ID returns the shard id.
func (s *Shard) ID() model.ShardID { return s.id }

// Start returns the shard start timestamp.
func (s *Shard) Start() int64 { return s.start }

// End returns the shard end timestamp.
func (s *Shard) End() int64 { return s.end }

// State returns the current state.
func (s *Shard) State() model.ShardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Seal moves the shard from active to sealing then sealed.
func (s *Shard) Seal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ShardActive {
		s.state = model.ShardSealing
	}
	if s.state == model.ShardSealing {
		s.state = model.ShardSealed
	}
}

// Archive marks the shard archived.
func (s *Shard) Archive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ShardSealed {
		s.state = model.ShardArchived
	}
}

// AddBlock records a storage block belonging to this shard.
func (s *Shard) AddBlock(id model.BlockID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = append(s.blocks, id)
}

// Blocks returns a copy of the block ids owned by the shard.
func (s *Shard) Blocks() []model.BlockID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.BlockID, len(s.blocks))
	copy(out, s.blocks)
	return out
}


// Manager owns the set of shards and handles rotation.
type Manager struct {
	mu     sync.Mutex
	window time.Duration
	shards []*Shard
	nextID model.ShardID
	now    func() time.Time
}

// NewManager creates a shard manager with a fixed time window.
func NewManager(window time.Duration, now func() time.Time) (*Manager, error) {
	if window <= 0 {
		return nil, fmt.Errorf("shard window must be positive")
	}
	if now == nil {
		now = time.Now
	}
	m := &Manager{window: window, now: now}
	start := m.aligned(now().UnixNano())
	m.shards = append(m.shards, &Shard{
		id:    1,
		start: start,
		end:   start + window.Nanoseconds(),
		state: model.ShardActive,
	})
	m.nextID = 2
	return m, nil
}

func (m *Manager) aligned(ts int64) int64 {
	step := m.window.Nanoseconds()
	return ts - ts%step
}

func (m *Manager) shardID(ts int64) model.ShardID {
	return model.ShardID(ts/m.window.Nanoseconds() + 1)
}

// Route returns the active shard covering ts, rotating if ts falls past the
// current shard's window.
func (m *Manager) Route(ts int64) (*Shard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	last := m.shards[len(m.shards)-1]
	if ts >= last.start && ts < last.end {
		return last, nil
	}
	if ts < last.start {
		return nil, fmt.Errorf("timestamp %d is before active shard %d", ts, last.start)
	}
	start := m.aligned(ts)
	sh := &Shard{
		id:    m.nextID,
		start: start,
		end:   start + m.window.Nanoseconds(),
		state: model.ShardActive,
	}
	m.nextID++
	// Seal all shards before the new one.
	for _, s := range m.shards {
		if s.end <= start {
			s.Seal()
		}
	}
	m.shards = append(m.shards, sh)
	return sh, nil
}

// Active returns the newest shard.
func (m *Manager) Active() *Shard {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shards[len(m.shards)-1]
}

// Snapshot returns a consistent copy of the shard list at a point in time.
func (m *Manager) Snapshot() []*Shard {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Shard, len(m.shards))
	copy(out, m.shards)
	return out
}

// ShardsCovering returns every shard whose window overlaps the range.
func (m *Manager) ShardsCovering(rng model.QueryRange) []*Shard {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Shard, 0)
	for _, s := range m.shards {
		if s.start < rng.End && s.end > rng.Start {
			out = append(out, s)
		}
	}
	return out
}

// ArchiveOlderThan archives sealed shards whose end is before the given time.
func (m *Manager) ArchiveOlderThan(ts int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.shards {
		if s.end <= ts && s.State() == model.ShardSealed {
			s.Archive()
			n++
		}
	}
	return n
}

// Count returns the number of shards.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.shards)
}
