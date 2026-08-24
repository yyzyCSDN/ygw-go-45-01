package shard

import "metricstore/internal/model"

// Stats is a snapshot of the shard manager.
type Stats struct {
	Shards   int `json:"shards"`
	Active   int `json:"active"`
	Sealed   int `json:"sealed"`
	Archived int `json:"archived"`
}

// Stats reports the number of shards in each state.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s Stats
	s.Shards = len(m.shards)
	for _, sh := range m.shards {
		switch sh.State() {
		case model.ShardActive:
			s.Active++
		case model.ShardSealed:
			s.Sealed++
		case model.ShardArchived:
			s.Archived++
		}
	}
	return s
}
