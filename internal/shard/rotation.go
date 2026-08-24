package shard

import "metricstore/internal/model"

// ForceRotate seals the current active shard and opens the next window
// immediately. In-flight timestamps still route to their correct window.
func (m *Manager) ForceRotate() (*Shard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	last := m.shards[len(m.shards)-1]
	last.Seal()
	start := last.end
	sh := &Shard{
		id:    m.nextID,
		start: start,
		end:   start + m.window.Nanoseconds(),
		state: model.ShardActive,
	}
	m.nextID++
	m.shards = append(m.shards, sh)
	return sh, nil
}
