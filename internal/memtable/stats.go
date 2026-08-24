package memtable

// Stats describes the live memtable.
type Stats struct {
	Points int   `json:"points"`
	MinTS  int64 `json:"min_ts"`
	MaxTS  int64 `json:"max_ts"`
}

// Stats returns a snapshot of the live memtable.
func (m *MemTable) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Stats{Points: m.points, MinTS: m.current.minTS, MaxTS: m.current.maxTS}
}
