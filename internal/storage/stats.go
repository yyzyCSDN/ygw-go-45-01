package storage

// Stats is a snapshot of store counters.
type Stats struct {
	Blocks  int `json:"blocks"`
	Removed int `json:"removed"`
}

// Stats returns a consistent snapshot of store counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Blocks: len(s.blocks), Removed: s.removed}
}
