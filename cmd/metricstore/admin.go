package main

import (
	"fmt"
	"net/http"
	"time"
)

// handleAdminCompact triggers an immediate compaction pass.
func (s *server) handleAdminCompact(w http.ResponseWriter, r *http.Request) {
	done, err := s.compact.CompactAll()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, `{"compacted":%d,"eligible":%d}`, done, len(s.compact.Eligible()))
}

// handleAdminSweep triggers an immediate retention sweep.
func (s *server) handleAdminSweep(w http.ResponseWriter, r *http.Request) {
	res := s.retention.Sweep(time.Now().UnixNano())
	fmt.Fprintf(w, `{"scanned":%d,"removed":%d,"step_ns":%d}`, res.Scanned, res.Removed, s.retention.Step())
}

// handleAdminRotate forces an immediate shard rotation.
func (s *server) handleAdminRotate(w http.ResponseWriter, r *http.Request) {
	sh, err := s.shards.ForceRotate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, `{"shard_id":%d,"start":%d,"end":%d}`, sh.ID(), sh.Start(), sh.End())
}
