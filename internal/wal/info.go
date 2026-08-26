package wal

// Info describes the current WAL layout.
type Info struct {
	Segments int    `json:"segments"`
	NextSeq  uint64 `json:"next_seq"`
	Size     int64  `json:"size"`
}

// Info returns a snapshot of the WAL layout.
func (w *WAL) Info() Info {
	w.mu.Lock()
	defer w.mu.Unlock()
	info := Info{NextSeq: w.nextSeq}
	for _, seg := range w.segments {
		info.Segments++
		info.Size += seg.size
	}
	return info
}
