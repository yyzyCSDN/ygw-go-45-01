package wal

import "testing"

// TestRecoverKeepsUnflushedTail simulates a crash that happens after a batch of
// writes but before any flush: the process is killed without advancing the
// replay watermark or truncating the WAL. On reopen, every unflushed record
// must still be replayable.
//
// Before the fix, Append itself advanced nextReplay to "last seq + 1" and
// persisted it, so on reopen ReplayUnflushed started past the newest record
// and replayed nothing — the ~30s gap.
func TestRecoverKeepsUnflushedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	const N = 100
	for i := 0; i < N; i++ {
		if _, err := w.Append(Record{SeriesID: 1, TS: int64(i), Value: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Durably persist the buffered writes so the reopen can read them, then
	// "crash" by closing without flushing/truncating.
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if got := w2.ReplayStart(); got != 0 {
		t.Fatalf("ReplayStart=%d after crash, want 0 (no flush happened)", got)
	}
	var n int
	if err := w2.ReplayUnflushed(func(Record) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != N {
		t.Fatalf("replayed %d records after reopen, want %d (unflushed tail lost)", n, N)
	}
}

// TestWatermarkOnlyAdvancesAfterFlush ensures that merely appending records does
// not move the persisted replay watermark — only an explicit SetReplayStart
// (the flush path) may advance it.
func TestWatermarkOnlyAdvancesAfterFlush(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 5; i++ {
		if _, err := w.Append(Record{SeriesID: 1, TS: int64(i), Value: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := w.ReplayStart(); got != 0 {
		t.Fatalf("ReplayStart=%d after writes only, want 0 (write path must not advance watermark)", got)
	}
	if err := w.SetReplayStart(3); err != nil { // simulate flush covering seqs 0..2
		t.Fatal(err)
	}
	if got := w.ReplayStart(); got != 3 {
		t.Fatalf("ReplayStart=%d after SetReplayStart(3), want 3", got)
	}
	var n int
	if err := w.ReplayUnflushed(func(Record) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 2 { // seqs 3,4 remain
		t.Fatalf("replayed %d, want 2", n)
	}
}

// TestReplayKeepsBinaryPayloadBytes guards the WAL framing. The old line-based
// framing split a record whenever any payload or CRC byte equaled 0x0A, so
// records whose TS low byte or CRC happened to be '\n' were silently dropped on
// every replay. This drives several such values (TS=10, plus whatever CRCs land
// on 0x0A) and asserts none are lost across a close+reopen.
func TestReplayKeepsBinaryPayloadBytes(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	const N = 2000
	written := make(map[uint64]Record, N)
	for i := 0; i < N; i++ {
		rec := Record{SeriesID: uint64(i % 7), TS: int64(i), Value: float64(i) * 1.5}
		seq, err := w.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		rec.Seq = seq
		written[seq] = rec
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	seen := make(map[uint64]bool, N)
	if err := w2.ReplayUnflushed(func(r Record) error {
		seen[r.Seq] = true
		if want, ok := written[r.Seq]; ok {
			if r != want {
				t.Errorf("seq %d mismatch: got %+v want %+v", r.Seq, r, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != N {
		var missing []uint64
		for seq := range written {
			if !seen[seq] {
				missing = append(missing, seq)
			}
		}
		t.Fatalf("replayed %d of %d records; missing seqs %v (binary byte framing bug)", len(seen), N, missing[:min(len(missing), 16)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
