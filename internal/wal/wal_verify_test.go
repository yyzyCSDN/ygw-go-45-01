package wal_test

import (
	"path/filepath"
	"testing"

	"metricstore/internal/wal"
)

func TestWALReplayAfterFlushConsistent(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	seq0, err := w.Append(wal.Record{SeriesID: 1, TS: 1, Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	seq1, err := w.Append(wal.Record{SeriesID: 1, TS: 2, Value: 2})
	if err != nil {
		t.Fatal(err)
	}
	w.SetReplayStart(seq1 + 1)
	seq2, err := w.Append(wal.Record{SeriesID: 1, TS: 3, Value: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var got []wal.Record
	if err := w2.ReplayUnflushed(func(rec wal.Record) error {
		got = append(got, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Seq != seq2 {
		t.Fatalf("replay after flush inconsistent: %+v", got)
	}
	_ = seq0
}
