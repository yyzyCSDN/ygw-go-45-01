package ingest_test

import (
	"path/filepath"
	"testing"
	"time"

	"metricstore/internal/ingest"
	"metricstore/internal/metric"
	"metricstore/internal/series"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
	"metricstore/internal/wal"
)

func TestWALTruncateKeepsIngestedRanges(t *testing.T) {
	dir := t.TempDir()
	idx := series.NewIndex(4)
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	shards, err := shard.NewManager(time.Hour, func() time.Time { return time.Unix(0, start) })
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore()
	in, err := ingest.New(ingest.Options{
		Index:          idx,
		WAL:            w,
		Shards:         shards,
		Store:          store,
		Metrics:        metric.New(),
		FlushThreshold: 100000,
		Concurrency:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := in.Append("cpu", nil, start+i, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	count := 0
	if err := w2.ReplayUnflushed(func(rec wal.Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("WAL lost ingested ranges: replayed %d records, want 3", count)
	}
}
