package compact_test

import (
	"testing"
	"time"

	"metricstore/internal/compact"
	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
)

func TestDownsampleKeepsBoundaryPoint(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	step := int64(time.Minute)
	shards, err := shard.NewManager(time.Hour, func() time.Time { return time.Unix(0, start) })
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore()
	block, err := store.Create(map[uint64][]model.Point{
		1: {
			{TS: start, Value: 1, SeriesID: 1},
			{TS: start + int64(30*time.Second), Value: 2, SeriesID: 1},
			{TS: start + step, Value: 3, SeriesID: 1},
		},
	}, start, start+step)
	if err != nil {
		t.Fatal(err)
	}
	block.Seal()
	sh, _ := shards.Route(start)
	sh.AddBlock(block.ID())
	cp := compact.New(store, shards, metric.New(), model.DownsamplePolicy{Window: time.Minute})
	outID, err := cp.Compact(block.ID())
	if err != nil {
		t.Fatal(err)
	}
	if outID == 0 {
		t.Fatal("compaction produced no block")
	}
	out, err := store.Open(outID)
	if err != nil || out == nil {
		t.Fatal("compacted block missing")
	}
	defer store.Close(out)
	found := false
	for _, p := range out.Range(1, model.QueryRange{Start: start, End: start + 2*step}) {
		if p.TS == start+step {
			found = true
		}
	}
	if !found {
		t.Fatalf("boundary point at %d was dropped by downsampling", start+step)
	}
}
