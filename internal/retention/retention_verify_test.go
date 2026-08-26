package retention_test

import (
	"testing"
	"time"

	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/retention"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
)

func TestRetentionKeepsBoundaryWrites(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	shards, err := shard.NewManager(time.Hour, func() time.Time { return time.Unix(0, start) })
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore()
	block, err := store.Create(map[uint64][]model.Point{
		1: {
			{TS: start, Value: 1, SeriesID: 1},
			{TS: start + 10, Value: 2, SeriesID: 1},
		},
	}, start, start+10)
	if err != nil {
		t.Fatal(err)
	}
	block.Seal()
	sh, _ := shards.Route(start)
	sh.AddBlock(block.ID())
	rt := retention.New(store, shards, metric.New(), model.RetentionPolicy{MaxAge: 0, SweepStep: time.Minute})
	rt.Sweep(start + 5)
	if store.Count() != 1 {
		t.Fatalf("boundary writes were removed by retention sweep")
	}
	b, err := store.Open(block.ID())
	if err != nil || b == nil {
		t.Fatalf("boundary block missing after sweep")
	}
	store.Close(b)
}
