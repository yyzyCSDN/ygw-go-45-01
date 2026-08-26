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

func TestDownsampleSurvivesRetentionSweep(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	shards, err := shard.NewManager(time.Hour, func() time.Time { return time.Unix(0, start) })
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore()
	block, err := store.Create(map[uint64][]model.Point{
		1: {
			{TS: start, Value: 1, SeriesID: 1},
			{TS: start + int64(30*time.Second), Value: 2, SeriesID: 1},
			{TS: start + int64(time.Minute), Value: 3, SeriesID: 1},
		},
	}, start, start+int64(time.Minute))
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
		t.Fatal("compaction produced no output; source block was lost")
	}
}
