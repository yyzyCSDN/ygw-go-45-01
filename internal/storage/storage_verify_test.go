package storage_test

import (
	"testing"

	"metricstore/internal/memtable"
	"metricstore/internal/model"
)

func TestNoStaleReadAfterMemtableFlush(t *testing.T) {
	mt := memtable.New()
	mt.Write(model.Point{TS: 100, Value: 1.5, SeriesID: 7})
	snap := mt.Latest()
	defer snap.Release()
	if _, err := mt.Flush(); err != nil {
		t.Fatal(err)
	}
	pts := snap.Range(7, model.QueryRange{Start: 0, End: 200})
	if len(pts) != 1 {
		t.Fatalf("stale reader after flush got %d points, want 1", len(pts))
	}
}
