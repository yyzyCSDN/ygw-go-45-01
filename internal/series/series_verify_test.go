package series_test

import (
	"testing"

	"metricstore/internal/model"
	"metricstore/internal/series"
)

func TestIndexExpansionKeepsSeriesVisible(t *testing.T) {
	ix := series.NewIndex(2)
	meta, created := ix.Register(model.SeriesMeta{Name: "cpu", Labels: map[string]string{"host": "a"}})
	if !created {
		t.Fatal("expected a new registration")
	}
	if err := ix.Expand(8); err != nil {
		t.Fatal(err)
	}
	found, ok := ix.Lookup("cpu", map[string]string{"host": "a"})
	if !ok {
		t.Fatalf("series %d lost after index expansion", meta.ID)
	}
	if found.ID != meta.ID {
		t.Fatalf("series id changed after expansion: %d != %d", found.ID, meta.ID)
	}
}
