package series

import (
	"fmt"
	"sync"
	"testing"

	"metricstore/internal/model"
)

func TestExpandPreservesExistingSeries(t *testing.T) {
	ix := NewIndex(4)
	original, created := ix.Register(model.SeriesMeta{Name: "cpu.usage", Labels: map[string]string{"host": "node-1"}})
	if !created {
		t.Fatal("expected first register to create the series")
	}

	if err := ix.Expand(32); err != nil {
		t.Fatalf("expand: %v", err)
	}

	if ix.BucketCount() != 32 {
		t.Fatalf("bucket count = %d, want 32", ix.BucketCount())
	}

	got, ok := ix.Lookup("cpu.usage", map[string]string{"host": "node-1"})
	if !ok {
		t.Fatal("series became invisible after expand")
	}
	if got.ID != original.ID {
		t.Fatalf("id changed after expand: got %d want %d", got.ID, original.ID)
	}
	if ix.Len() != 1 {
		t.Fatalf("len = %d, want 1", ix.Len())
	}
}

func TestExpandRedistributesAcrossBuckets(t *testing.T) {
	ix := NewIndex(2)
	for i := 0; i < 200; i++ {
		ix.Register(model.SeriesMeta{
			Name:   "cpu.usage",
			Labels: map[string]string{"host": fmt.Sprintf("node-%d", i)},
		})
	}
	before := ix.Len()

	if err := ix.Expand(64); err != nil {
		t.Fatalf("expand: %v", err)
	}

	if ix.Len() != before {
		t.Fatalf("series count changed across expand: got %d want %d", ix.Len(), before)
	}
	// Every previously registered series must still resolve.
	for i := 0; i < 200; i++ {
		if _, ok := ix.Lookup("cpu.usage", map[string]string{"host": fmt.Sprintf("node-%d", i)}); !ok {
			t.Fatalf("series node-%d lost after expand", i)
		}
	}
}

// TestRegisterConcurrentWithExpand reproduces the reported outage: a register
// racing with expand must not end up lost in the discarded old table.
func TestRegisterConcurrentWithExpand(t *testing.T) {
	ix := NewIndex(8)
	for i := 0; i < 100; i++ {
		ix.Register(model.SeriesMeta{
			Name:   "cpu.usage",
			Labels: map[string]string{"host": fmt.Sprintf("seed-%d", i)},
		})
	}

	var wg sync.WaitGroup
	register := func(n int) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			ix.Register(model.SeriesMeta{
				Name:   "cpu.usage",
				Labels: map[string]string{"host": fmt.Sprintf("r%d", i)},
			})
		}
	}
	wg.Add(3)
	go register(500)
	go register(500)
	go func() {
		defer wg.Done()
		_ = ix.Expand(256)
	}()
	wg.Wait()

	// After expansion every series registered so far must be lookupable.
	for i := 0; i < 500; i++ {
		if _, ok := ix.Lookup("cpu.usage", map[string]string{"host": fmt.Sprintf("r%d", i)}); !ok {
			t.Fatalf("r%d lost after expand", i)
		}
	}
}
