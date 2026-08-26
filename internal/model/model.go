package model

import (
	"sort"
	"strings"
	"time"
)

// Point is a single metric sample at a timestamp.
type Point struct {
	TS       int64
	Value    float64
	SeriesID uint64
}

// SeriesMeta describes one named metric series with its label set.
type SeriesMeta struct {
	ID     uint64
	Name   string
	Labels map[string]string
}

// LabelsKey returns a canonical, order-stable rendering of the label set so the
// same label combination always hashes to the same key.
func (s SeriesMeta) LabelsKey() string {
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(s.Name)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(s.Labels[k])
	}
	return b.String()
}

// ShardID identifies a time shard.
type ShardID uint32

// BlockID identifies a storage block within a shard.
type BlockID uint64

// RetentionPolicy controls how long raw data is kept.
type RetentionPolicy struct {
	MaxAge    time.Duration
	SweepStep time.Duration
}

// DownsamplePolicy controls aggregation of raw points into coarser points.
type DownsamplePolicy struct {
	Window time.Duration
}

// QueryRange is a half-open time range [Start, End).
type QueryRange struct {
	Start int64
	End   int64
}

// ShardState mirrors the shard state machine used by the shard package.
type ShardState int

const (
	ShardActive ShardState = iota
	ShardSealing
	ShardSealed
	ShardArchived
)

// BlockState mirrors the storage block state machine.
type BlockState int

const (
	BlockMutable BlockState = iota
	BlockImmutable
	BlockCompacting
	BlockArchived
)

// SweepResult reports how many blocks a retention sweep removed.
type SweepResult struct {
	Scanned int
	Removed int
}
