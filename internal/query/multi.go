package query

import "metricstore/internal/model"

// MultiResult is a series-tagged aggregation result.
type MultiResult struct {
	SeriesID uint64   `json:"series_id"`
	Name     string   `json:"name"`
	Results  []Result `json:"results"`
}

// QueryMulti aggregates several series in one pass over the same shard
// snapshot, avoiding repeated snapshot acquisition.
func (e *Engine) QueryMulti(seriesIDs []uint64, rng model.QueryRange, agg AggFunc, step int64) ([]MultiResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	pointsBySeries := make(map[uint64][]model.Point, len(seriesIDs))
	for _, id := range seriesIDs {
		pointsBySeries[id] = e.collect(id, rng)
	}
	out := make([]MultiResult, 0, len(seriesIDs))
	for _, id := range seriesIDs {
		res := aggregate(pointsBySeries[id], agg, step)
		name := ""
		if meta, ok := e.idx.MetaByID(id); ok {
			name = meta.Name
		}
		out = append(out, MultiResult{SeriesID: id, Name: name, Results: res})
	}
	return out, nil
}
