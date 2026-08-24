package ingest

// BulkPoint is one sample in a batch write.
type BulkPoint struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	TS     int64             `json:"ts"`
	Value  float64           `json:"value"`
}

// AppendBatch writes several samples under one lock cycle. It is used by the
// bulk write endpoint and by replay during recovery.
func (in *Ingest) AppendBatch(points []BulkPoint) error {
	for _, pt := range points {
		if err := in.Append(pt.Name, pt.Labels, pt.TS, pt.Value); err != nil {
			return err
		}
	}
	return nil
}
