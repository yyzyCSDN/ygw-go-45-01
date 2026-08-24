package query

// AggDelta computes the difference between consecutive window values. It is a
// real aggregation used for rate-of-change inspection on the console.
const (
	AggDelta AggFunc = 100
)

func deltaOf(values []float64) []float64 {
	if len(values) == 0 {
		return nil
	}
	out := make([]float64, len(values))
	out[0] = values[0]
	for i := 1; i < len(values); i++ {
		out[i] = values[i] - values[i-1]
	}
	return out
}
