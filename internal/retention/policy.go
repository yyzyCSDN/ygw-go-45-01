package retention

// Cutoff returns the oldest timestamp (in ns) that must be retained.
func (r *Retention) Cutoff(now int64) int64 {
	return now - r.policy.MaxAge.Nanoseconds()
}

// Step returns how often sweeps should run.
func (r *Retention) Step() int64 {
	return r.policy.SweepStep.Nanoseconds()
}
