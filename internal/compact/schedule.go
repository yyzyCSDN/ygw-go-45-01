package compact

import "metricstore/internal/model"

// Eligible returns the ids of blocks currently ready for compaction.
func (c *Compactor) Eligible() []model.BlockID {
	ids := c.store.List()
	out := make([]model.BlockID, 0, len(ids))
	for _, id := range ids {
		b, err := c.store.Open(id)
		if err != nil || b == nil {
			continue
		}
		st := b.State()
		c.store.Close(b)
		if st == model.BlockImmutable {
			out = append(out, id)
		}
	}
	return out
}

// Next returns the next block to compact, or zero if none is eligible.
func (c *Compactor) Next() model.BlockID {
	eligible := c.Eligible()
	if len(eligible) == 0 {
		return 0
	}
	return eligible[0]
}
