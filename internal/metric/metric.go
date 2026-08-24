package metric

import (
	"fmt"
	"sort"
	"sync"
)

// Counter is a monotonic counter.
type Counter struct {
	mu    sync.Mutex
	name  string
	value int64
}

// Inc adds delta to the counter.
func (c *Counter) Inc(delta int64) {
	c.mu.Lock()
	c.value += delta
	c.mu.Unlock()
}

// Value returns the current value.
func (c *Counter) Value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Gauge is a settable gauge.
type Gauge struct {
	mu    sync.Mutex
	name  string
	value int64
}

// Set updates the gauge.
func (g *Gauge) Set(v int64) {
	g.mu.Lock()
	g.value = v
	g.mu.Unlock()
}

// Add adjusts the gauge by delta.
func (g *Gauge) Add(delta int64) {
	g.mu.Lock()
	g.value += delta
	g.mu.Unlock()
}

// Value returns the current value.
func (g *Gauge) Value() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.value
}

// Registry collects named counters and gauges.
type Registry struct {
	mu       sync.Mutex
	counters map[string]*Counter
	gauges   map[string]*Gauge
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{counters: make(map[string]*Counter), gauges: make(map[string]*Gauge)}
}

// Counter returns (creating if needed) a counter.
func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &Counter{name: name}
		r.counters[name] = c
	}
	return c
}

// Gauge returns (creating if needed) a gauge.
func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.gauges[name]
	if !ok {
		g = &Gauge{name: name}
		r.gauges[name] = g
	}
	return g
}

// Snapshot renders every metric in text format.
func (r *Registry) Snapshot() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.counters)+len(r.gauges))
	for name := range r.counters {
		names = append(names, name)
	}
	for name := range r.gauges {
		names = append(names, name)
	}
	sort.Strings(names)
	var out string
	for _, name := range names {
		if c, ok := r.counters[name]; ok {
			out += fmt.Sprintf("%s %d\n", name, c.Value())
		}
		if g, ok := r.gauges[name]; ok {
			out += fmt.Sprintf("%s %d\n", name, g.Value())
		}
	}
	return out
}
