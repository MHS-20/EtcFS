package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Registry holds Prometheus-style counters, gauges, and histograms.
// This is a lightweight in-memory implementation designed for the test harness;
// in production it would be backed by the Prometheus client library.
type Registry struct {
	mu       sync.Mutex
	counters map[string]*counter
	gauges   map[string]*gauge
}

type counter struct {
	labels map[string]float64
}
type gauge struct {
	val float64
}

func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*counter),
		gauges:   make(map[string]*gauge),
	}
}

// IncCounter increments a labeled counter by 1.
func (r *Registry) IncCounter(name, label string) {
	r.AddCounter(name, label, 1)
}

// AddCounter increments a labeled counter by a given value.
func (r *Registry) AddCounter(name, label string, val float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		c.labels[label] += val
		return
	}
	r.counters[name] = &counter{labels: map[string]float64{label: val}}
}

// SetGauge sets a gauge to an absolute value.
func (r *Registry) SetGauge(name string, val float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = &gauge{val: val}
}

// CounterValue returns the sum of all label values for a counter.
func (r *Registry) CounterValue(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		var sum float64
		for _, v := range c.labels {
			sum += v
		}
		return sum
	}
	return 0
}

// CounterLabels returns all label keys for a counter.
func (r *Registry) CounterLabels(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		keys := make([]string, 0, len(c.labels))
		for k := range c.labels {
			keys = append(keys, k)
		}
		return keys
	}
	return nil
}

// GaugeValue returns the current value of a gauge.
func (r *Registry) GaugeValue(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g.val
	}
	return 0
}

// TrackDuration records an observed duration in a histogram-like counter.
// Count is stored at name+"_count", cumulative seconds at name.
func (r *Registry) TrackDuration(name, label string, d time.Duration) {
	r.AddCounter(name+"_count", label, 1)
	r.AddCounter(name, label, d.Seconds())
}

// HasCounter checks whether a counter with the given name exists.
func (r *Registry) HasCounter(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.counters[name]
	return ok
}

// HasGauge checks whether a gauge with the given name exists.
func (r *Registry) HasGauge(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.gauges[name]
	return ok
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		for name, c := range r.counters {
			for label, val := range c.labels {
				_, _ = fmt.Fprintf(w, "%s{label=%q} %g\n", name, label, val)
			}
		}
		for name, g := range r.gauges {
			_, _ = fmt.Fprintf(w, "%s %g\n", name, g.val)
		}
	})
}

func StartServer(addr string, reg *Registry) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	return http.ListenAndServe(addr, mux)
}
