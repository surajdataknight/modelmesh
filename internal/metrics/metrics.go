// Package metrics is a small, dependency-free implementation of Prometheus
// counters, gauges, and histograms with text exposition format 0.0.4.
//
// ModelMesh avoids prometheus/client_golang to stay standard-library only
// (ADR 0006). The subset implemented here is what the gateway needs: label
// vectors with bounded cardinality, cumulative histograms, and callback
// gauges evaluated at scrape time.
package metrics

import (
	"bufio"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	write(w *bufio.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// WriteTo renders every family in text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	r.mu.Unlock()
	cw := &countWriter{w: w}
	bw := bufio.NewWriter(cw)
	for _, f := range fams {
		f.write(bw)
	}
	err := bw.Flush()
	return cw.n, err
}

// Handler serves the registry over HTTP.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// atomicFloat is a float64 updated with compare-and-swap.
type atomicFloat struct{ bits atomic.Uint64 }

func (f *atomicFloat) Add(v float64) {
	for {
		old := f.bits.Load()
		if f.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}
func (f *atomicFloat) Set(v float64) { f.bits.Store(math.Float64bits(v)) }
func (f *atomicFloat) Load() float64 { return math.Float64frombits(f.bits.Load()) }

// vec maps label values to series. Label sets in ModelMesh come from
// configuration (provider and model names) or small enums, so cardinality
// is bounded by construction.
type vec[T any] struct {
	name, help, typ string
	labels          []string
	mu              sync.RWMutex
	series          map[string]*T
	values          map[string][]string
	newT            func() *T
}

func newVec[T any](name, help, typ string, labels []string, newT func() *T) *vec[T] {
	return &vec[T]{name: name, help: help, typ: typ, labels: labels,
		series: map[string]*T{}, values: map[string][]string{}, newT: newT}
}

func (v *vec[T]) with(values ...string) *T {
	if len(values) != len(v.labels) {
		panic("metrics: " + v.name + ": wrong number of label values")
	}
	key := strings.Join(values, "\xff")
	v.mu.RLock()
	s, ok := v.series[key]
	v.mu.RUnlock()
	if ok {
		return s
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if s, ok = v.series[key]; ok {
		return s
	}
	s = v.newT()
	v.series[key] = s
	v.values[key] = append([]string(nil), values...)
	return s
}

func (v *vec[T]) sortedKeys() []string {
	v.mu.RLock()
	keys := make([]string, 0, len(v.series))
	for k := range v.series {
		keys = append(keys, k)
	}
	v.mu.RUnlock()
	sort.Strings(keys)
	return keys
}

func (v *vec[T]) get(key string) (*T, []string) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.series[key], v.values[key]
}

func writeHeader(w *bufio.Writer, name, help, typ string) {
	w.WriteString("# HELP ")
	w.WriteString(name)
	w.WriteByte(' ')
	w.WriteString(strings.NewReplacer("\\", `\\`, "\n", `\n`).Replace(help))
	w.WriteString("\n# TYPE ")
	w.WriteString(name)
	w.WriteByte(' ')
	w.WriteString(typ)
	w.WriteByte('\n')
}

var labelEscaper = strings.NewReplacer("\\", `\\`, "\"", `\"`, "\n", `\n`)

func writeSample(w *bufio.Writer, name string, labels, values []string, extraName, extraValue string, v float64) {
	w.WriteString(name)
	if len(labels) > 0 || extraName != "" {
		w.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				w.WriteByte(',')
			}
			w.WriteString(l)
			w.WriteString(`="`)
			w.WriteString(labelEscaper.Replace(values[i]))
			w.WriteByte('"')
		}
		if extraName != "" {
			if len(labels) > 0 {
				w.WriteByte(',')
			}
			w.WriteString(extraName)
			w.WriteString(`="`)
			w.WriteString(extraValue)
			w.WriteByte('"')
		}
		w.WriteByte('}')
	}
	w.WriteByte(' ')
	w.WriteString(formatFloat(v))
	w.WriteByte('\n')
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Counter is a monotonically increasing value.
type Counter struct{ v atomicFloat }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds a non-negative value.
func (c *Counter) Add(v float64) {
	if v > 0 {
		c.v.Add(v)
	}
}

// Value returns the current value.
func (c *Counter) Value() float64 { return c.v.Load() }

// CounterVec is a counter family partitioned by labels.
type CounterVec struct{ v *vec[Counter] }

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{v: newVec(name, help, "counter", labels, func() *Counter { return &Counter{} })}
	r.add(c)
	return c
}

// With returns the series for the label values, creating it if needed.
// Hot paths should call With once and keep the *Counter.
func (c *CounterVec) With(values ...string) *Counter { return c.v.with(values...) }

func (c *CounterVec) write(w *bufio.Writer) {
	writeHeader(w, c.v.name, c.v.help, c.v.typ)
	for _, k := range c.v.sortedKeys() {
		s, vals := c.v.get(k)
		writeSample(w, c.v.name, c.v.labels, vals, "", "", s.Value())
	}
}

// Gauge is a value that can go up and down.
type Gauge struct{ v atomicFloat }

// Set replaces the value.
func (g *Gauge) Set(v float64) { g.v.Set(v) }

// Add adds v (may be negative).
func (g *Gauge) Add(v float64) { g.v.Add(v) }

// Value returns the current value.
func (g *Gauge) Value() float64 { return g.v.Load() }

// GaugeVec is a gauge family partitioned by labels.
type GaugeVec struct{ v *vec[Gauge] }

// NewGaugeVec registers a gauge family.
func (r *Registry) NewGaugeVec(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{v: newVec(name, help, "gauge", labels, func() *Gauge { return &Gauge{} })}
	r.add(g)
	return g
}

// With returns the series for the label values.
func (g *GaugeVec) With(values ...string) *Gauge { return g.v.with(values...) }

func (g *GaugeVec) write(w *bufio.Writer) {
	writeHeader(w, g.v.name, g.v.help, g.v.typ)
	for _, k := range g.v.sortedKeys() {
		s, vals := g.v.get(k)
		writeSample(w, g.v.name, g.v.labels, vals, "", "", s.Value())
	}
}

// gaugeFunc is evaluated at scrape time.
type gaugeFunc struct {
	name, help, typ string
	labels          []string
	collect         func(emit func(v float64, values ...string))
}

// NewGaugeFunc registers a gauge whose samples are produced by collect at
// scrape time. Use it for values owned elsewhere (breaker state, goroutines).
func (r *Registry) NewGaugeFunc(name, help string, labels []string, collect func(emit func(v float64, values ...string))) {
	r.add(&gaugeFunc{name: name, help: help, typ: "gauge", labels: labels, collect: collect})
}

// NewCounterFunc registers a counter read at scrape time.
func (r *Registry) NewCounterFunc(name, help string, labels []string, collect func(emit func(v float64, values ...string))) {
	r.add(&gaugeFunc{name: name, help: help, typ: "counter", labels: labels, collect: collect})
}

func (g *gaugeFunc) write(w *bufio.Writer) {
	writeHeader(w, g.name, g.help, g.typ)
	g.collect(func(v float64, values ...string) {
		writeSample(w, g.name, g.labels, values, "", "", v)
	})
}

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	upper  []float64
	counts []atomic.Uint64 // non-cumulative; the +Inf bucket is last
	sum    atomicFloat
	count  atomic.Uint64
}

func newHistogram(buckets []float64) *Histogram {
	return &Histogram{upper: buckets, counts: make([]atomic.Uint64, len(buckets)+1)}
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.upper, v) // first bucket with upper >= v
	h.counts[i].Add(1)
	h.sum.Add(v)
	h.count.Add(1)
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// HistogramVec is a histogram family partitioned by labels.
type HistogramVec struct {
	v       *vec[Histogram]
	buckets []float64
}

// NewHistogramVec registers a histogram family. Buckets must be sorted.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	h := &HistogramVec{buckets: b}
	h.v = newVec(name, help, "histogram", labels, func() *Histogram { return newHistogram(b) })
	r.add(h)
	return h
}

// With returns the series for the label values.
func (h *HistogramVec) With(values ...string) *Histogram { return h.v.with(values...) }

func (h *HistogramVec) write(w *bufio.Writer) {
	writeHeader(w, h.v.name, h.v.help, h.v.typ)
	for _, k := range h.v.sortedKeys() {
		s, vals := h.v.get(k)
		var cum uint64
		for i, ub := range s.upper {
			cum += s.counts[i].Load()
			writeSample(w, h.v.name+"_bucket", h.v.labels, vals, "le", formatFloat(ub), float64(cum))
		}
		cum += s.counts[len(s.upper)].Load()
		writeSample(w, h.v.name+"_bucket", h.v.labels, vals, "le", "+Inf", float64(cum))
		writeSample(w, h.v.name+"_sum", h.v.labels, vals, "", "", s.sum.Load())
		writeSample(w, h.v.name+"_count", h.v.labels, vals, "", "", float64(cum))
	}
}

// LatencyBuckets suits LLM request latencies: milliseconds to minutes.
var LatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}
