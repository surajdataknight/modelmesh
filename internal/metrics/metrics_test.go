package metrics

import (
	"strings"
	"sync"
	"testing"
)

func render(r *Registry) string {
	var b strings.Builder
	_, _ = r.WriteTo(&b)
	return b.String()
}

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("mm_requests_total", "Requests.", "model", "outcome")
	c.With("fast", "success").Add(3)
	c.With("fast", "upstream_error").Inc()
	c.With(`we"ird`, "x").Inc()
	g := r.NewGaugeVec("mm_inflight", "In flight.")
	g.With().Set(7)
	h := r.NewHistogramVec("mm_latency_seconds", "Latency.", []float64{0.1, 1}, "model")
	for _, v := range []float64{0.05, 0.1, 0.5, 2} {
		h.With("fast").Observe(v)
	}
	r.NewGaugeFunc("mm_state", "State.", []string{"provider"}, func(emit func(float64, ...string)) {
		emit(2, "a")
	})

	out := render(r)
	for _, want := range []string{
		"# TYPE mm_requests_total counter",
		`mm_requests_total{model="fast",outcome="success"} 3`,
		`mm_requests_total{model="fast",outcome="upstream_error"} 1`,
		`mm_requests_total{model="we\"ird",outcome="x"} 1`,
		"mm_inflight 7",
		`mm_latency_seconds_bucket{model="fast",le="0.1"} 2`,
		`mm_latency_seconds_bucket{model="fast",le="1"} 3`,
		`mm_latency_seconds_bucket{model="fast",le="+Inf"} 4`,
		`mm_latency_seconds_sum{model="fast"} 2.65`,
		`mm_latency_seconds_count{model="fast"} 4`,
		`mm_state{provider="a"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("c_total", "c", "k")
	h := r.NewHistogramVec("h", "h", LatencyBuckets)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.With("x").Inc()
				h.With().Observe(0.2)
				_ = render(r)
			}
		}()
	}
	wg.Wait()
	if v := c.With("x").Value(); v != 8000 {
		t.Fatalf("counter = %v", v)
	}
	if n := h.With().Count(); n != 8000 {
		t.Fatalf("histogram count = %v", n)
	}
}

func TestRuntimeMetrics(t *testing.T) {
	r := NewRegistry()
	RegisterRuntime(r)
	out := render(r)
	if !strings.Contains(out, "go_goroutines ") {
		t.Fatalf("missing go_goroutines:\n%s", out)
	}
}
