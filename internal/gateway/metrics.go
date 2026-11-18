package gateway

import (
	"github.com/modelmesh/modelmesh/internal/metrics"
)

// gwMetrics are the gateway's metric families. Every label value comes from
// configuration (model aliases, provider names) or a fixed enum, so series
// cardinality is bounded no matter what clients send.
type gwMetrics struct {
	requests           *metrics.CounterVec   // model, outcome
	duration           *metrics.HistogramVec // model, stream
	ttft               *metrics.HistogramVec // model
	attempts           *metrics.CounterVec   // provider, result
	upstreamTTFT       *metrics.HistogramVec // provider
	skips              *metrics.CounterVec   // provider, reason
	retries            *metrics.CounterVec   // model
	failovers          *metrics.CounterVec   // model
	budgetExhausted    *metrics.Counter
	shed               *metrics.CounterVec // reason
	authFailures       *metrics.Counter
	inflight           *metrics.Gauge
	streams            *metrics.Gauge
	tokens             *metrics.CounterVec // model, provider, kind, source
	cost               *metrics.CounterVec // model, provider
	breakerTransitions *metrics.CounterVec // provider, to
}

func newMetrics(r *metrics.Registry, g *Gateway) *gwMetrics {
	m := &gwMetrics{
		requests: r.NewCounterVec("modelmesh_requests_total",
			"Client requests by model alias and final outcome.", "model", "outcome"),
		duration: r.NewHistogramVec("modelmesh_request_duration_seconds",
			"Total request duration, including the full stream.", metrics.LatencyBuckets, "model", "stream"),
		ttft: r.NewHistogramVec("modelmesh_time_to_first_byte_seconds",
			"Time from request receipt to the first response byte sent to the client (TTFT for streams).",
			metrics.LatencyBuckets, "model"),
		attempts: r.NewCounterVec("modelmesh_upstream_attempts_total",
			"Upstream attempts by provider and result.", "provider", "result"),
		upstreamTTFT: r.NewHistogramVec("modelmesh_upstream_first_event_seconds",
			"Time from sending an upstream request to its first SSE event or full response.",
			metrics.LatencyBuckets, "provider"),
		skips: r.NewCounterVec("modelmesh_provider_skips_total",
			"Targets skipped without an attempt, by reason.", "provider", "reason"),
		retries: r.NewCounterVec("modelmesh_retries_total",
			"Repeat attempts against a provider already tried for the same request.", "model"),
		failovers: r.NewCounterVec("modelmesh_failovers_total",
			"Attempts moved to a different provider after a failure.", "model"),
		shed: r.NewCounterVec("modelmesh_load_shed_total",
			"Requests rejected before routing, by reason.", "reason"),
		tokens: r.NewCounterVec("modelmesh_tokens_total",
			"Tokens by kind (prompt|completion) and source (reported|estimated).", "model", "provider", "kind", "source"),
		cost: r.NewCounterVec("modelmesh_estimated_cost_usd_total",
			"Estimated spend from configured per-1K-token prices.", "model", "provider"),
		breakerTransitions: r.NewCounterVec("modelmesh_breaker_transitions_total",
			"Circuit breaker transitions by provider and new state.", "provider", "to"),
	}
	m.budgetExhausted = r.NewCounterVec("modelmesh_retry_budget_exhausted_total",
		"Retries denied because the retry budget was empty.").With()
	m.authFailures = r.NewCounterVec("modelmesh_auth_failures_total",
		"Requests rejected for missing or invalid API keys.").With()
	m.inflight = r.NewGaugeVec("modelmesh_inflight_requests",
		"Admitted requests currently being processed.").With()
	m.streams = r.NewGaugeVec("modelmesh_inflight_streams",
		"Streams committed to clients and still open.").With()

	r.NewGaugeFunc("modelmesh_breaker_state",
		"Circuit breaker state per provider: 0 closed, 1 half-open, 2 open.", []string{"provider"},
		func(emit func(float64, ...string)) {
			for _, p := range g.providers {
				emit(float64(p.Breaker.State()), p.Name)
			}
		})
	r.NewGaugeFunc("modelmesh_provider_inflight",
		"Upstream attempts in flight per provider.", []string{"provider"},
		func(emit func(float64, ...string)) {
			for _, p := range g.providers {
				emit(float64(p.Sem.InUse()), p.Name)
			}
		})
	r.NewGaugeFunc("modelmesh_retry_budget_tokens",
		"Retries currently available in the retry budget.", nil,
		func(emit func(float64, ...string)) { emit(g.budget.Tokens()) })
	return m
}
