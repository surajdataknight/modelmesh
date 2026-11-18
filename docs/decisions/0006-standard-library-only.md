# ADR 0006: Standard library only

- Status: Accepted (2026-11-18)

## Context

The project should show what Go's standard library can do for network infrastructure,
and every dependency is something to audit and keep updated. The build environment for
the first version also had no network access, so third-party modules could not be
fetched at all.

## Decision

`go.mod` has no `require` lines.

- **Configuration is JSON** (`encoding/json` with `DisallowUnknownFields`, so typos fail
  at startup). Durations are strings like `"30s"`.
- **Metrics are a small Prometheus text-format implementation** (`internal/metrics`):
  counters, gauges, cumulative histograms, and callback gauges, with bounded label sets.
  It covers what the gateway needs and nothing more.
- **Logging uses `log/slog`** (JSON). **Profiling uses `net/http/pprof`** on the admin
  listener.

## Consequences

- The hand-written metrics code needs its own tests, and it has them: format, escaping,
  and concurrency.
- Distributed tracing (OpenTelemetry) is not included. It is future work and would be
  the first justified dependency.
