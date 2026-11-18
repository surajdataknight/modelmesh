# ADR 0005: Reliability policy — breaker, cooldown, retry budget, load shedding

- Status: Accepted (2026-11-18)

## Decisions

- **Circuit breaker per provider, based on consecutive failures.**
  - It opens after `failure_threshold` consecutive failures and stays open for
    `open_duration`. After that it admits `half_open_probes` concurrent probes; a
    successful probe closes it and a failed one reopens it.
  - Failures are 5xx, 408, timeouts, connection errors, malformed responses, provider
    credential errors, and broken streams.
  - Client cancellations, client-side 4xx, and 429s are ignored.
  - Consecutive counting was chosen over a rolling error rate because it is deterministic,
    easy to test, and needs no window tuning. The trade-off: a provider failing randomly
    20% of the time still trips occasionally (0.2^5 ≈ 1 in 3,000 windows), which the
    `faults` benchmark shows.
- **429 means cooldown, not breaker.** A 429 makes the whole provider ineligible for its
  `Retry-After`, capped by `max_cooldown`. When `Retry-After` is missing, `default_cooldown`
  applies. Provider rate limits are usually per account, and one key per provider is the
  model here, so cooling down the whole provider is correct. If every target is cooling
  down, the client gets a 429 with the smallest remaining `Retry-After`.
- **Failover is free; retries are budgeted.**
  - Moving to a provider not yet tried for this request is immediate and costs no retry
    budget. It adds no load to the failing provider, and amplification is bounded by the
    target list and `max_attempts`.
  - Trying an already-tried provider again spends a retry-budget token and waits a
    full-jitter backoff.
  - The budget refills at `budget_ratio` per request plus `budget_min_per_second`. That
    caps retries at roughly 20% of traffic and prevents retry storms.
  - An earlier version also charged failovers to the budget. The `breaker` benchmark
    showed that this turned a dead primary into 504s at 50 req/s even though a healthy
    secondary was available, so the rule was changed.
- **Load shedding without a queue.** Above `max_concurrent_requests`, requests get an
  immediate 503 with `Retry-After: 1`, unless `admission_wait` is set. A per-provider
  `max_concurrent` limit makes the router skip a saturated provider (with no attempt and
  no breaker penalty) and move to the next target.
- **Per-key rate limiting.** A token bucket per API key (`per_key_rps`, `per_key_burst`)
  is checked before any other work.

## Consequences

All of this state lives in memory on one instance (ADR 0001). Every rule above has a
failure-matrix test in `internal/gateway/matrix_test.go`.
