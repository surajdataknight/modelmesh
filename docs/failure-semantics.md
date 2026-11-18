# Failure semantics

This is ModelMesh's behavioral contract. Every row is pinned by a test. The test name is
in the last column; all of them are in `internal/gateway/*_test.go`.

**Commit point**: the moment the gateway sends response headers and the first
upstream event (for streams) or the full validated body (for non-streaming requests).
Before it, failures are invisible to the client and may be retried or failed over. After
it, failures arrive in-band and are never retried.

## Upstream failures before the commit point

| Failure | Gateway behavior | Breaker | Test |
|---|---|---|---|
| 5xx or 408 | Fail over immediately to the next untried target; repeat a target only with backoff and budget | failure | `TestFailoverOn5xxBeforeCommit`, `TestRetriesAreBoundedByMaxAttempts` |
| 429 with `Retry-After` | The provider cools down for `Retry-After` (capped) and is skipped until then; fail over | ignored | `TestRateLimitedProviderCoolsDownAndIsSkipped` |
| Every target is cooling down | `429 upstream_rate_limited` with `Retry-After` = smallest remaining cooldown | — | `TestAllProvidersRateLimitedReturns429WithRetryAfter` |
| No answer before `first_token` (streams) | Attempt cancelled (the provider sees the disconnect); fail over | failure | `TestFirstTokenTimeoutFailsOver` |
| No answer before `attempt` (non-streaming) | Attempt cancelled; fail over | failure | (same code path as above) |
| Stream ends or is cut before its first event | Fail over | failure | `TestPreCommitStreamFaultsFailOver` |
| First event is invalid JSON, `[DONE]`, or an error object | Fail over | failure | `TestPreCommitStreamFaultsFailOver` |
| Non-streaming body is cut off or invalid JSON | Fail over | failure | `TestNonStreamBrokenResponsesFailOver` |
| 401 or 403 (bad provider credentials) | Logged at error level; fail over; client gets `502 upstream_auth_failed` if nothing else works | failure | — |
| 404 (provider doesn't serve the model) | Fail over; `502 upstream_model_not_found` if nothing else works | ignored | — |
| Other 4xx (400, 413, 422, …) | **Not retried**: the provider's status and JSON body are forwarded unchanged | ignored | `TestUpstreamClientErrorIsForwardedNotRetried` |
| `max_attempts` reached | Error from the last failure (502 / 504 / 429) | — | `TestRetriesAreBoundedByMaxAttempts` |
| Retry budget empty | Stops retrying an already-tried provider; failover is unaffected | — | `TestRetryBudgetPreventsRetryStorm`, `TestFailoverDoesNotSpendRetryBudget` |
| Breaker open | Target skipped with no attempt; after `open_duration`, one half-open probe | — | `TestCircuitBreakerOpensAndRecovers` |
| Provider at `max_concurrent` | Target skipped with no attempt and no breaker penalty | — | `TestProviderConcurrencyLimitSkipsToNextTarget` |
| No target can be attempted | `503 no_available_provider`, `Retry-After: 1` | — | (covered by the breaker and concurrency tests) |

## Upstream failures after the commit point (streams)

The client receives one final event, then the stream closes **without** `data: [DONE]`:

```
data: {"error":{"message":"upstream stream stalled","type":"timeout_error","param":null,"code":"upstream_stalled"}}
```

| Failure | `code` | Retried? | Upstream | Breaker | Test |
|---|---|---|---|---|---|
| No event for `timeouts.idle` | `upstream_stalled` | no | cancelled | failure | `TestMidStreamFaultsAreReportedInBandAndNotRetried/stall` |
| Connection cut, or EOF without `[DONE]` | `upstream_interrupted` | no | closed | failure | `…/cut` |
| Invalid JSON event | `upstream_malformed_response` | no | closed | failure | `…/malformed` |
| Provider sends an `{"error":…}` event | the provider's event is forwarded as-is | no | closed | failure | — |
| Event larger than `max_event_bytes` | `upstream_malformed_response` | no | closed | failure | (SSE unit tests) |
| `timeouts.request` reached | `request_timeout` | no | cancelled | ignored | `TestRequestDeadlineEndsStreamInBand` |
| Shutdown grace period expires | `gateway_shutdown` | no | cancelled | ignored | `TestShutdownGraceExpiryEndsStreamsWithErrorEvent` |

## Client-side events

| Event | Behavior | Test |
|---|---|---|
| Client disconnects mid-stream | The attempt context is cancelled and the upstream connection is closed at once (under 1 ms in tests); all resources are released; outcome `client_closed` | `TestClientDisconnectCancelsUpstream` |
| Client stops reading (slow consumer) | The per-write deadline (`client_write`) expires, the stream ends, and upstream is cancelled. Memory stays bounded because nothing is buffered beyond one event; outcome `slow_client` | `TestSlowConsumerIsDisconnected` |
| Request body arrives too slowly | `408 body_read_timeout`, connection closed | `TestSlowRequestBodyGets408` |
| Body over `max_body_bytes` | `413 request_too_large` | `TestRequestValidation` |
| Invalid JSON, missing `model` or `messages`, bad `stream` | `400 invalid_request` with `param` | `TestRequestValidation` |
| Unknown model alias | `404 model_not_found` | `TestRequestValidation` |
| Missing or invalid API key | `401 missing_api_key` / `invalid_api_key` | `TestAuthenticationAndPerKeyRateLimit` |
| Per-key rate limit exceeded | `429 rate_limit_exceeded` with `Retry-After` | `TestAuthenticationAndPerKeyRateLimit` |
| Gateway at `max_concurrent_requests` | Immediate `503 gateway_overloaded`, `Retry-After: 1`; never reaches a provider | `TestOverloadIsShedImmediately` |

## Gateway lifecycle

| Event | Behavior | Test |
|---|---|---|
| SIGTERM with streams in flight | `/readyz` turns 503; listener closes; streams finish within `shutdown_grace` | `TestGracefulShutdownDrainsInFlightStreams` |
| Streams longer than every server timeout | Unaffected: only per-write and idle deadlines apply | `TestLongStreamOutlivesServerTimeouts` |
| Many requests across all failure modes | Goroutine count returns to baseline (7 → 7) | `TestNoGoroutineLeaksAcrossFailureModes` |
| Sequential requests to one provider | Connections are reused (1 connection for 10 requests, even when the upstream ends its response late) | `TestUpstreamConnectionsAreReused`, `TestConnectionReusedWhenTerminatorArrivesLate` |
| Event followed by half of the next | The complete event is flushed immediately | `TestEventIsNotHeldBehindPartialEvent` |

## Token accounting

- For streams, the gateway always sets `stream_options.include_usage=true` upstream. The
  extra usage chunk is removed unless the client asked for it
  (`TestStreamSuccessFiltersInjectedUsageChunk`).
- Provider-reported usage is counted as `source="reported"`.
- When there is none, for example a stream cut short or a provider that ignores
  `include_usage`, the gateway counts `source="estimated"`:
  - prompt tokens ≈ request bytes / 4;
  - completion tokens ≈ content chunks (streams) or response bytes / 4.

  These are rough by design and never mixed with reported numbers.
