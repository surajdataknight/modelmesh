# Operations

## Running locally (₹0)

```sh
make build
make run-mocks                                   # terminal 1: mocks on :9001 and :9002
MODELMESH_API_KEYS=dev:sk-dev-local-key \
  bin/modelmesh -config configs/local.json       # terminal 2
curl -N localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-dev-local-key' \
  -d '{"model":"fast","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Change mock behavior live, for example to make mock-a fail 30% of the time:

```sh
curl -X PUT localhost:9001/_mock/profile -d '{"error_rate":0.3}'
curl localhost:9001/_mock/stats
```

### With a real local model (optional, still ₹0)

[Ollama](https://ollama.com) serves an OpenAI-compatible API on `:11434/v1`.

1. Install it and pull a small model (`ollama pull llama3.2:1b`).
2. Run `bin/modelmesh -config configs/ollama.json`. The `local` alias tries Ollama first
   and falls back to mock-a.

Use local models only: Ollama's cloud models need a sign-in and are outside this
project's ₹0 scope. A small model on CPU is slow, which suits a realism check. It is not a
benchmark target; benchmarks use the mock so that results measure the gateway, not the
model.

## Metrics

All metrics are on the admin listener at `/metrics`. Label values come only from
configuration or fixed enums, so cardinality is bounded.

| Metric | Type | Labels |
|---|---|---|
| `modelmesh_requests_total` | counter | `model`, `outcome` (`success`, `bad_request`, `model_not_found`, `rate_limited`, `overloaded`, `upstream_client_error`, `upstream_error`, `upstream_rate_limited`, `timeout`, `interrupted`, `client_closed`, `slow_client`, `shutdown`) |
| `modelmesh_request_duration_seconds` | histogram | `model`, `stream` |
| `modelmesh_time_to_first_byte_seconds` | histogram | `model` (TTFT, as seen by the client, for streams) |
| `modelmesh_upstream_attempts_total` | counter | `provider`, `result` |
| `modelmesh_upstream_first_event_seconds` | histogram | `provider` |
| `modelmesh_provider_skips_total` | counter | `provider`, `reason` (`cooldown`, `breaker_open`, `saturated`) |
| `modelmesh_failovers_total`, `modelmesh_retries_total` | counter | `model` |
| `modelmesh_retry_budget_exhausted_total` | counter | — |
| `modelmesh_retry_budget_tokens` | gauge | — |
| `modelmesh_load_shed_total` | counter | `reason` (`concurrency`, `key_rate_limit`) |
| `modelmesh_auth_failures_total` | counter | — |
| `modelmesh_inflight_requests`, `modelmesh_inflight_streams` | gauge | — |
| `modelmesh_provider_inflight` | gauge | `provider` |
| `modelmesh_breaker_state` | gauge | `provider` (0 closed, 1 half-open, 2 open) |
| `modelmesh_breaker_transitions_total` | counter | `provider`, `to` |
| `modelmesh_tokens_total` | counter | `model`, `provider`, `kind` (prompt/completion), `source` (reported/estimated) |
| `modelmesh_estimated_cost_usd_total` | counter | `model`, `provider` (only when prices are configured) |
| `go_goroutines`, `go_heap_objects_bytes`, `go_gc_cycles_total`, `go_heap_allocs_bytes_total` | gauge/counter | — |
| `process_cpu_seconds_total`, `process_resident_memory_bytes` | counter/gauge | Linux only |

`docker compose --profile observability up` starts a Prometheus server (Apache-2.0)
that scrapes the gateway.

## Logs

At `info`, ModelMesh writes one JSON line per request to stdout:

```json
{"time":"…","level":"INFO","msg":"request","request_id":"1b2b02c238ff70b0","model":"fast","stream":false,
 "outcome":"success","status":200,"attempts":2,"duration_ms":51.886,"key":"dev","provider":"mock-b",
 "upstream_model":"mock-small","ttfb_ms":51.88,"prompt_tokens":1,"completion_tokens":4,
 "usage_source":"reported","retries":0,"failovers":1}
```

Prompts, completions, and key values are never logged; the `key` field is the key's
configured name. Breaker transitions are logged at `warn` when a breaker opens and at
`info` when it closes. Rejected provider credentials (401/403) are logged at `error`.

## Profiling

```sh
go tool pprof http://127.0.0.1:9090/debug/pprof/profile?seconds=20   # CPU
go tool pprof http://127.0.0.1:9090/debug/pprof/heap                 # memory
curl 'http://127.0.0.1:9090/debug/pprof/goroutine?debug=1' | head    # goroutines
```

## Security

### In the MVP

- **Client authentication.** Bearer API keys, required unless `auth.mode` is explicitly
  `none`.
  - Keys must be at least 16 characters.
  - They are compared as SHA-256 digests in constant time, against every configured key.
  - They come from an environment variable or a file, never from the config file.
- **Provider credentials.** These come from environment variables only and are sent only
  to the configured base URL. The upstream client never follows redirects, and the
  client's own `Authorization` header is never forwarded; a test covers this.
- **No SSRF surface.** Upstream URLs come only from configuration, and clients cannot
  influence where the gateway connects.
- **Bounded inputs.** Request bodies (`max_body_bytes`), upstream events
  (`max_event_bytes`), non-streaming upstream bodies (`max_response_bytes`), header
  sizes, header and body read deadlines, and request IDs (validated before being echoed
  or forwarded) all have limits.
- **Abuse limits.** There is a per-key token bucket, a global concurrency limit with
  immediate shedding, and per-provider concurrency limits.
- **Admin isolation.** Metrics, pprof, and provider status are on a separate listener
  bound to `127.0.0.1` by default; a warning is logged otherwise.
- **TLS.** Optional native TLS (`tls.cert_file`, `tls.key_file`). Otherwise bind to
  localhost or terminate TLS in front of the gateway.

### Not included (future work)

- per-key model allow-lists;
- key rotation without restart;
- mTLS to providers;
- audit logging;
- tenant isolation (out of scope; see ADR 0001).

## Shutdown

Send SIGTERM. Readiness turns 503, the listener closes, and in-flight requests have
`timeouts.shutdown_grace` to finish. Streams still open after that receive a
`gateway_shutdown` error event. A second SIGTERM exits immediately.
