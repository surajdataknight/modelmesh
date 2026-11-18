# Configuration

ModelMesh reads one JSON file (`-config path`). Fields not listed in the file take the
defaults below. **Unknown fields are rejected**, so a typo fails at startup. All
validation errors are reported together. `modelmesh -check -config file.json` validates
the file and exits.

Secrets never go in the file:

- client API keys come from the environment variable named by `auth.keys_env`, or from
  `auth.keys_file`;
- provider keys come from the environment variable named by each provider's
  `api_key_env`.

Durations are strings such as `"250ms"`, `"30s"`, or `"5m"`.

## Reference

| Field | Default | Meaning |
|---|---|---|
| `listen` | `":8080"` | Public listener |
| `admin_listen` | `"127.0.0.1:9090"` | Metrics, pprof, provider status. `""` disables it. A warning is logged if it is not loopback. |
| `log_level` | `"info"` | `debug`, `info`, `warn`, or `error`. At `info`, one JSON access-log line per request (no prompts or responses). |
| `tls.cert_file`, `tls.key_file` | — | Enable HTTPS on the public listener |
| `auth.mode` | `"api_keys"` | `api_keys`, or `none`, which must be set explicitly and is logged as a warning |
| `auth.keys_env` | `"MODELMESH_API_KEYS"` | Comma-separated keys, each optionally `name:key`. Names appear in logs. Keys must be at least 16 characters. |
| `auth.keys_file` | — | One key (or `name:key`) per line; `#` starts a comment. Overrides `keys_env`. |
| `limits.max_body_bytes` | `1048576` | Request body limit (413 above it) |
| `limits.max_response_bytes` | `8388608` | Limit for non-streaming upstream bodies |
| `limits.max_event_bytes` | `1048576` | Limit for one upstream SSE event |
| `limits.max_concurrent_requests` | `2048` | Admitted requests, streams included. Above it: immediate 503. |
| `limits.admission_wait` | `"0s"` | Time to wait for a slot before shedding. 0 sheds immediately. |
| `limits.per_key_rps`, `limits.per_key_burst` | `0`, = rps | Token bucket per API key; 0 disables it |
| `timeouts.request` | `"5m"` | Overall deadline, full stream included |
| `timeouts.first_token` | `"30s"` | Streaming: from sending upstream to receiving the first event |
| `timeouts.idle` | `"30s"` | Streaming: longest gap between upstream events |
| `timeouts.attempt` | `"2m"` | Non-streaming: one upstream attempt |
| `timeouts.client_write` | `"10s"` | One write to the client; ends streams to clients that stop reading |
| `timeouts.body_read` | `"10s"` | Reading the request body (408 above it) |
| `timeouts.read_header` | `"10s"` | Reading request headers |
| `timeouts.idle_conn` | `"2m"` | Keep-alive idle timeout |
| `timeouts.shutdown_grace` | `"30s"` | Drain time on SIGTERM |
| `retry.max_attempts` | `3` | Total upstream attempts per request (1–10) |
| `retry.base_backoff`, `retry.max_backoff` | `"100ms"`, `"2s"` | Full-jitter backoff, used only when repeating an already-tried provider |
| `retry.budget_ratio` | `0.2` | Repeat attempts allowed per request, on average |
| `retry.budget_min_per_second` | `5` | Retry-budget floor for low traffic |
| `retry.default_cooldown` | `"1s"` | Cooldown after a 429 without `Retry-After` |
| `retry.max_cooldown` | `"1m"` | Cap on any `Retry-After` |
| `breaker.failure_threshold` | `5` | Consecutive failures that open a provider's breaker |
| `breaker.open_duration` | `"10s"` | Time before a half-open probe |
| `breaker.half_open_probes` | `1` | Concurrent probes while half-open |
| `providers[].name` | required | `[A-Za-z0-9][A-Za-z0-9._:/-]*`, unique |
| `providers[].base_url` | required | Including the API prefix, e.g. `http://127.0.0.1:11434/v1`. No embedded credentials. |
| `providers[].api_key_env` | — | Environment variable holding the provider key. Startup fails if it is set but empty. |
| `providers[].max_concurrent` | `256` | In-flight attempts to this provider; also its idle-connection pool size |
| `providers[].price_per_1k_prompt`, `price_per_1k_completion` | `0` | USD per 1K tokens, used only for `modelmesh_estimated_cost_usd_total` |
| `models[].name` | required | Public alias that clients send as `model` |
| `models[].targets[]` | required | Ordered `{ "provider": …, "model": … }`; the first is preferred, the rest are failover |

## Example files

| File | Use |
|---|---|
| `configs/local.json` | Two local `mockllm` instances with failover: the default development setup |
| `configs/bench.json` | Benchmark settings used by `scripts/bench.sh` |
| `configs/ollama.json` | A local Ollama model with a mock fallback (₹0; needs Ollama installed) |
| `configs/hosted-optional.json` | **Optional and paid**: a hosted OpenAI-compatible API with a mock fallback. Never needed for development or tests. |

## Deployment notes

- Behind a load balancer, use `/readyz` for readiness and `/healthz` for liveness.
- Under a container memory limit, consider setting `GOMEMLIMIT` a little below the limit.
  At thousands of open streams, memory is mostly goroutine stacks and garbage-collector
  headroom (see `docs/benchmarks.md`).
- `GOMAXPROCS` follows cgroup CPU limits automatically from Go 1.25 onwards. On 1.24, set
  it explicitly in CPU-limited containers.
