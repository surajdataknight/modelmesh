# API

ModelMesh exposes a **subset** of the OpenAI Chat Completions API. Point an OpenAI SDK at
`http://<host>:8080/v1` and use a ModelMesh key as the API key.

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:8080/v1", api_key="sk-dev-local-key")
for chunk in client.chat.completions.create(
        model="fast", messages=[{"role": "user", "content": "hi"}], stream=True):
    print(chunk.choices[0].delta.content or "", end="")
```

## Endpoints

| Method and path | Auth | Purpose |
|---|---|---|
| `POST /v1/chat/completions` | Bearer key | Chat completion, streaming or not |
| `GET /v1/models` | Bearer key | Lists the configured model **aliases** |
| `GET /healthz` | none | Liveness (always 200 while the process runs) |
| `GET /readyz` | none | Readiness (503 while shutting down) |

Admin listener (`admin_listen`, loopback by default):

| Path | Purpose |
|---|---|
| `GET /metrics` | Prometheus metrics |
| `GET /admin/providers` | Breaker state, in-flight requests, cooldown per provider, retry-budget balance |
| `/debug/pprof/*` | Go profiling |

## What the gateway guarantees, and what it passes through

ModelMesh **acts on** these request fields:

| Field | Behavior |
|---|---|
| `model` | Required, non-empty string. Must be a configured alias, or the response is 404. Rewritten to the target's upstream model name. |
| `messages` | Required, non-empty array of objects, each with a non-empty string `role`. Content is **not** validated by the gateway. |
| `stream` | Optional boolean. |
| `stream_options.include_usage` | Optional; allowed only when `stream` is true. The gateway always requests usage upstream, and strips the usage chunk unless the client asked for it. |

**Every other field** (`temperature`, `max_tokens`, `tools`, `response_format`, vendor
extensions, …) is forwarded byte-for-byte. Whether it works depends on the upstream. The
gateway doesn't claim universal compatibility.

## Not supported

- The Responses, Assistants, Embeddings, Images, Audio, Realtime, and legacy Completions
  APIs.
- Translation between dialects. Every upstream must speak OpenAI Chat Completions.
- Request or response caching, guardrails, and prompt storage.

## Response headers

| Header | Meaning |
|---|---|
| `X-Request-Id` | The client's value, if it is a valid ID of up to 64 characters (`[A-Za-z0-9._-]`); otherwise generated. Forwarded upstream. |
| `X-ModelMesh-Provider` | The provider that served the response |
| `X-ModelMesh-Upstream-Model` | The upstream model name used |
| `X-ModelMesh-Attempts` | Upstream attempts made, including failed ones |
| `Retry-After` | On 429 and 503 responses |

## Errors

Errors before the commit point use the OpenAI error envelope:

```json
{"error": {"message": "…", "type": "upstream_error", "param": null, "code": "upstream_error"}}
```

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_request` | Invalid JSON or invalid `model` / `messages` / `stream` / `stream_options` (`param` names the field) |
| 401 | `missing_api_key`, `invalid_api_key` | Authentication |
| 404 | `model_not_found` | Unknown alias |
| 404 | `not_found` | Unknown path |
| 408 | `body_read_timeout` | Request body slower than `timeouts.body_read` |
| 413 | `request_too_large` | Body over `limits.max_body_bytes` |
| 429 | `rate_limit_exceeded` | Per-key rate limit |
| 429 | `upstream_rate_limited` | Every target is cooling down after a 429 |
| 4xx | (the upstream's own body) | Upstream rejected the request as invalid; forwarded, not retried |
| 502 | `upstream_error`, `upstream_unreachable`, `upstream_interrupted`, `upstream_malformed_response`, `upstream_auth_failed`, `upstream_model_not_found` | Upstream failure after all attempts |
| 503 | `gateway_overloaded` | Over `limits.max_concurrent_requests` |
| 503 | `no_available_provider` | Every target's breaker is open or every target is at capacity |
| 503 | `gateway_shutdown` | Shutting down |
| 504 | `upstream_timeout`, `upstream_stalled`, `request_timeout` | Timeouts |

Errors after the commit point (streams) arrive as one final SSE event with the same
envelope, and the stream closes **without** `[DONE]`. See
[failure-semantics.md](failure-semantics.md).
