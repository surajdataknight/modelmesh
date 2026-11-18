# ADR 0002: OpenAI Chat Completions is the public API and the internal lingua franca

- Status: Accepted (2026-11-18)

## Context

A gateway can either forward bytes, like a reverse proxy, or decode traffic into a
neutral internal model and re-encode it for each provider. Pure byte forwarding
(`httputil.ReverseProxy`) has no hooks for the things that make this gateway useful:

- failover across providers before the first byte;
- first-token and idle timeouts;
- in-band error events;
- usage extraction;
- model aliasing.

A fully neutral model, on the other hand, means a decode and re-encode for every chunk.
It also drops provider fields the model doesn't know about.

Every ₹0 upstream speaks the OpenAI dialect: `mockllm`, Ollama, llama.cpp server, and
vLLM.

## Decision

- The public API is a documented subset of OpenAI Chat Completions (`docs/api.md`).
- Internally, the OpenAI dialect is the lingua franca:
  - **Requests:** the gateway decodes only the fields it acts on (`model`, `messages`
    shape, `stream`, `stream_options`). It rewrites `model` to the upstream name, forces
    `stream_options.include_usage=true` for streams, and forwards every other field
    verbatim.
  - **Responses:** each stream chunk is inspected without being re-encoded. The gateway
    validates the JSON and extracts usage, in-band errors and content presence. The chunk's
    bytes are forwarded unchanged.
- There is one concrete `provider.Provider` type and no adapter interface yet. A second
  dialect (for example Anthropic Messages) will introduce an interface when it is built.

## Consequences

- Unknown and vendor-specific fields pass through untouched (this is tested).
- The internals are coupled to OpenAI's schema. A future second *public* dialect would
  have to translate twice.
- Validating each chunk with `json.Valid` is the largest CPU cost the gateway itself adds
  (measured in `docs/benchmarks.md`). It is kept because forwarding truncated JSON to
  clients is worse.
