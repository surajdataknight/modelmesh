# ADR 0007: Mock-first testing with a real-HTTP fault-injecting provider

- Status: Accepted (2026-11-18)

## Decision

- `mockllm` is a real HTTP server that speaks the OpenAI wire format. It can inject:
  - latency: first-token delay, jitter, and tokens per second;
  - pre-response faults: hang, 429 with `Retry-After`, and arbitrary error status;
  - mid-stream faults: stall, connection cut, and malformed JSON, at a chosen token or
    before the first chunk;
  - overload: a 429 once more than `max_concurrent` requests are in flight.
- Its profile can be changed at runtime (`PUT /_mock/profile`), and seeds make faults
  reproducible.
- Gateway tests run the real gateway handler against real `mockllm` servers over loopback
  HTTP. There are no in-process fakes of `net/http`.
- Time-dependent primitives (breaker, budget, token bucket) take an injectable clock, so
  their unit tests never sleep.

## Limits (stated honestly)

- Passing tests against our own mock proves the gateway agrees with our model of
  providers, not that every provider behaves this way.
- The README states exactly what has been validated against what. The generic adapter
  targets the OpenAI-compatible endpoints of Ollama, vLLM, and llama.cpp, but this build
  environment could not run Ollama.
