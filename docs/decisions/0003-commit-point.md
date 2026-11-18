# ADR 0003: The commit point — retries happen only before the first byte

- Status: Accepted (2026-11-18)

## Context

Once a streaming response has sent `200 OK` and any part of the body, the status code
can't change. Retrying at that point would:

- duplicate tokens the client has already shown;
- double-bill the provider usage;
- produce text that silently changes halfway through.

## Decision

- The **commit point** is the moment the gateway writes the response headers and the
  first upstream event to the client.
  - For streams, that happens only after the first upstream SSE event has been read and
    validated.
  - For non-streaming requests, it happens only after the full body has been read and
    validated.
- **Before the commit point,** any retryable failure may be retried or failed over. These
  include connection errors, 5xx, 408, 429, a first-token timeout, an invalid first event,
  and a stream that ends before its first event. The client never sees these failures.
- **After the commit point,** nothing is retried. A failure such as a stall, a cut, a
  malformed event, a provider error event, the request deadline, or shutdown is reported
  as a final SSE event, `data: {"error": {...}}`. The stream then closes **without**
  `[DONE]`, which OpenAI SDKs treat as an incomplete stream.

## Consequences

- The benchmark `faults` scenario shows the split directly. Failures before the commit
  point are almost entirely masked. Failures after it reach the client, by design.
- The time to first token can't be faster than the first upstream event. This costs
  nothing: the client could not have received a token earlier anyway.
