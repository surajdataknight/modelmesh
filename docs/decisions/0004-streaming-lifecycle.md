# ADR 0004: Streaming lifecycle and timeouts

- Status: Accepted (2026-11-18)

## Decisions

1. **One goroutine owns each stream.** The handler goroutine reads an upstream event,
   inspects it, writes it to the client, and flushes, in strict alternation. There are no
   channels and no per-stream helper goroutines. A slow client slows the upstream read
   through TCP flow control, and memory per stream is bounded by one event.
2. **Per-write deadlines instead of `Server.WriteTimeout`.** `WriteTimeout` sets one
   deadline for the whole response. A test against Go 1.24 confirmed it cuts a 500 ms
   stream off after 100 ms. The handler instead arms a rolling deadline
   (`timeouts.client_write`) through `http.ResponseController`. It re-arms only when a
   quarter of the window has passed, to avoid resetting a timer on every event. A
   middleware also re-arms the deadline at the start of every request, so a stale deadline
   from a previous request on a keep-alive connection cannot fire.
3. **Watchdog for first-token and idle timeouts.** Each streaming attempt gets a lazy
   timer. Every event stores a timestamp with an atomic write (`kick()`). When the timer
   fires, it checks the real deadline and either re-arms or cancels the attempt context
   with a cause (`errFirstTokenTimeout` or `errIdleTimeout`). Context causes let the
   gateway tell a stalled provider from a vanished client, a request deadline, or a
   shutdown.
4. **Flush coalescing that never holds an event back.** After writing an event, the
   gateway flushes unless another *complete* event is already buffered. An earlier version
   checked only "any bytes buffered". A test showed it could hold a complete event for
   200 ms behind half of the next one, and that bug is now covered by
   `TestEventIsNotHeldBehindPartialEvent`.
5. **Cancel before close; drain after `[DONE]`.**
   - On failure, the attempt context is cancelled before the body is closed, so a live
     stream is aborted rather than drained.
   - On success, the gateway reads up to 4 KiB for at most 100 ms after `[DONE]`. That
     consumes the chunked terminator so the upstream connection returns to the pool.
   - Without this, a test showed 10 requests opening 10 upstream connections; with it,
     they open 1.
6. **Body reads have their own deadline.** `timeouts.body_read` applies to the request
   body, and `ReadTimeout` stays zero. A slow upload gets a 408 with `Connection: close`.
   Without `Connection: close`, net/http would try to drain the unread body before sending
   the response and would wait on the same slow client. A test caught this.
7. **SSE readers are pooled.** A `sync.Pool` recycles each reader and its 4 KiB buffer.
   Profiling showed the per-stream read buffer was the largest single allocation on the
   request path. Pooling cut the gateway's bytes allocated per streamed request roughly
   in half.

## Consequences

Each stream costs about four goroutines, a fixed cost of net/http's HTTP/1.1 server and
client:

- the connection-serving goroutine;
- the background read that detects client disconnects;
- the transport's `readLoop`;
- the transport's `writeLoop`.

At thousands of open streams, goroutine stacks dominate memory (`docs/benchmarks.md`).
HTTP/2 to upstreams would remove two of those per stream, and that is listed as future work.
