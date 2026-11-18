package gateway

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/resilience"
	"github.com/modelmesh/modelmesh/internal/sse"
)

var doneEvent = []byte("data: [DONE]\n\n")

// readerPool recycles SSE readers (and their read buffers) across streams;
// profiling showed the per-stream buffer was the largest single allocation
// in the gateway path.
var readerPool = sync.Pool{New: func() any { return sse.NewReader(nil, 0) }}

// drainTimeout bounds how long the gateway waits for the end of an upstream
// response after [DONE] in order to reuse the connection.
const drainTimeout = 100 * time.Millisecond

// stream handles a 2xx streaming response.
//
// Before the first event arrives nothing has been sent to the client, so
// any failure is returned to route() and may be retried elsewhere. The
// first valid event is the commit point: headers and that event are
// written and flushed, and from then on failures are reported in-band as an
// SSE error event followed by closing the stream — never retried, because
// the client already holds part of an answer.
//
// One goroutine (the handler's) owns both the upstream body and the
// ResponseWriter. Reads and writes alternate synchronously, so a slow client
// slows reading from the provider through TCP flow control, and memory per
// stream stays bounded by one event.
func (g *Gateway) stream(ctx, actx context.Context, cancel context.CancelCauseFunc, w http.ResponseWriter,
	rc *http.ResponseController, st *reqState, t target, resp *http.Response, wd *watchdog, sent time.Time) (bool, *failure) {
	p := t.p
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != "text/event-stream" {
		return g.attemptFailed(p, &failure{result: "malformed", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_malformed_response",
			outcome: outUpstreamError, msg: "upstream did not return an event stream"})
	}

	rd := readerPool.Get().(*sse.Reader)
	rd.Reset(resp.Body, g.cfg.Limits.MaxEventBytes)
	defer func() {
		rd.Reset(nil, 0) // drop the reference to the body before pooling
		readerPool.Put(rd)
	}()
	first, err := rd.Next()
	if err != nil {
		return g.attemptFailed(p, g.preCommitStreamFailure(ctx, actx, err))
	}
	info := api.InspectChunk(first)
	switch {
	case string(first) == "[DONE]" || !info.Valid:
		return g.attemptFailed(p, &failure{result: "malformed", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_malformed_response",
			outcome: outUpstreamError, msg: "upstream sent an invalid first stream event"})
	case info.ErrorBody:
		return g.attemptFailed(p, &failure{result: "error_event", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_error",
			outcome: outUpstreamError, msg: "upstream reported an error before streaming"})
	}

	// ---- commit point ----
	// The provider has started a valid response: count it as healthy now,
	// not when the stream ends. Recording successes only at stream end while
	// failures are recorded instantly makes a consecutive-failure breaker
	// trip on a modestly flaky provider (found by the faults benchmark).
	p.Breaker.Done(resilience.Success)
	wd.kick()
	g.m.upstreamTTFT.With(p.Name).Observe(g.now().Sub(sent).Seconds())
	st.committed, st.provider, st.upstreamModel = true, p, t.model
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	g.setRoutingHeaders(h, st)
	w.WriteHeader(http.StatusOK)
	g.m.streams.Add(1)
	defer g.m.streams.Add(-1)

	cw := &clientWriter{w: w, rc: rc, window: g.cfg.Timeouts.ClientWrite.D()}
	result, outcome := resilience.Success, outSuccess
	ev, evInfo := first, info
	for {
		g.account(st, evInfo)
		if !(evInfo.UsageOnly && !st.req.IncludeUsage) {
			if err := cw.event(ev); err != nil {
				result, outcome = resilience.Ignore, writeOutcome(err)
				break
			}
		}
		if st.ttfb == 0 || !rd.EventBuffered() {
			// Coalesce flushes: when another complete upstream event is
			// already buffered, write it first and flush once.
			if err := cw.flush(); err != nil {
				result, outcome = resilience.Ignore, writeOutcome(err)
				break
			}
			if st.ttfb == 0 {
				st.ttfb = g.now().Sub(st.start)
			}
		}

		ev, err = rd.Next()
		if err != nil {
			result, outcome = g.postCommitFailure(ctx, actx, cw, st, err)
			break
		}
		wd.kick()
		if string(ev) == "[DONE]" {
			if err := cw.raw(doneEvent); err != nil {
				result, outcome = resilience.Ignore, writeOutcome(err)
			}
			// Read the (tiny) rest of the body, normally just the chunked
			// terminator, so the upstream connection returns to the pool
			// instead of being closed. Bounded in size and time: an upstream
			// that keeps the response open after [DONE] loses its connection.
			stopDrain := time.AfterFunc(drainTimeout, func() { cancel(nil) })
			_, _ = io.CopyN(io.Discard, resp.Body, 4<<10)
			stopDrain.Stop()
			break
		}
		evInfo = api.InspectChunk(ev)
		if !evInfo.Valid {
			cw.errorEvent(api.TypeUpstream, "upstream_malformed_response", "upstream sent an invalid stream event")
			result, outcome = resilience.Failure, outInterrupted
			st.errCode = "upstream_malformed_response"
			break
		}
		if evInfo.ErrorBody {
			// Forward the provider's own error event, then end the stream.
			_ = cw.event(ev)
			_ = cw.flush()
			result, outcome = resilience.Failure, outInterrupted
			st.errCode = "upstream_error_event"
			break
		}
	}

	resultLabel := "success"
	if outcome != outSuccess {
		resultLabel = "stream_" + outcome
	}
	g.m.attempts.With(p.Name, resultLabel).Inc()
	if result == resilience.Failure {
		p.Breaker.Fail() // the stream broke after it had started
	}
	st.status, st.outcome = http.StatusOK, outcome
	return true, nil
}

func (g *Gateway) account(st *reqState, info api.ChunkInfo) {
	if info.HasContent {
		st.contentChunks++
	}
	if info.Usage != nil {
		st.usage = info.Usage
	}
}

// preCommitStreamFailure classifies a failure to read the first event.
func (g *Gateway) preCommitStreamFailure(ctx, actx context.Context, err error) *failure {
	if f := g.contextFailure(ctx, actx); f != nil {
		return f
	}
	if errors.Is(err, sse.ErrEventTooLarge) {
		return &failure{result: "malformed", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_malformed_response",
			outcome: outUpstreamError, msg: "upstream stream event exceeds max_event_bytes"}
	}
	return &failure{result: "interrupted", retryable: true, breaker: resilience.Failure,
		status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_interrupted",
		outcome: outUpstreamError, msg: "upstream closed the stream before sending any event"}
}

// postCommitFailure reports a mid-stream failure in-band and classifies it.
func (g *Gateway) postCommitFailure(ctx, actx context.Context, cw *clientWriter, st *reqState, err error) (resilience.Result, string) {
	if f := g.contextFailure(ctx, actx); f != nil {
		switch f.outcome {
		case outClientClosed:
			return resilience.Ignore, outClientClosed
		case outShutdown, outTimeout:
			if f.result == "request_timeout" || f.outcome == outShutdown {
				cw.errorEvent(f.errType, f.code, f.msg)
				st.errCode = f.code
				return resilience.Ignore, f.outcome
			}
			cw.errorEvent(f.errType, f.code, f.msg) // idle timeout: provider stalled
			st.errCode = f.code
			return resilience.Failure, outInterrupted
		}
	}
	code, msg := "upstream_interrupted", "upstream stream ended unexpectedly"
	switch {
	case errors.Is(err, sse.ErrEventTooLarge):
		code, msg = "upstream_malformed_response", "upstream stream event exceeds max_event_bytes"
	case errors.Is(err, io.EOF):
		msg = "upstream closed the stream without [DONE]"
	}
	cw.errorEvent(api.TypeUpstream, code, msg)
	st.errCode = code
	return resilience.Failure, outInterrupted
}

// clientWriter writes SSE to the client with a rolling per-write deadline
// so a client that stops reading cannot pin a goroutine and an upstream
// connection forever. The deadline is re-armed lazily to avoid resetting a
// timer on every event.
type clientWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	window  time.Duration
	lastArm time.Time
}

func (c *clientWriter) arm() {
	now := time.Now()
	if now.Sub(c.lastArm) > c.window/4 {
		_ = c.rc.SetWriteDeadline(now.Add(c.window))
		c.lastArm = now
	}
}

func (c *clientWriter) event(payload []byte) error {
	c.arm()
	return sse.WriteEvent(c.w, payload)
}

func (c *clientWriter) raw(b []byte) error {
	c.arm()
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	return c.rc.Flush()
}

func (c *clientWriter) flush() error {
	c.arm()
	return c.rc.Flush()
}

// errorEvent sends the in-band error used after the commit point. The
// stream then ends without [DONE], which OpenAI SDKs treat as incomplete.
func (c *clientWriter) errorEvent(typ, code, msg string) {
	b, _ := marshalError(typ, code, msg)
	_ = c.event(b)
	_ = c.flush()
}

// watchdog enforces the first-token and idle timeouts for one streaming
// attempt by cancelling the attempt context with a descriptive cause.
//
// Rather than resetting a timer for every event (hot path), kick() only
// records the time; when the timer fires it checks whether the deadline
// actually passed and re-arms itself otherwise.
type watchdog struct {
	mu      sync.Mutex
	timer   *time.Timer
	last    atomic.Int64 // unix nanos of the last event (or the start)
	started atomic.Bool  // first event seen; idle window applies
	first   time.Duration
	idle    time.Duration
	cancel  context.CancelCauseFunc
	stopped bool
}

func startWatchdog(first, idle time.Duration, cancel context.CancelCauseFunc) *watchdog {
	w := &watchdog{first: first, idle: idle, cancel: cancel}
	w.last.Store(time.Now().UnixNano())
	w.mu.Lock()
	w.timer = time.AfterFunc(first, w.fire)
	w.mu.Unlock()
	return w
}

func (w *watchdog) kick() {
	w.last.Store(time.Now().UnixNano())
	if !w.started.Load() {
		w.started.Store(true)
	}
}

func (w *watchdog) fire() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	window, cause := w.first, errFirstTokenTimeout
	if w.started.Load() {
		window, cause = w.idle, errIdleTimeout
	}
	remaining := time.Duration(w.last.Load()+int64(window)) - time.Duration(time.Now().UnixNano())
	if remaining > 0 {
		w.timer.Reset(remaining)
		return
	}
	w.cancel(cause)
}

func (w *watchdog) stop() {
	w.mu.Lock()
	w.stopped = true
	w.timer.Stop()
	w.mu.Unlock()
}
