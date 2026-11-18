package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/provider"
	"github.com/modelmesh/modelmesh/internal/resilience"
)

// Outcomes recorded in modelmesh_requests_total and the access log.
const (
	outSuccess        = "success"
	outBadRequest     = "bad_request"
	outModelNotFound  = "model_not_found"
	outRateLimited    = "rate_limited"
	outOverloaded     = "overloaded"
	outUpstreamClient = "upstream_client_error"
	outUpstreamError  = "upstream_error"
	outUpstreamLimit  = "upstream_rate_limited"
	outTimeout        = "timeout"
	outInterrupted    = "interrupted"
	outClientClosed   = "client_closed"
	outSlowClient     = "slow_client"
	outShutdown       = "shutdown"
)

// reqState accumulates what happened to one request for logs and metrics.
type reqState struct {
	id            string
	key           *apiKey
	start         time.Time
	route         *route
	req           *api.ChatRequest
	bodyLen       int
	attempts      int
	retries       int
	failovers     int
	provider      *provider.Provider
	upstreamModel string
	committed     bool
	ttfb          time.Duration
	usage         *api.Usage
	contentChunks int
	respBytes     int
	outcome       string
	status        int
	errCode       string
}

// failure describes why an attempt (or the request) did not succeed.
type failure struct {
	result     string // upstream attempt result label
	retryable  bool
	breaker    resilience.Result
	status     int
	errType    string
	code       string
	msg        string
	outcome    string
	retryAfter time.Duration
	passBody   []byte // upstream 4xx body forwarded verbatim
	passType   string
}

func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request) {
	g.active.Add(1)
	defer g.active.Done()
	st := &reqState{id: requestID(r.Context()), start: g.now()}
	st.key, _ = r.Context().Value(ctxAPIKey).(*apiKey)
	defer g.finish(st)

	if b := st.key.bucket; b != nil {
		if ok, wait := b.Allow(); !ok {
			g.m.shed.With("key_rate_limit").Inc()
			w.Header().Set("Retry-After", retryAfterHeader(wait))
			g.fail(w, st, http.StatusTooManyRequests, api.TypeRateLimit, "rate_limit_exceeded", outRateLimited,
				"rate limit exceeded for this API key")
			return
		}
	}

	if !g.admit.Acquire(r.Context(), g.cfg.Limits.AdmissionWait.D()) {
		g.m.shed.With("concurrency").Inc()
		w.Header().Set("Retry-After", "1")
		g.fail(w, st, http.StatusServiceUnavailable, api.TypeOverloaded, "gateway_overloaded", outOverloaded,
			"gateway is at its concurrency limit; retry later")
		return
	}
	defer g.admit.Release()
	g.m.inflight.Add(1)
	defer g.m.inflight.Add(-1)

	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(g.now().Add(g.cfg.Timeouts.BodyRead.D()))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.cfg.Limits.MaxBodyBytes))
	_ = rc.SetReadDeadline(time.Time{})
	if err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			g.fail(w, st, http.StatusRequestEntityTooLarge, api.TypeInvalidRequest, "request_too_large", outBadRequest,
				"request body exceeds "+strconv.FormatInt(g.cfg.Limits.MaxBodyBytes, 10)+" bytes")
		case errors.Is(err, os.ErrDeadlineExceeded):
			// Close the connection: otherwise net/http would try to discard
			// the unread body before sending this response, waiting on the
			// same slow client.
			w.Header().Set("Connection", "close")
			g.fail(w, st, http.StatusRequestTimeout, api.TypeInvalidRequest, "body_read_timeout", outBadRequest,
				"timed out reading request body")
		default:
			st.outcome = outClientClosed
		}
		return
	}
	st.bodyLen = len(body)

	req, rerr := api.ParseChatRequest(body)
	if rerr != nil {
		st.status, st.errCode, st.outcome = http.StatusBadRequest, "invalid_request", outBadRequest
		writeError(w, http.StatusBadRequest, api.TypeInvalidRequest, "invalid_request", rerr.Param, rerr.Message)
		return
	}
	st.req = req
	rt, ok := g.routes[req.Model]
	if !ok {
		g.fail(w, st, http.StatusNotFound, api.TypeNotFound, "model_not_found", outModelNotFound,
			"model "+strconv.Quote(req.Model)+" is not configured")
		return
	}
	st.route = rt
	g.budget.Deposit()

	ctx, cancel := context.WithTimeoutCause(r.Context(), g.cfg.Timeouts.Request.D(), errRequestTimeout)
	defer cancel()
	g.route(ctx, w, rc, st)
}

// route walks the target list until an attempt commits a response or the
// request runs out of attempts, budget, or time. Rules:
//   - a target is skipped (no attempt) while in 429 cooldown, while its
//     breaker is open, or while its concurrency limit is full;
//   - moving to a provider not yet tried (failover) happens immediately and
//     does not spend retry budget;
//   - re-trying a provider already tried for this request spends one
//     retry-budget token and waits a full-jitter backoff;
//   - nothing is retried once a byte has been sent to the client.
func (g *Gateway) route(ctx context.Context, w http.ResponseWriter, rc *http.ResponseController, st *reqState) {
	var last *failure
	var tried []*provider.Provider
	var minCooldown time.Duration
	skippedCooldown, skippedOther := 0, 0
	maxAttempts := g.cfg.Retry.MaxAttempts

	for st.attempts < maxAttempts {
		progressed := false
		for _, t := range st.route.targets {
			if st.attempts >= maxAttempts {
				break
			}
			if ctx.Err() != nil {
				g.writeFailure(w, st, g.contextFailure(ctx, ctx))
				return
			}
			p := t.p
			if rem := p.CooldownRemaining(); rem > 0 {
				g.m.skips.With(p.Name, "cooldown").Inc()
				skippedCooldown++
				if minCooldown == 0 || rem < minCooldown {
					minCooldown = rem
				}
				continue
			}
			if !p.Breaker.Allow() {
				g.m.skips.With(p.Name, "breaker_open").Inc()
				skippedOther++
				continue
			}
			if !p.Sem.TryAcquire() {
				p.Breaker.Done(resilience.Ignore)
				g.m.skips.With(p.Name, "saturated").Inc()
				skippedOther++
				continue
			}
			if st.attempts > 0 {
				if contains(tried, p) {
					// A retry against a provider that already failed this
					// request: this is what multiplies load on a struggling
					// provider, so it is paid for from the retry budget and
					// delayed by a jittered backoff.
					if !g.budget.TryWithdraw() {
						p.Sem.Release()
						p.Breaker.Done(resilience.Ignore)
						g.m.budgetExhausted.Inc()
						g.writeFailure(w, st, last)
						return
					}
					st.retries++
					g.m.retries.With(st.route.name).Inc()
					if !sleepCtx(ctx, resilience.Backoff(st.retries, g.cfg.Retry.BaseBackoff.D(), g.cfg.Retry.MaxBackoff.D())) {
						p.Sem.Release()
						p.Breaker.Done(resilience.Ignore)
						g.writeFailure(w, st, g.contextFailure(ctx, ctx))
						return
					}
				} else {
					// Failover to a provider not yet tried: immediate and not
					// budgeted. It adds no load to the failing provider, and
					// its amplification is bounded by the target list and
					// max_attempts.
					st.failovers++
					g.m.failovers.With(st.route.name).Inc()
				}
			}
			tried = append(tried, p)
			st.attempts++
			done, f := g.attempt(ctx, w, rc, st, t)
			p.Sem.Release()
			if done {
				return
			}
			last = f
			progressed = true
			if !f.retryable {
				g.writeFailure(w, st, f)
				return
			}
		}
		if !progressed {
			break
		}
	}

	if last == nil {
		// No attempt was possible at all.
		if skippedCooldown > 0 && skippedOther == 0 {
			last = &failure{status: http.StatusTooManyRequests, errType: api.TypeRateLimit, code: "upstream_rate_limited",
				outcome: outUpstreamLimit, retryAfter: minCooldown,
				msg: "all providers for this model are rate limited; retry later"}
		} else {
			last = &failure{status: http.StatusServiceUnavailable, errType: api.TypeOverloaded, code: "no_available_provider",
				outcome: outOverloaded, retryAfter: time.Second,
				msg: "no provider for this model is currently available (circuit open or at capacity)"}
		}
	}
	g.writeFailure(w, st, last)
}

// attempt performs one upstream call. It returns done=true when a response
// (success, or a terminal error) has been written to the client; otherwise
// it returns the failure, which the caller may retry.
func (g *Gateway) attempt(ctx context.Context, w http.ResponseWriter, rc *http.ResponseController, st *reqState, t target) (bool, *failure) {
	p := t.p
	body, err := st.req.UpstreamBody(t.model)
	if err != nil {
		p.Breaker.Done(resilience.Ignore)
		return false, &failure{result: "internal", status: http.StatusInternalServerError, errType: api.TypeServer,
			code: "internal_error", outcome: outUpstreamError, msg: "failed to encode upstream request"}
	}

	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wd *watchdog
	if st.req.Stream {
		wd = startWatchdog(g.cfg.Timeouts.FirstToken.D(), g.cfg.Timeouts.Idle.D(), cancel)
		defer wd.stop()
	} else {
		var stop context.CancelFunc
		actx, stop = context.WithTimeoutCause(actx, g.cfg.Timeouts.Attempt.D(), errAttemptTimeout)
		defer stop()
	}

	sent := g.now()
	resp, err := p.Do(actx, body, st.req.Stream, st.id)
	if err != nil {
		f := g.contextFailure(ctx, actx)
		if f == nil {
			f = &failure{result: "conn_error", retryable: true, breaker: resilience.Failure,
				status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_unreachable",
				outcome: outUpstreamError, msg: "could not reach upstream provider"}
			g.log.Debug("upstream transport error", "request_id", st.id, "provider", p.Name, "err", err)
		}
		return g.attemptFailed(p, f)
	}
	defer func() {
		// Cancel before closing: closing a body that is still streaming must
		// abort it, not try to drain it.
		cancel(nil)
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return g.attemptFailed(p, g.statusFailure(p, resp))
	}
	if st.req.Stream {
		return g.stream(ctx, actx, cancel, w, rc, st, t, resp, wd, sent)
	}
	return g.complete(ctx, actx, w, st, t, resp, sent)
}

func (g *Gateway) attemptFailed(p *provider.Provider, f *failure) (bool, *failure) {
	g.m.attempts.With(p.Name, f.result).Inc()
	p.Breaker.Done(f.breaker)
	return false, f
}

// statusFailure classifies a non-2xx upstream response.
func (g *Gateway) statusFailure(p *provider.Provider, resp *http.Response) *failure {
	code := resp.StatusCode
	switch {
	case code == http.StatusTooManyRequests:
		d, ok := resilience.ParseRetryAfter(resp.Header.Get("Retry-After"), g.now())
		if !ok || d <= 0 {
			d = g.cfg.Retry.DefaultCooldown.D()
		}
		d = min(d, g.cfg.Retry.MaxCooldown.D())
		p.CoolDown(d)
		return &failure{result: "rate_limited", retryable: true, breaker: resilience.Ignore,
			status: http.StatusTooManyRequests, errType: api.TypeRateLimit, code: "upstream_rate_limited",
			outcome: outUpstreamLimit, retryAfter: d, msg: "upstream provider rate limited the request"}
	case code == http.StatusRequestTimeout || code >= 500:
		return &failure{result: "error_" + strconv.Itoa(code), retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_error",
			outcome: outUpstreamError, msg: "upstream provider returned HTTP " + strconv.Itoa(code)}
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		g.log.Error("upstream rejected provider credentials", "provider", p.Name, "status", code)
		return &failure{result: "auth_error", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_auth_failed",
			outcome: outUpstreamError, msg: "upstream provider rejected the gateway's credentials"}
	case code == http.StatusNotFound:
		return &failure{result: "not_found", retryable: true, breaker: resilience.Ignore,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_model_not_found",
			outcome: outUpstreamError, msg: "upstream provider does not serve the configured model"}
	case code >= 400:
		// The request itself is invalid for the provider: retrying elsewhere
		// would fail the same way, so forward the provider's error verbatim.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		ct := resp.Header.Get("Content-Type")
		if !json.Valid(b) {
			b, ct = nil, ""
		}
		return &failure{result: "client_error", retryable: false, breaker: resilience.Ignore,
			status: code, errType: api.TypeInvalidRequest, code: "upstream_rejected_request",
			outcome: outUpstreamClient, msg: "upstream provider rejected the request", passBody: b, passType: ct}
	default:
		return &failure{result: "unexpected_status", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_error",
			outcome: outUpstreamError, msg: "unexpected upstream status " + strconv.Itoa(code)}
	}
}

// contextFailure explains a cancelled context, or returns nil if neither
// ctx (request) nor actx (attempt) is done.
func (g *Gateway) contextFailure(ctx, actx context.Context) *failure {
	if ctx.Err() != nil {
		switch cause := context.Cause(ctx); {
		case errors.Is(cause, errShutdown):
			return &failure{result: "shutdown", breaker: resilience.Ignore, status: http.StatusServiceUnavailable,
				errType: api.TypeOverloaded, code: "gateway_shutdown", outcome: outShutdown, msg: "gateway is shutting down"}
		case errors.Is(cause, errRequestTimeout):
			return &failure{result: "request_timeout", breaker: resilience.Ignore, status: http.StatusGatewayTimeout,
				errType: api.TypeTimeout, code: "request_timeout", outcome: outTimeout, msg: "request deadline exceeded"}
		default:
			return &failure{result: "client_closed", breaker: resilience.Ignore, outcome: outClientClosed}
		}
	}
	if actx.Err() == nil {
		return nil
	}
	switch cause := context.Cause(actx); {
	case errors.Is(cause, errFirstTokenTimeout):
		return &failure{result: "first_token_timeout", retryable: true, breaker: resilience.Failure,
			status: http.StatusGatewayTimeout, errType: api.TypeTimeout, code: "upstream_timeout",
			outcome: outTimeout, msg: "upstream did not start streaming within the first-token timeout"}
	case errors.Is(cause, errIdleTimeout):
		return &failure{result: "idle_timeout", retryable: true, breaker: resilience.Failure,
			status: http.StatusGatewayTimeout, errType: api.TypeTimeout, code: "upstream_stalled",
			outcome: outTimeout, msg: "upstream stream stalled"}
	case errors.Is(cause, errAttemptTimeout):
		return &failure{result: "attempt_timeout", retryable: true, breaker: resilience.Failure,
			status: http.StatusGatewayTimeout, errType: api.TypeTimeout, code: "upstream_timeout",
			outcome: outTimeout, msg: "upstream did not respond within the attempt timeout"}
	}
	return nil
}

// complete handles a non-streaming 2xx response. The body is read fully and
// validated before anything is written, so a broken upstream response can
// still be retried.
func (g *Gateway) complete(ctx, actx context.Context, w http.ResponseWriter, st *reqState, t target, resp *http.Response, sent time.Time) (bool, *failure) {
	p := t.p
	limit := g.cfg.Limits.MaxResponseBytes
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		f := g.contextFailure(ctx, actx)
		if f == nil {
			f = &failure{result: "interrupted", retryable: true, breaker: resilience.Failure,
				status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_interrupted",
				outcome: outUpstreamError, msg: "upstream response was cut off"}
		}
		return g.attemptFailed(p, f)
	}
	if int64(len(data)) > limit {
		return g.attemptFailed(p, &failure{result: "too_large", breaker: resilience.Ignore,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_response_too_large",
			outcome: outUpstreamError, msg: "upstream response exceeds max_response_bytes"})
	}
	if !json.Valid(data) {
		return g.attemptFailed(p, &failure{result: "malformed", retryable: true, breaker: resilience.Failure,
			status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_malformed_response",
			outcome: outUpstreamError, msg: "upstream returned an invalid JSON response"})
	}

	g.m.attempts.With(p.Name, "success").Inc()
	p.Breaker.Done(resilience.Success)
	g.m.upstreamTTFT.With(p.Name).Observe(g.now().Sub(sent).Seconds())
	st.committed, st.provider, st.upstreamModel = true, p, t.model
	st.usage = api.ResponseUsage(data)
	st.respBytes = len(data)

	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(data)))
	g.setRoutingHeaders(h, st)
	w.WriteHeader(http.StatusOK)
	st.ttfb = g.now().Sub(st.start)
	if _, err := w.Write(data); err != nil {
		st.outcome = writeOutcome(err)
		return true, nil
	}
	st.status, st.outcome = http.StatusOK, outSuccess
	return true, nil
}

func (g *Gateway) setRoutingHeaders(h http.Header, st *reqState) {
	h.Set("X-ModelMesh-Provider", st.provider.Name)
	h.Set("X-ModelMesh-Upstream-Model", st.upstreamModel)
	h.Set("X-ModelMesh-Attempts", strconv.Itoa(st.attempts))
}

// fail writes an error response for failures that happen before routing.
func (g *Gateway) fail(w http.ResponseWriter, st *reqState, status int, typ, code, outcome, msg string) {
	st.status, st.errCode, st.outcome = status, code, outcome
	writeError(w, status, typ, code, "", msg)
}

// writeFailure writes the final pre-commit error response.
func (g *Gateway) writeFailure(w http.ResponseWriter, st *reqState, f *failure) {
	if f == nil {
		f = &failure{status: http.StatusBadGateway, errType: api.TypeUpstream, code: "upstream_error",
			outcome: outUpstreamError, msg: "upstream request failed and the retry budget is exhausted"}
	}
	st.outcome, st.errCode = f.outcome, f.code
	if f.outcome == outClientClosed {
		return // nobody is listening
	}
	st.status = f.status
	if f.retryAfter > 0 {
		w.Header().Set("Retry-After", retryAfterHeader(f.retryAfter))
	}
	if f.passBody != nil {
		w.Header().Set("Content-Type", f.passType)
		w.WriteHeader(f.status)
		_, _ = w.Write(f.passBody)
		return
	}
	writeError(w, f.status, f.errType, f.code, "", f.msg)
}

// finish emits the access log line and request metrics.
func (g *Gateway) finish(st *reqState) {
	elapsed := g.now().Sub(st.start)
	model := "unknown"
	if st.route != nil {
		model = st.route.name
	}
	if st.outcome == "" {
		st.outcome = outUpstreamError
	}
	stream := "false"
	if st.req != nil && st.req.Stream {
		stream = "true"
	}
	g.m.requests.With(model, st.outcome).Inc()
	g.m.duration.With(model, stream).Observe(elapsed.Seconds())
	if st.committed {
		g.m.ttft.With(model).Observe(st.ttfb.Seconds())
	}

	var prompt, completion int
	source := "none"
	if st.provider != nil {
		if st.usage != nil {
			prompt, completion, source = st.usage.PromptTokens, st.usage.CompletionTokens, "reported"
		} else {
			prompt, source = api.EstimateTokens(st.bodyLen), "estimated"
			if st.req.Stream {
				completion = st.contentChunks
			} else {
				completion = api.EstimateTokens(st.respBytes)
			}
		}
		pn := st.provider.Name
		g.m.tokens.With(model, pn, "prompt", source).Add(float64(prompt))
		g.m.tokens.With(model, pn, "completion", source).Add(float64(completion))
		if c := (float64(prompt)*st.provider.PricePrompt + float64(completion)*st.provider.PriceCompletion) / 1000; c > 0 {
			g.m.cost.With(model, pn).Add(c)
		}
	}

	if !g.log.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	attrs := []slog.Attr{
		slog.String("request_id", st.id),
		slog.String("model", model),
		slog.Bool("stream", stream == "true"),
		slog.String("outcome", st.outcome),
		slog.Int("status", st.status),
		slog.Int("attempts", st.attempts),
		slog.Float64("duration_ms", ms(elapsed)),
	}
	if st.key != nil {
		attrs = append(attrs, slog.String("key", st.key.name))
	}
	if st.provider != nil {
		attrs = append(attrs, slog.String("provider", st.provider.Name), slog.String("upstream_model", st.upstreamModel),
			slog.Float64("ttfb_ms", ms(st.ttfb)), slog.Int("prompt_tokens", prompt),
			slog.Int("completion_tokens", completion), slog.String("usage_source", source))
	}
	if st.retries > 0 || st.failovers > 0 {
		attrs = append(attrs, slog.Int("retries", st.retries), slog.Int("failovers", st.failovers))
	}
	if st.errCode != "" {
		attrs = append(attrs, slog.String("error_code", st.errCode))
	}
	g.log.LogAttrs(context.Background(), slog.LevelInfo, "request", attrs...)
}

func ms(d time.Duration) float64 { return math.Round(float64(d.Microseconds())) / 1000 }

func writeOutcome(err error) string {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return outSlowClient
	}
	return outClientClosed
}

func retryAfterHeader(d time.Duration) string {
	return strconv.Itoa(int(math.Ceil(max(d.Seconds(), 1))))
}

func contains(ps []*provider.Provider, p *provider.Provider) bool {
	for _, x := range ps {
		if x == p {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
