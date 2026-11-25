package gateway

// Failure-matrix tests. Each test pins down one row of
// docs/failure-semantics.md: what the client sees, whether the gateway
// retries, and what happens to the upstream request.

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/mock"
	"github.com/modelmesh/modelmesh/internal/sse"
)

func healthy(name string) mockSpec { return mockSpec{name: name} }

func TestStreamSuccessFiltersInjectedUsageChunk(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, nil)

	// The client did not ask for usage: the gateway requested it upstream
	// for accounting but must not leak the extra chunk to the client.
	resp := h.post(context.Background(), chatBody("a-only", true, ""))
	if resp.StatusCode != 200 || resp.Header.Get("X-ModelMesh-Provider") != "a" {
		t.Fatalf("status=%d provider=%q", resp.StatusCode, resp.Header.Get("X-ModelMesh-Provider"))
	}
	r := readStream(resp)
	if !r.done || r.content != 8 || r.usage != nil || r.err != nil {
		t.Fatalf("stream = %+v", r)
	}
	eventually(t, "reported usage recorded", func() bool {
		return h.gw.m.tokens.With("a-only", "a", "completion", "reported").Value() == 8
	})

	// The client asked for usage: it receives the usage chunk.
	resp = h.post(context.Background(), chatBody("a-only", true, `"stream_options":{"include_usage":true}`))
	r = readStream(resp)
	if !r.done || r.usage == nil || r.usage.CompletionTokens != 8 {
		t.Fatalf("stream with usage = %+v", r)
	}
	eventually(t, "two successes", func() bool { return h.requests("a-only", outSuccess) == 2 })
}

func TestUpstreamConnectionsAreReused(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, nil)
	for i := 0; i < 50; i++ {
		if r := readStream(h.post(context.Background(), chatBody("a-only", i%2 == 0, ""))); r.err != nil {
			t.Fatal(r.err)
		}
	}
	if n := h.conns["a"].Load(); n > 2 {
		t.Fatalf("50 sequential requests opened %d upstream connections; expected keep-alive reuse", n)
	}
}

func TestConnectionReusedWhenTerminatorArrivesLate(t *testing.T) {
	// The upstream flushes [DONE] and only ends the HTTP response 20ms
	// later. Without draining after [DONE] the gateway would close (and
	// later re-dial) the connection on every request.
	var conns atomic.Int64
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond)
	}))
	upstream.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()
	h := newHarness(t, nil, func(c *config.Config) {
		c.Providers = []config.Provider{{Name: "u", BaseURL: upstream.URL + "/v1", MaxConcurrent: 4}}
		c.Models = []config.Model{{Name: "u", Targets: []config.Target{{Provider: "u", Model: "x"}}}}
	})
	for i := 0; i < 10; i++ {
		if r := readStream(h.post(context.Background(), chatBody("u", true, ""))); !r.done {
			t.Fatalf("stream %d incomplete: %+v", i, r)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("10 requests used %d upstream connections, want 1", n)
	}
}

func TestEventIsNotHeldBehindPartialEvent(t *testing.T) {
	// After the first event, the upstream sends one complete event plus half
	// of the next, then pauses. Flush coalescing must not hold the complete
	// event back while waiting for the partial one.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"0\"}}]}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"de")
		w.(http.Flusher).Flush()
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "lta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	h := newHarness(t, nil, func(c *config.Config) {
		c.Providers = []config.Provider{{Name: "u", BaseURL: upstream.URL + "/v1", MaxConcurrent: 4}}
		c.Models = []config.Model{{Name: "u", Targets: []config.Target{{Provider: "u", Model: "x"}}}}
	})
	resp := h.post(context.Background(), chatBody("u", true, ""))
	defer resp.Body.Close()
	rd := sse.NewReader(resp.Body, 1<<20)
	if _, err := rd.Next(); err != nil {
		t.Fatal(err)
	}
	first := time.Now()
	ev, err := rd.Next()
	if err != nil || !strings.Contains(string(ev), `"a"`) {
		t.Fatalf("second event = %q, %v", ev, err)
	}
	if gap := time.Since(first); gap > 120*time.Millisecond {
		t.Fatalf("complete event delivered %v after the previous one; it was held behind a partial event", gap)
	}
	if ev, err := rd.Next(); err != nil || !strings.Contains(string(ev), `"b"`) {
		t.Fatalf("third event = %q, %v", ev, err)
	}
}

func TestNonStreamSuccess(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, nil)
	resp := h.post(context.Background(), chatBody("a-only", false, ""), "X-Request-Id", "client-req-1")
	defer resp.Body.Close()
	var out struct {
		Object string `json:"object"`
		Usage  struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || out.Object != "chat.completion" || out.Usage.CompletionTokens != 8 {
		t.Fatalf("status=%d body=%+v", resp.StatusCode, out)
	}
	if resp.Header.Get("X-Request-Id") != "client-req-1" {
		t.Fatalf("request id not echoed: %q", resp.Header.Get("X-Request-Id"))
	}
}

func TestFailoverOn5xxBeforeCommit(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1 }}, healthy("b")}, nil)
	for _, stream := range []bool{true, false} {
		resp := h.post(context.Background(), chatBody("m", stream, ""))
		if resp.StatusCode != 200 || resp.Header.Get("X-ModelMesh-Provider") != "b" || resp.Header.Get("X-ModelMesh-Attempts") != "2" {
			t.Fatalf("stream=%v status=%d headers=%v", stream, resp.StatusCode, resp.Header)
		}
		if stream {
			if r := readStream(resp); !r.done {
				t.Fatalf("stream not completed: %+v", r)
			}
		} else {
			resp.Body.Close()
		}
	}
	if h.gw.m.failovers.With("m").Value() != 2 {
		t.Fatalf("failovers = %v", h.gw.m.failovers.With("m").Value())
	}
}

func TestRateLimitedProviderCoolsDownAndIsSkipped(t *testing.T) {
	h := newHarness(t, []mockSpec{
		{"a", func(p *mock.Profile) { p.RateLimitRate = 1; p.RetryAfter = config.Duration(5 * time.Second) }},
		healthy("b"),
	}, nil)
	for i := 0; i < 3; i++ {
		resp := h.post(context.Background(), chatBody("m", false, ""))
		resp.Body.Close()
		if resp.StatusCode != 200 || resp.Header.Get("X-ModelMesh-Provider") != "b" {
			t.Fatalf("request %d: status=%d provider=%q", i, resp.StatusCode, resp.Header.Get("X-ModelMesh-Provider"))
		}
	}
	if n := h.mockRequests("a"); n != 1 {
		t.Fatalf("provider a should be skipped during its Retry-After cooldown; got %d requests", n)
	}
	if h.gw.m.skips.With("a", "cooldown").Value() != 2 {
		t.Fatalf("cooldown skips = %v", h.gw.m.skips.With("a", "cooldown").Value())
	}
}

func TestAllProvidersRateLimitedReturns429WithRetryAfter(t *testing.T) {
	h := newHarness(t, []mockSpec{
		{"a", func(p *mock.Profile) { p.RateLimitRate = 1; p.RetryAfter = config.Duration(3 * time.Second) }},
	}, nil)
	resp := h.post(context.Background(), chatBody("a-only", true, ""))
	e := decodeError(t, resp)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "3" || e.Code != "upstream_rate_limited" {
		t.Fatalf("status=%d retry-after=%q err=%+v", resp.StatusCode, resp.Header.Get("Retry-After"), e)
	}
	if h.mockRequests("a") != 1 {
		t.Fatalf("a rate-limited provider must not be hammered: %d requests", h.mockRequests("a"))
	}
}

func TestRetriesAreBoundedByMaxAttempts(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1 }}}, nil)
	resp := h.post(context.Background(), chatBody("a-only", true, ""))
	e := decodeError(t, resp)
	if resp.StatusCode != 502 || e.Code != "upstream_error" {
		t.Fatalf("status=%d err=%+v", resp.StatusCode, e)
	}
	if n := h.mockRequests("a"); n != 3 {
		t.Fatalf("expected max_attempts=3 upstream requests, got %d", n)
	}
	if h.gw.m.retries.With("a-only").Value() != 2 {
		t.Fatalf("retries = %v", h.gw.m.retries.With("a-only").Value())
	}
}

func TestRetryBudgetPreventsRetryStorm(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1 }}}, func(c *config.Config) {
		c.Retry.BudgetRatio = 0
		c.Retry.BudgetMinPerSecond = 0 // budget starts with 10 tokens and never refills
		c.Breaker.FailureThreshold = 1000
	})
	for i := 0; i < 8; i++ {
		resp := h.post(context.Background(), chatBody("a-only", false, ""))
		resp.Body.Close()
		if resp.StatusCode != 502 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	}
	// 5 requests x 3 attempts use the 10 retry tokens; the remaining 3
	// requests get exactly one attempt each.
	if n := h.mockRequests("a"); n != 18 {
		t.Fatalf("upstream requests = %d, want 18", n)
	}
	if v := h.gw.m.budgetExhausted.Value(); v != 3 {
		t.Fatalf("budget exhausted = %v, want 3", v)
	}
}

func TestFailoverDoesNotSpendRetryBudget(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1 }}, healthy("b")}, func(c *config.Config) {
		c.Retry.BudgetRatio = 0
		c.Retry.BudgetMinPerSecond = 0
		c.Breaker.FailureThreshold = 1000
	})
	for i := 0; i < 30; i++ { // far more failovers than the 10-token budget
		resp := h.post(context.Background(), chatBody("m", false, ""))
		resp.Body.Close()
		if resp.StatusCode != 200 || resp.Header.Get("X-ModelMesh-Provider") != "b" {
			t.Fatalf("request %d: status=%d provider=%q", i, resp.StatusCode, resp.Header.Get("X-ModelMesh-Provider"))
		}
	}
	if h.gw.m.budgetExhausted.Value() != 0 || h.gw.budget.Tokens() < 9.99 {
		t.Fatalf("failover consumed retry budget: exhausted=%v tokens=%v", h.gw.m.budgetExhausted.Value(), h.gw.budget.Tokens())
	}
}

func TestFirstTokenTimeoutFailsOver(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.HangRate = 1 }}, healthy("b")}, nil)
	start := time.Now()
	resp := h.post(context.Background(), chatBody("m", true, ""))
	r := readStream(resp)
	elapsed := time.Since(start)
	if resp.Header.Get("X-ModelMesh-Provider") != "b" || !r.done {
		t.Fatalf("provider=%q stream=%+v", resp.Header.Get("X-ModelMesh-Provider"), r)
	}
	if elapsed < 300*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("elapsed %v; expected first_token timeout (300ms) then failover", elapsed)
	}
	eventually(t, "hung upstream request cancelled", func() bool { return h.mocks["a"].Stats().Cancelled == 1 })
	if h.gw.m.attempts.With("a", "first_token_timeout").Value() != 1 {
		t.Fatal("first_token_timeout attempt not recorded")
	}
}

func TestPreCommitStreamFaultsFailOver(t *testing.T) {
	cases := map[string]func(*mock.Profile){
		"malformed first event":  func(p *mock.Profile) { p.MalformedRate = 1; p.FaultAfterTokens = -1 },
		"cut before first event": func(p *mock.Profile) { p.CutRate = 1; p.FaultAfterTokens = -1 },
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, []mockSpec{{"a", set}, healthy("b")}, nil)
			resp := h.post(context.Background(), chatBody("m", true, ""))
			r := readStream(resp)
			if resp.Header.Get("X-ModelMesh-Provider") != "b" || !r.done || r.content != 8 {
				t.Fatalf("provider=%q stream=%+v", resp.Header.Get("X-ModelMesh-Provider"), r)
			}
		})
	}
}

func TestNonStreamBrokenResponsesFailOver(t *testing.T) {
	cases := map[string]func(*mock.Profile){
		"malformed json": func(p *mock.Profile) { p.MalformedRate = 1 },
		"connection cut": func(p *mock.Profile) { p.CutRate = 1 },
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, []mockSpec{{"a", set}, healthy("b")}, nil)
			resp := h.post(context.Background(), chatBody("m", false, ""))
			resp.Body.Close()
			if resp.StatusCode != 200 || resp.Header.Get("X-ModelMesh-Provider") != "b" {
				t.Fatalf("status=%d provider=%q", resp.StatusCode, resp.Header.Get("X-ModelMesh-Provider"))
			}
		})
	}
}

func TestEarlyStreamStallUsesIdleTimeout(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) {
		p.FaultAfterTokens = 1
		p.StallRate = 1
	}}}, func(c *config.Config) {
		c.Timeouts.FirstToken = config.Duration(10 * time.Second)
		c.Timeouts.Idle = config.Duration(100 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp := h.post(ctx, chatBody("a-only", true, ""))
	r := readStream(resp)
	if r.content != 1 || r.errEv == nil || r.errEv.Code != "upstream_stalled" {
		t.Fatalf("expected one chunk followed by an idle timeout, got %+v", r)
	}
}

func TestMidStreamFaultsAreReportedInBandAndNotRetried(t *testing.T) {
	cases := []struct {
		name string
		set  func(*mock.Profile)
		code string
	}{
		{"stall", func(p *mock.Profile) { p.StallRate = 1 }, "upstream_stalled"},
		{"cut", func(p *mock.Profile) { p.CutRate = 1 }, "upstream_interrupted"},
		{"malformed", func(p *mock.Profile) { p.MalformedRate = 1 }, "upstream_malformed_response"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			set := func(p *mock.Profile) { p.FaultAfterTokens = 3; c.set(p) }
			h := newHarness(t, []mockSpec{{"a", set}, healthy("b")}, nil)
			resp := h.post(context.Background(), chatBody("m", true, ""))
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			r := readStream(resp)
			if r.done || r.errEv == nil || r.errEv.Code != c.code || r.content != 3 {
				t.Fatalf("stream = %+v errEv=%+v", r, r.errEv)
			}
			if h.mockRequests("b") != 0 {
				t.Fatal("a stream must never be retried after the commit point")
			}
			eventually(t, "interrupted outcome", func() bool { return h.requests("m", outInterrupted) == 1 })
			if c.name == "stall" {
				eventually(t, "stalled upstream cancelled", func() bool { return h.mocks["a"].Stats().Cancelled == 1 })
			}
		})
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 50; p.OutputTokens = 10000 }}}, nil)
	cancelled := make(chan time.Time, 1)
	h.mocks["a"].SetOnCancel(func() { cancelled <- time.Now() })

	ctx, cancel := context.WithCancel(context.Background())
	resp := h.post(ctx, chatBody("a-only", true, ""))
	buf := make([]byte, 512)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	clientGone := time.Now()
	cancel()
	resp.Body.Close()

	select {
	case at := <-cancelled:
		if lag := at.Sub(clientGone); lag > 500*time.Millisecond {
			t.Fatalf("upstream cancellation took %v", lag)
		} else {
			t.Logf("client disconnect reached the provider in %v", lag)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("provider never observed the cancellation")
	}
	eventually(t, "gateway released the request", func() bool {
		return h.gw.m.inflight.Value() == 0 && h.gw.m.streams.Value() == 0 && h.mocks["a"].Stats().Active == 0
	})
	eventually(t, "client_closed outcome", func() bool { return h.requests("a-only", outClientClosed) == 1 })
}

func TestOverloadIsShedImmediately(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 20; p.OutputTokens = 1000 }}},
		func(c *config.Config) { c.Limits.MaxConcurrentRequests = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := h.post(ctx, chatBody("a-only", true, ""))
	defer first.Body.Close()
	if _, err := first.Body.Read(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	resp := h.post(context.Background(), chatBody("a-only", true, ""))
	e := decodeError(t, resp)
	if resp.StatusCode != 503 || e.Code != "gateway_overloaded" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status=%d err=%+v", resp.StatusCode, e)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("load shedding must not queue")
	}
	if h.mockRequests("a") != 1 {
		t.Fatal("shed request must not reach the provider")
	}
}

func TestProviderConcurrencyLimitSkipsToNextTarget(t *testing.T) {
	h := newHarness(t, []mockSpec{
		{"a", func(p *mock.Profile) { p.TokensPerSecond = 20; p.OutputTokens = 1000 }}, healthy("b"),
	}, func(c *config.Config) { c.Providers[0].MaxConcurrent = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := h.post(ctx, chatBody("m", true, ""))
	defer first.Body.Close()
	if first.Header.Get("X-ModelMesh-Provider") != "a" {
		t.Fatal("first request should use a")
	}
	resp := h.post(context.Background(), chatBody("m", false, ""))
	resp.Body.Close()
	if resp.Header.Get("X-ModelMesh-Provider") != "b" || resp.Header.Get("X-ModelMesh-Attempts") != "1" {
		t.Fatalf("expected skip to b without an attempt on a; headers=%v", resp.Header)
	}
}

func TestAuthenticationAndPerKeyRateLimit(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, func(c *config.Config) {
		c.Limits.PerKeyRPS = 1
		c.Limits.PerKeyBurst = 1
	})
	for hdr, code := range map[string]string{"": "missing_api_key", "Bearer wrong-key-0123456789": "invalid_api_key"} {
		req, _ := http.NewRequest(http.MethodPost, h.ts.URL+"/v1/chat/completions", strings.NewReader(chatBody("a-only", false, "")))
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		resp, err := h.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if e := decodeError(t, resp); resp.StatusCode != 401 || e.Code != code {
			t.Fatalf("auth %q: status=%d err=%+v", hdr, resp.StatusCode, e)
		}
	}
	resp := h.post(context.Background(), chatBody("a-only", false, ""))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("first request status = %d", resp.StatusCode)
	}
	resp = h.post(context.Background(), chatBody("a-only", false, ""))
	if e := decodeError(t, resp); resp.StatusCode != 429 || e.Code != "rate_limit_exceeded" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("rate limit: status=%d err=%+v", resp.StatusCode, e)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, func(c *config.Config) { c.Limits.MaxBodyBytes = 256 })
	cases := []struct {
		body   string
		status int
		code   string
	}{
		{`{not json`, 400, "invalid_request"},
		{`{"model":"a-only","messages":[]}`, 400, "invalid_request"},
		{chatBody("nope", false, ""), 404, "model_not_found"},
		{chatBody("a-only", false, `"pad":"`+strings.Repeat("x", 300)+`"`), 413, "request_too_large"},
	}
	for _, c := range cases {
		resp := h.post(context.Background(), c.body)
		if e := decodeError(t, resp); resp.StatusCode != c.status || e.Code != c.code {
			t.Errorf("body %.40q: status=%d err=%+v", c.body, resp.StatusCode, e)
		}
	}
	if h.mockRequests("a") != 0 {
		t.Fatal("invalid requests must not reach providers")
	}
	resp, err := h.client.Get(h.ts.URL + "/v2/unknown")
	if err != nil {
		t.Fatal(err)
	}
	if e := decodeError(t, resp); resp.StatusCode != 404 || e.Type != "not_found_error" {
		t.Fatalf("unknown path: %d %+v", resp.StatusCode, e)
	}
}

func TestUpstreamClientErrorIsForwardedNotRetried(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1; p.ErrorStatus = 422 }}, healthy("b")}, nil)
	resp := h.post(context.Background(), chatBody("m", false, ""))
	e := decodeError(t, resp)
	if resp.StatusCode != 422 || e.Message != "mock injected failure" {
		t.Fatalf("status=%d err=%+v (upstream body should pass through)", resp.StatusCode, e)
	}
	if h.mockRequests("b") != 0 {
		t.Fatal("a request the provider rejected as invalid must not be failed over")
	}
}

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.ErrorRate = 1 }}, healthy("b")}, func(c *config.Config) {
		c.Breaker.FailureThreshold = 2
		c.Breaker.OpenDuration = config.Duration(300 * time.Millisecond)
	})
	send := func() *http.Response {
		resp := h.post(context.Background(), chatBody("m", false, ""))
		resp.Body.Close()
		return resp
	}
	send()
	send()
	if h.mockRequests("a") != 2 {
		t.Fatalf("a requests = %d", h.mockRequests("a"))
	}
	resp := send()
	if h.mockRequests("a") != 2 || resp.Header.Get("X-ModelMesh-Attempts") != "1" {
		t.Fatal("open breaker must skip provider a without an attempt")
	}

	p := h.mocks["a"].Profile()
	p.ErrorRate = 0
	if err := h.mocks["a"].SetProfile(p); err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	resp = send()
	if resp.Header.Get("X-ModelMesh-Provider") != "a" {
		t.Fatalf("half-open probe should go to a; got %q", resp.Header.Get("X-ModelMesh-Provider"))
	}
	for _, to := range []string{"open", "half_open", "closed"} {
		if h.gw.m.breakerTransitions.With("a", to).Value() != 1 {
			t.Errorf("transition to %s not recorded once", to)
		}
	}
}

func TestBreakerCountsStreamSuccessAtCommit(t *testing.T) {
	// failure, then a long stream starts, then two failures. The stream is a
	// success that happened between the failures, so with a threshold of 3
	// the breaker must stay closed. (When success was recorded only at the
	// end of the stream, this sequence tripped it.)
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 20; p.OutputTokens = 40 }}},
		func(c *config.Config) { c.Breaker.FailureThreshold = 3; c.Retry.MaxAttempts = 1 })
	setErr := func(rate float64) {
		p := h.mocks["a"].Profile()
		p.ErrorRate = rate
		if err := h.mocks["a"].SetProfile(p); err != nil {
			t.Fatal(err)
		}
	}
	fail := func() {
		resp := h.post(context.Background(), chatBody("a-only", false, ""))
		resp.Body.Close()
		if resp.StatusCode != 502 {
			t.Fatalf("expected 502, got %d", resp.StatusCode)
		}
	}
	setErr(1)
	fail()
	setErr(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := h.post(ctx, chatBody("a-only", true, ""))
	defer stream.Body.Close()
	if _, err := stream.Body.Read(make([]byte, 64)); err != nil { // committed
		t.Fatal(err)
	}
	setErr(1)
	fail()
	fail()
	if st := h.gw.providers[0].Breaker.State().String(); st != "closed" {
		t.Fatalf("breaker = %s; a started stream must count as a success", st)
	}
}

func TestRequestDeadlineEndsStreamInBand(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 20; p.OutputTokens = 1000 }}},
		func(c *config.Config) { c.Timeouts.Request = config.Duration(400 * time.Millisecond) })
	start := time.Now()
	r := readStream(h.post(context.Background(), chatBody("a-only", true, "")))
	if r.errEv == nil || r.errEv.Code != "request_timeout" || r.done {
		t.Fatalf("stream = %+v", r)
	}
	if el := time.Since(start); el < 400*time.Millisecond || el > 2*time.Second {
		t.Fatalf("elapsed %v", el)
	}
	eventually(t, "timeout outcome", func() bool { return h.requests("a-only", outTimeout) == 1 })
}

func TestRequestRewritingAndHeaderHygiene(t *testing.T) {
	var gotBody atomic.Pointer[map[string]any]
	var gotHeaders atomic.Pointer[http.Header]
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotBody.Store(&body)
		hdr := r.Header.Clone()
		gotHeaders.Store(&hdr)
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer upstream.Close()

	h := newHarness(t, nil, func(c *config.Config) {
		c.Providers = []config.Provider{{Name: "cap", BaseURL: upstream.URL + "/v1", MaxConcurrent: 4, APIKey: "sk-provider-secret"}}
		c.Models = []config.Model{{Name: "alias", Targets: []config.Target{{Provider: "cap", Model: "real-model:7b"}}}}
	})
	r := readStream(h.post(context.Background(),
		chatBody("alias", true, `"temperature":0.3,"x_vendor":{"keep":true}`), "X-Request-Id", "trace-abc"))
	if !r.done || r.content != 1 || r.usage != nil {
		t.Fatalf("stream = %+v", r)
	}
	body, hdr := *gotBody.Load(), *gotHeaders.Load()
	if body["model"] != "real-model:7b" || body["temperature"] != 0.3 {
		t.Errorf("model/temperature not forwarded correctly: %v", body)
	}
	if v, ok := body["x_vendor"].(map[string]any); !ok || v["keep"] != true {
		t.Errorf("unknown field not preserved: %v", body)
	}
	if so, _ := body["stream_options"].(map[string]any); so["include_usage"] != true {
		t.Errorf("include_usage not injected: %v", body)
	}
	if hdr.Get("Authorization") != "Bearer sk-provider-secret" {
		t.Errorf("provider credential not sent; client key must never be forwarded: %q", hdr.Get("Authorization"))
	}
	if hdr.Get("X-Request-Id") != "trace-abc" {
		t.Errorf("request id not propagated: %q", hdr.Get("X-Request-Id"))
	}
}
