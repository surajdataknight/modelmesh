// Package provider holds one upstream OpenAI-compatible endpoint together
// with the state the gateway keeps about it: its HTTP transport, its
// concurrency limit, its circuit breaker, and its rate-limit cooldown.
//
// There is a single concrete type rather than an interface: every MVP
// upstream speaks the OpenAI dialect. A second dialect adapter will
// introduce an interface when it exists, not before.
package provider

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/resilience"
)

// Provider is one upstream endpoint. It is safe for concurrent use.
type Provider struct {
	Name    string
	Breaker *resilience.Breaker
	Sem     *resilience.Semaphore

	chatURL   string
	authz     string
	client    *http.Client
	transport *http.Transport
	now       func() time.Time
	cooldown  atomic.Int64 // unix nanoseconds until which the provider is skipped

	PricePrompt, PriceCompletion float64 // USD per 1K tokens
}

// New builds a provider with its own connection pool.
//
// Transport choices:
//   - one Transport per provider, so a slow or failing provider cannot
//     exhaust connections that other providers need;
//   - MaxIdleConnsPerHost equals the provider's concurrency limit (the
//     standard library default of 2 forces constant reconnects under load);
//   - compression disabled, because transparent gzip buffers SSE and adds
//     CPU for no benefit on token streams;
//   - redirects are never followed, so credentials cannot leak to another
//     host and a compromised upstream cannot steer requests elsewhere;
//   - response deadlines come from per-attempt contexts, not from
//     Transport.ResponseHeaderTimeout, so streaming and non-streaming
//     requests can use different budgets.
func New(cfg config.Provider, br config.Breaker, now func() time.Time, onBreaker func(from, to resilience.State)) *Provider {
	if now == nil {
		now = time.Now
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConnsPerHost:    cfg.MaxConcurrent,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ExpectContinueTimeout:  time.Second,
		DisableCompression:     true,
		MaxResponseHeaderBytes: 64 << 10,
	}
	p := &Provider{
		Name:            cfg.Name,
		Breaker:         resilience.NewBreaker(br.FailureThreshold, br.OpenDuration.D(), br.HalfOpenProbes, now, onBreaker),
		Sem:             resilience.NewSemaphore(cfg.MaxConcurrent),
		chatURL:         strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions",
		transport:       tr,
		client:          &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		now:             now,
		PricePrompt:     cfg.PricePer1KPrompt,
		PriceCompletion: cfg.PricePer1KCompletion,
	}
	if cfg.APIKey != "" {
		p.authz = "Bearer " + cfg.APIKey
	}
	return p
}

// Do sends one chat completion request. The caller owns the response body.
func (p *Provider) Do(ctx context.Context, body []byte, stream bool, requestID string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.chatURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	h := req.Header
	h.Set("Content-Type", "application/json")
	if stream {
		h.Set("Accept", "text/event-stream")
	} else {
		h.Set("Accept", "application/json")
	}
	if p.authz != "" {
		h.Set("Authorization", p.authz)
	}
	if requestID != "" {
		h.Set("X-Request-Id", requestID)
	}
	h.Set("User-Agent", "modelmesh")
	return p.client.Do(req)
}

// CoolDown makes the provider ineligible for d (after a 429). An existing
// longer cooldown is kept.
func (p *Provider) CoolDown(d time.Duration) {
	until := p.now().Add(d).UnixNano()
	for {
		cur := p.cooldown.Load()
		if cur >= until || p.cooldown.CompareAndSwap(cur, until) {
			return
		}
	}
}

// CooldownRemaining returns how long the provider remains in cooldown.
func (p *Provider) CooldownRemaining() time.Duration {
	rem := p.cooldown.Load() - p.now().UnixNano()
	if rem <= 0 {
		return 0
	}
	return time.Duration(rem)
}

// CloseIdleConnections releases pooled connections (used at shutdown).
func (p *Provider) CloseIdleConnections() { p.transport.CloseIdleConnections() }
