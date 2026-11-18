// Package gateway implements the ModelMesh HTTP gateway: authentication,
// admission control, routing with retries and failover, and streaming
// proxying with a well-defined commit point (ADR 0003).
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/metrics"
	"github.com/modelmesh/modelmesh/internal/provider"
	"github.com/modelmesh/modelmesh/internal/resilience"
)

// Sentinel causes attached to context cancellation, so the gateway can
// tell apart the many reasons an upstream read stops.
var (
	errShutdown          = errors.New("gateway shutting down")
	errRequestTimeout    = errors.New("request deadline exceeded")
	errAttemptTimeout    = errors.New("upstream attempt timed out")
	errFirstTokenTimeout = errors.New("upstream first-token timeout")
	errIdleTimeout       = errors.New("upstream idle timeout")
)

// Gateway holds configuration, providers, routes, and shared limiters.
type Gateway struct {
	cfg       *config.Config
	log       *slog.Logger
	reg       *metrics.Registry
	m         *gwMetrics
	providers []*provider.Provider
	routes    map[string]*route
	models    []string
	keys      []*apiKey
	anonymous *apiKey
	admit     *resilience.Semaphore
	budget    *resilience.RetryBudget
	ready     atomic.Bool
	active    sync.WaitGroup
	now       func() time.Time
}

type route struct {
	name    string
	targets []target
}

type target struct {
	p     *provider.Provider
	model string
}

type apiKey struct {
	name   string
	hash   [sha256.Size]byte
	bucket *resilience.TokenBucket
}

// New builds a gateway from a validated configuration.
func New(cfg *config.Config, log *slog.Logger, reg *metrics.Registry) (*Gateway, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if reg == nil {
		reg = metrics.NewRegistry()
	}
	g := &Gateway{
		cfg:    cfg,
		log:    log,
		reg:    reg,
		routes: map[string]*route{},
		admit:  resilience.NewSemaphore(cfg.Limits.MaxConcurrentRequests),
		budget: resilience.NewRetryBudget(cfg.Retry.BudgetRatio, cfg.Retry.BudgetMinPerSecond, nil),
		now:    time.Now,
	}
	g.m = newMetrics(reg, g)

	byName := map[string]*provider.Provider{}
	for _, pc := range cfg.Providers {
		name := pc.Name
		p := provider.New(pc, cfg.Breaker, nil, func(from, to resilience.State) {
			g.m.breakerTransitions.With(name, to.String()).Inc()
			level := slog.LevelWarn
			if to == resilience.Closed {
				level = slog.LevelInfo
			}
			g.log.Log(context.Background(), level, "circuit breaker transition", "provider", name, "from", from.String(), "to", to.String())
		})
		byName[name] = p
		g.providers = append(g.providers, p)
	}
	for _, mc := range cfg.Models {
		r := &route{name: mc.Name}
		for _, t := range mc.Targets {
			p, ok := byName[t.Provider]
			if !ok {
				return nil, errors.New("unknown provider " + t.Provider)
			}
			r.targets = append(r.targets, target{p: p, model: t.Model})
		}
		g.routes[mc.Name] = r
		g.models = append(g.models, mc.Name)
	}

	newBucket := func() *resilience.TokenBucket {
		if cfg.Limits.PerKeyRPS <= 0 {
			return nil
		}
		return resilience.NewTokenBucket(cfg.Limits.PerKeyRPS, cfg.Limits.PerKeyBurst, nil)
	}
	for _, k := range cfg.Auth.Keys {
		g.keys = append(g.keys, &apiKey{name: k.Name, hash: sha256.Sum256([]byte(k.Value)), bucket: newBucket()})
	}
	if cfg.Auth.Mode == "none" {
		g.anonymous = &apiKey{name: "anonymous", bucket: newBucket()}
	}
	g.ready.Store(true)
	return g, nil
}

// SetReady toggles the readiness endpoint (false during shutdown).
func (g *Gateway) SetReady(ready bool) { g.ready.Store(ready) }

// Handler returns the public HTTP handler.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", g.authenticated(http.HandlerFunc(g.handleChat)))
	mux.Handle("GET /v1/models", g.authenticated(http.HandlerFunc(g.handleModels)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", g.handleReady)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, api.TypeNotFound, "not_found", "", "no route for "+r.Method+" "+r.URL.Path)
	})
	return g.withRequestContext(mux)
}

// AdminHandler serves metrics, pprof, and provider status. It must only be
// bound to a trusted interface (loopback by default).
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", g.reg.Handler())
	mux.HandleFunc("GET /admin/providers", g.handleProviders)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxAPIKey
)

// withRequestContext assigns a request ID and arms a write deadline for
// every request. The deadline matters on reused keep-alive connections:
// with Server.WriteTimeout disabled (required for long streams) a deadline
// left over from a previous request would otherwise still apply.
func (g *Gateway) withRequestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if !validRequestID(id) {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		_ = http.NewResponseController(w).SetWriteDeadline(g.now().Add(g.cfg.Timeouts.ClientWrite.D()))
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
	})
}

func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxRequestID).(string)
	return id
}

// authenticated enforces API keys. Keys are compared as SHA-256 digests in
// constant time, and every configured key is checked so timing does not
// reveal which key (if any) matched.
func (g *Gateway) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := g.anonymous
		if key == nil {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || token == "" {
				g.m.authFailures.Inc()
				writeError(w, http.StatusUnauthorized, api.TypeAuthentication, "missing_api_key", "", "missing bearer token in Authorization header")
				return
			}
			sum := sha256.Sum256([]byte(token))
			for _, k := range g.keys {
				if subtle.ConstantTimeCompare(sum[:], k.hash[:]) == 1 {
					key = k
				}
			}
			if key == nil {
				g.m.authFailures.Inc()
				writeError(w, http.StatusUnauthorized, api.TypeAuthentication, "invalid_api_key", "", "invalid API key")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxAPIKey, key)))
	})
}

func (g *Gateway) handleModels(w http.ResponseWriter, _ *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	data := make([]model, 0, len(g.models))
	for _, name := range g.models {
		data = append(data, model{ID: name, Object: "model", OwnedBy: "modelmesh"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (g *Gateway) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !g.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (g *Gateway) handleProviders(w http.ResponseWriter, _ *http.Request) {
	type status struct {
		Name        string  `json:"name"`
		Breaker     string  `json:"breaker"`
		InFlight    int     `json:"in_flight"`
		MaxInFlight int     `json:"max_in_flight"`
		CooldownS   float64 `json:"cooldown_seconds"`
	}
	out := make([]status, 0, len(g.providers))
	for _, p := range g.providers {
		out = append(out, status{
			Name: p.Name, Breaker: p.Breaker.State().String(), InFlight: p.Sem.InUse(),
			MaxInFlight: p.Sem.Cap(), CooldownS: p.CooldownRemaining().Seconds(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": out, "retry_budget_tokens": g.budget.Tokens()})
}

// CloseIdleConnections closes pooled upstream connections.
func (g *Gateway) CloseIdleConnections() {
	for _, p := range g.providers {
		p.CloseIdleConnections()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, code, param, msg string) {
	body := api.ErrorBody{Error: api.ErrorDetail{Message: msg, Type: typ, Code: code}}
	if param != "" {
		body.Error.Param = &param
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, body)
}

func marshalError(typ, code, msg string) ([]byte, error) {
	return json.Marshal(api.ErrorBody{Error: api.ErrorDetail{Message: msg, Type: typ, Code: code}})
}
