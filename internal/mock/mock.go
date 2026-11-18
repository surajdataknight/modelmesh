// Package mock implements mockllm, a fault-injecting OpenAI-compatible LLM
// provider. It exists so that every ModelMesh behavior — including every
// failure mode — can be developed, tested, and benchmarked at zero cost.
//
// The mock speaks the real wire format over real HTTP; it is not an
// in-process stub. Its behavior is described by a Profile that can be set at
// startup or changed at runtime through PUT /_mock/profile.
package mock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/modelmesh/modelmesh/internal/config"
)

// Profile describes latency and fault behavior. Rates are probabilities in
// [0,1]. Pre-response faults (hang, rate limit, error) are mutually
// exclusive and drawn first; mid-stream faults (stall, cut, malformed) are
// drawn independently for responses that start.
type Profile struct {
	TTFT            config.Duration `json:"ttft"`
	TTFTJitter      config.Duration `json:"ttft_jitter"`
	TokensPerSecond float64         `json:"tokens_per_second"`
	OutputTokens    int             `json:"output_tokens"`

	HangRate      float64         `json:"hang_rate"`
	RateLimitRate float64         `json:"rate_limit_rate"`
	RetryAfter    config.Duration `json:"retry_after"`
	ErrorRate     float64         `json:"error_rate"`
	ErrorStatus   int             `json:"error_status"`

	StallRate        float64 `json:"stall_rate"`
	CutRate          float64 `json:"cut_rate"`
	MalformedRate    float64 `json:"malformed_rate"`
	FaultAfterTokens int     `json:"fault_after_tokens"`

	// MaxConcurrent > 0 makes the mock answer 429 (overloaded) beyond that
	// many in-flight requests, like a saturated provider.
	MaxConcurrent int `json:"max_concurrent"`
	// Seed makes fault and text selection reproducible.
	Seed uint64 `json:"seed"`
}

// DefaultProfile is a fast, healthy provider.
func DefaultProfile() Profile {
	return Profile{
		TTFT:         config.Duration(50 * time.Millisecond),
		OutputTokens: 16,
		ErrorStatus:  http.StatusServiceUnavailable,
		RetryAfter:   config.Duration(time.Second),
		Seed:         1,
	}
}

// Validate checks the profile for impossible values.
func (p Profile) Validate() error {
	for name, r := range map[string]float64{
		"hang_rate": p.HangRate, "rate_limit_rate": p.RateLimitRate, "error_rate": p.ErrorRate,
		"stall_rate": p.StallRate, "cut_rate": p.CutRate, "malformed_rate": p.MalformedRate,
	} {
		if r < 0 || r > 1 {
			return fmt.Errorf("%s must be within [0,1]", name)
		}
	}
	if p.HangRate+p.RateLimitRate+p.ErrorRate > 1 {
		return errors.New("hang_rate + rate_limit_rate + error_rate must not exceed 1")
	}
	if p.StallRate+p.CutRate+p.MalformedRate > 1 {
		return errors.New("stall_rate + cut_rate + malformed_rate must not exceed 1")
	}
	if p.ErrorStatus != 0 && (p.ErrorStatus < 400 || p.ErrorStatus > 599) {
		return errors.New("error_status must be a 4xx or 5xx code")
	}
	if p.TTFT < 0 || p.TTFTJitter < 0 || p.TokensPerSecond < 0 || p.OutputTokens < 0 || p.MaxConcurrent < 0 {
		return errors.New("durations, rates, and counts must not be negative")
	}
	return nil
}

// Stats are cumulative counters, exposed at GET /_mock/stats.
type Stats struct {
	Requests  uint64 `json:"requests"`
	Active    int64  `json:"active"`
	Completed uint64 `json:"completed"`
	Cancelled uint64 `json:"cancelled"`
	Hangs     uint64 `json:"hangs"`
	RateLimit uint64 `json:"rate_limited"`
	Errors    uint64 `json:"errors"`
	Overload  uint64 `json:"overloaded"`
	Stalls    uint64 `json:"stalls"`
	Cuts      uint64 `json:"cuts"`
	Malformed uint64 `json:"malformed"`
}

// Server is the mock provider. It is safe for concurrent use.
type Server struct {
	profile atomic.Pointer[Profile]
	seq     atomic.Uint64
	mux     *http.ServeMux

	requests, completed, cancelled     atomic.Uint64
	hangs, rateLimited, errs, overload atomic.Uint64
	stalls, cuts, malformed            atomic.Uint64
	active                             atomic.Int64

	onCancel atomic.Pointer[func()]
}

// SetOnCancel registers f to run whenever the mock observes that a client
// abandoned a request. Tests use it to measure how quickly cancellation
// propagates through the gateway.
func (s *Server) SetOnCancel(f func()) { s.onCancel.Store(&f) }

// New returns a Server with the given profile.
func New(p Profile) (*Server, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	s := &Server{mux: http.NewServeMux()}
	s.profile.Store(&p)
	s.mux.HandleFunc("POST /v1/chat/completions", s.chat)
	s.mux.HandleFunc("GET /v1/models", s.models)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	s.mux.HandleFunc("GET /_mock/profile", s.getProfile)
	s.mux.HandleFunc("PUT /_mock/profile", s.putProfile)
	s.mux.HandleFunc("GET /_mock/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.Stats())
	})
	return s, nil
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// SetProfile atomically replaces the profile for subsequent requests.
func (s *Server) SetProfile(p Profile) error {
	if err := p.Validate(); err != nil {
		return err
	}
	s.profile.Store(&p)
	return nil
}

// Profile returns the current profile.
func (s *Server) Profile() Profile { return *s.profile.Load() }

// Stats returns a snapshot of the counters.
func (s *Server) Stats() Stats {
	return Stats{
		Requests: s.requests.Load(), Active: s.active.Load(), Completed: s.completed.Load(),
		Cancelled: s.cancelled.Load(), Hangs: s.hangs.Load(), RateLimit: s.rateLimited.Load(),
		Errors: s.errs.Load(), Overload: s.overload.Load(), Stalls: s.stalls.Load(),
		Cuts: s.cuts.Load(), Malformed: s.malformed.Load(),
	}
}

func (s *Server) getProfile(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Profile())
}

func (s *Server) putProfile(w http.ResponseWriter, r *http.Request) {
	p := s.Profile()
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if err := s.SetProfile(p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) models(w http.ResponseWriter, _ *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": []model{
		{"mock-small", "model", 0, "mockllm"}, {"mock-large", "model", 0, "mockllm"},
	}})
}

type chatRequest struct {
	Model         string `json:"model"`
	Stream        bool   `json:"stream"`
	MaxTokens     *int   `json:"max_tokens"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	Messages []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

type fault int

const (
	faultNone fault = iota
	faultStall
	faultCut
	faultMalformed
)

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	p := s.Profile()
	ctx := r.Context()

	if p.MaxConcurrent > 0 && active > int64(p.MaxConcurrent) {
		s.overload.Add(1)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "overloaded_error", "mock provider is at capacity")
		return
	}

	var req chatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil || req.Model == "" || len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "model and messages are required")
		return
	}

	seq := s.seq.Add(1)
	rng := rand.New(rand.NewPCG(p.Seed, seq))

	// Pre-response faults.
	roll := rng.Float64()
	switch {
	case roll < p.HangRate:
		s.hangs.Add(1)
		<-ctx.Done()
		s.observeCancel()
		return
	case roll < p.HangRate+p.RateLimitRate:
		s.rateLimited.Add(1)
		w.Header().Set("Retry-After", strconv.FormatFloat(p.RetryAfter.D().Seconds(), 'f', -1, 64))
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "mock rate limit exceeded")
		return
	case roll < p.HangRate+p.RateLimitRate+p.ErrorRate:
		s.errs.Add(1)
		status := p.ErrorStatus
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, "server_error", "mock injected failure")
		return
	}

	tokens := p.OutputTokens
	finish := "stop"
	if req.MaxTokens != nil && *req.MaxTokens > 0 && *req.MaxTokens < tokens {
		tokens, finish = *req.MaxTokens, "length"
	}
	mid := faultNone
	roll = rng.Float64()
	switch {
	case roll < p.StallRate:
		mid = faultStall
	case roll < p.StallRate+p.CutRate:
		mid = faultCut
	case roll < p.StallRate+p.CutRate+p.MalformedRate:
		mid = faultMalformed
	}
	// fault_after_tokens: 0 means "halfway"; a negative value injects the
	// fault before the first chunk (before a gateway could commit).
	faultAt := p.FaultAfterTokens
	if faultAt == 0 || faultAt > tokens {
		faultAt = tokens / 2
	}

	g := &generator{
		s: s, rng: rng, p: p, seq: seq, tokens: tokens, finish: finish,
		promptTokens: promptTokens(req.Messages),
		model:        mustJSON(req.Model),
		created:      time.Now().Unix(),
		timer:        time.NewTimer(time.Hour),
	}
	g.timer.Stop()
	defer g.timer.Stop()

	if req.Stream {
		includeUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage
		g.stream(ctx, w, mid, faultAt, includeUsage)
		return
	}
	g.complete(ctx, w, mid)
}

func (s *Server) observeCancel() {
	s.cancelled.Add(1)
	if f := s.onCancel.Load(); f != nil && *f != nil {
		(*f)()
	}
}

type generator struct {
	s            *Server
	rng          *rand.Rand
	p            Profile
	seq          uint64
	tokens       int
	finish       string
	promptTokens int
	model        []byte
	created      int64
	timer        *time.Timer
}

// wait sleeps for d unless ctx ends first.
func (g *generator) wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	g.timer.Reset(d)
	select {
	case <-g.timer.C:
		return true
	case <-ctx.Done():
		if !g.timer.Stop() {
			select {
			case <-g.timer.C:
			default:
			}
		}
		return false
	}
}

func (g *generator) ttft() time.Duration {
	d := g.p.TTFT.D()
	if j := g.p.TTFTJitter.D(); j > 0 {
		d += time.Duration(g.rng.Int64N(int64(j) + 1))
	}
	return d
}

func (g *generator) interToken() time.Duration {
	if g.p.TokensPerSecond <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / g.p.TokensPerSecond)
}

func (g *generator) word(i int) string {
	return words[(int(g.seq)+i)%len(words)]
}

func (g *generator) complete(ctx context.Context, w http.ResponseWriter, mid fault) {
	total := g.ttft() + time.Duration(g.tokens)*g.interToken()
	if mid == faultStall {
		g.s.stalls.Add(1)
		<-ctx.Done()
		g.s.observeCancel()
		return
	}
	if !g.wait(ctx, total) {
		g.s.observeCancel()
		return
	}
	if mid == faultCut {
		g.s.cuts.Add(1)
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Type", "application/json")
	if mid == faultMalformed {
		g.s.malformed.Add(1)
		_, _ = io.WriteString(w, `{"id":"chatcmpl-mock","choices":[{"message":`)
		return
	}
	text := make([]byte, 0, g.tokens*8)
	for i := 0; i < g.tokens; i++ {
		if i > 0 {
			text = append(text, ' ')
		}
		text = append(text, g.word(i)...)
	}
	resp := map[string]any{
		"id": fmt.Sprintf("chatcmpl-mock-%d", g.seq), "object": "chat.completion", "created": g.created,
		"model": json.RawMessage(g.model),
		"choices": []any{map[string]any{
			"index": 0, "finish_reason": g.finish,
			"message": map[string]any{"role": "assistant", "content": string(text)},
		}},
		"usage": map[string]int{
			"prompt_tokens": g.promptTokens, "completion_tokens": g.tokens,
			"total_tokens": g.promptTokens + g.tokens,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
	g.s.completed.Add(1)
}

func (g *generator) stream(ctx context.Context, w http.ResponseWriter, mid fault, faultAt int, includeUsage bool) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()

	buf := make([]byte, 0, 512)
	send := func(b []byte) bool {
		if _, err := w.Write(b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}

	if !g.wait(ctx, g.ttft()) {
		g.s.observeCancel()
		return
	}
	if faultAt < 0 && mid != faultNone && !g.fault(ctx, mid, send) {
		return
	}
	if !send(g.chunk(buf[:0], `{"role":"assistant","content":""}`, "null")) {
		g.s.observeCancel()
		return
	}
	gap := g.interToken()
	for i := 0; i < g.tokens; i++ {
		if i == faultAt && mid != faultNone && !g.fault(ctx, mid, send) {
			return
		}
		if i > 0 && !g.wait(ctx, gap) {
			g.s.observeCancel()
			return
		}
		if !send(g.contentChunk(buf[:0], g.word(i))) {
			g.s.observeCancel()
			return
		}
	}
	if !send(g.chunk(buf[:0], `{}`, strconv.Quote(g.finish))) {
		g.s.observeCancel()
		return
	}
	if includeUsage {
		u := fmt.Sprintf(`data: {"id":"chatcmpl-mock-%d","object":"chat.completion.chunk","created":%d,"model":%s,"choices":[],"usage":{"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d}}`+"\n\n",
			g.seq, g.created, g.model, g.promptTokens, g.tokens, g.promptTokens+g.tokens)
		if !send([]byte(u)) {
			g.s.observeCancel()
			return
		}
	}
	if send([]byte("data: [DONE]\n\n")) {
		g.s.completed.Add(1)
	} else {
		g.s.observeCancel()
	}
}

// fault injects a mid-stream fault. It returns false when the stream must
// end (always, for the faults implemented here).
func (g *generator) fault(ctx context.Context, mid fault, send func([]byte) bool) bool {
	switch mid {
	case faultStall:
		g.s.stalls.Add(1)
		<-ctx.Done()
		g.s.observeCancel()
	case faultCut:
		g.s.cuts.Add(1)
		panic(http.ErrAbortHandler)
	case faultMalformed:
		g.s.malformed.Add(1)
		send([]byte("data: {\"id\":\"chatcmpl-mock\",\"choices\":[{\"delta\":{\"content\":\n\n"))
	default:
		return true
	}
	return false
}

// chunk renders one SSE chat.completion.chunk event into buf.
func (g *generator) chunk(buf []byte, delta, finish string) []byte {
	buf = g.chunkPrefix(buf)
	buf = append(buf, delta...)
	buf = append(buf, `,"finish_reason":`...)
	buf = append(buf, finish...)
	buf = append(buf, "}]}\n\n"...)
	return buf
}

// contentChunk renders a content delta without intermediate allocations.
func (g *generator) contentChunk(buf []byte, word string) []byte {
	buf = g.chunkPrefix(buf)
	buf = append(buf, `{"content":" `...)
	buf = append(buf, word...)
	buf = append(buf, `"},"finish_reason":null}]}`+"\n\n"...)
	return buf
}

func (g *generator) chunkPrefix(buf []byte) []byte {
	buf = append(buf, `data: {"id":"chatcmpl-mock-`...)
	buf = strconv.AppendUint(buf, g.seq, 10)
	buf = append(buf, `","object":"chat.completion.chunk","created":`...)
	buf = strconv.AppendInt(buf, g.created, 10)
	buf = append(buf, `,"model":`...)
	buf = append(buf, g.model...)
	buf = append(buf, `,"choices":[{"index":0,"delta":`...)
	return buf
}

func promptTokens(msgs []struct {
	Content json.RawMessage `json:"content"`
}) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
	}
	return max(1, (n+3)/4)
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": msg, "type": typ, "param": nil, "code": nil}})
}

// words is the deterministic vocabulary for generated text.
var words = []string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
	"sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et",
	"dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam", "quis",
	"nostrud", "exercitation", "ullamco", "laboris", "nisi", "aliquip", "ex", "ea",
}
