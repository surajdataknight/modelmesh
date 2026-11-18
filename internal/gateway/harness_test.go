package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/metrics"
	"github.com/modelmesh/modelmesh/internal/mock"
	"github.com/modelmesh/modelmesh/internal/sse"
)

const testKey = "sk-test-local-key"

type mockSpec struct {
	name string
	set  func(*mock.Profile)
}

type harness struct {
	t      *testing.T
	cfg    *config.Config
	gw     *Gateway
	ts     *httptest.Server
	mocks  map[string]*mock.Server
	conns  map[string]*atomic.Int64 // TCP connections accepted per mock
	client *http.Client
}

// testConfig uses short timeouts so failure paths finish quickly.
func testConfig() *config.Config {
	cfg := config.Defaults()
	cfg.Auth.Keys = []config.APIKey{{Name: "test", Value: testKey}}
	t := &cfg.Timeouts
	t.FirstToken = config.Duration(300 * time.Millisecond)
	t.Idle = config.Duration(300 * time.Millisecond)
	t.Attempt = config.Duration(2 * time.Second)
	t.ClientWrite = config.Duration(300 * time.Millisecond)
	t.Request = config.Duration(10 * time.Second)
	t.ShutdownGrace = config.Duration(2 * time.Second)
	cfg.Retry.BaseBackoff = config.Duration(5 * time.Millisecond)
	cfg.Retry.MaxBackoff = config.Duration(20 * time.Millisecond)
	return &cfg
}

// newHarness starts one mock provider per spec and a gateway with:
//   - model "m" targeting every mock in spec order;
//   - model "<name>-only" targeting a single mock.
func newHarness(t *testing.T, specs []mockSpec, mutate func(*config.Config)) *harness {
	t.Helper()
	cfg := testConfig()
	h := &harness{t: t, cfg: cfg, mocks: map[string]*mock.Server{}, conns: map[string]*atomic.Int64{}}
	all := config.Model{Name: "m"}
	for _, s := range specs {
		p := mock.DefaultProfile()
		p.TTFT = 0
		p.OutputTokens = 8
		if s.set != nil {
			s.set(&p)
		}
		ms, err := mock.New(p)
		if err != nil {
			t.Fatal(err)
		}
		mts := httptest.NewUnstartedServer(ms)
		counter := new(atomic.Int64)
		mts.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				counter.Add(1)
			}
		}
		mts.Start()
		t.Cleanup(mts.Close)
		h.mocks[s.name] = ms
		h.conns[s.name] = counter
		cfg.Providers = append(cfg.Providers, config.Provider{Name: s.name, BaseURL: mts.URL + "/v1", MaxConcurrent: 256})
		all.Targets = append(all.Targets, config.Target{Provider: s.name, Model: "mock-small"})
		cfg.Models = append(cfg.Models, config.Model{Name: s.name + "-only",
			Targets: []config.Target{{Provider: s.name, Model: "mock-small"}}})
	}
	cfg.Models = append(cfg.Models, all)
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	gw, err := New(cfg, nil, metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h.gw = gw
	h.ts = httptest.NewServer(gw.Handler())
	h.client = &http.Client{Transport: &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 64}}
	t.Cleanup(func() {
		h.client.CloseIdleConnections()
		h.ts.Close()
		gw.CloseIdleConnections()
	})
	return h
}

func chatBody(model string, stream bool, extra string) string {
	b := `{"model":"` + model + `","messages":[{"role":"user","content":"hello"}]`
	if stream {
		b += `,"stream":true`
	}
	if extra != "" {
		b += "," + extra
	}
	return b + "}"
}

func (h *harness) post(ctx context.Context, body string, headers ...string) *http.Response {
	h.t.Helper()
	resp, err := h.postErr(ctx, body, headers...)
	if err != nil {
		h.t.Fatalf("POST: %v", err)
	}
	return resp
}

func (h *harness) postErr(ctx context.Context, body string, headers ...string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.ts.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+testKey)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return h.client.Do(req)
}

type streamResult struct {
	events  []string // data payloads, excluding [DONE]
	content int      // events carrying content
	done    bool     // saw [DONE]
	usage   *api.Usage
	errEv   *api.ErrorDetail // in-band error event, if any
	err     error            // read error other than clean EOF
}

func readStream(resp *http.Response) streamResult {
	defer resp.Body.Close()
	var r streamResult
	rd := sse.NewReader(resp.Body, 1<<20)
	for {
		ev, err := rd.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				r.err = err
			}
			return r
		}
		if string(ev) == "[DONE]" {
			r.done = true
			continue
		}
		r.events = append(r.events, string(ev))
		info := api.InspectChunk(ev)
		if info.HasContent {
			r.content++
		}
		if info.Usage != nil {
			r.usage = info.Usage
		}
		if info.ErrorBody {
			var body api.ErrorBody
			_ = json.Unmarshal(ev, &body)
			r.errEv = &body.Error
		}
	}
}

func decodeError(t *testing.T, resp *http.Response) api.ErrorDetail {
	t.Helper()
	defer resp.Body.Close()
	var body api.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return body.Error
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func (h *harness) requests(model, outcome string) float64 {
	return h.gw.m.requests.With(model, outcome).Value()
}

func (h *harness) mockRequests(name string) uint64 { return h.mocks[name].Stats().Requests }
