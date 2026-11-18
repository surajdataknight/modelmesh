package mock

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/sse"
)

func newMock(t *testing.T, mutate func(*Profile)) (*Server, *httptest.Server) {
	t.Helper()
	p := DefaultProfile()
	p.TTFT = 0
	if mutate != nil {
		mutate(&p)
	}
	s, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return s, ts
}

func post(t *testing.T, ctx context.Context, url, body string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return http.DefaultClient.Do(req)
}

const streamBody = `{"model":"mock-small","messages":[{"role":"user","content":"hello there"}],"stream":true,"stream_options":{"include_usage":true}}`

func TestStreamIsValidOpenAIDialect(t *testing.T) {
	_, ts := newMock(t, func(p *Profile) { p.OutputTokens = 5 })
	resp, err := post(t, context.Background(), ts.URL, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	rd := sse.NewReader(resp.Body, 1<<20)
	var content, events int
	var usage *api.Usage
	for {
		ev, err := rd.Next()
		if err != nil {
			t.Fatalf("stream ended without [DONE]: %v", err)
		}
		if string(ev) == "[DONE]" {
			break
		}
		events++
		info := api.InspectChunk(ev)
		if !info.Valid {
			t.Fatalf("invalid chunk: %s", ev)
		}
		if info.HasContent {
			content++
		}
		if info.Usage != nil {
			usage = info.Usage
		}
	}
	if content != 5 || usage == nil || usage.CompletionTokens != 5 || events != 8 {
		t.Fatalf("content=%d events=%d usage=%+v", content, events, usage)
	}
}

func TestNonStreamAndMaxTokens(t *testing.T) {
	_, ts := newMock(t, nil)
	resp, err := post(t, context.Background(), ts.URL, `{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":3}`)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Choices []struct {
			FinishReason string                   `json:"finish_reason"`
			Message      struct{ Content string } `json:"message"`
		} `json:"choices"`
		Usage api.Usage `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Usage.CompletionTokens != 3 || out.Choices[0].FinishReason != "length" || len(strings.Fields(out.Choices[0].Message.Content)) != 3 {
		t.Fatalf("response = %+v", out)
	}
}

func TestPreResponseFaults(t *testing.T) {
	s, ts := newMock(t, func(p *Profile) { p.RateLimitRate = 1; p.RetryAfter = config.Duration(2 * time.Second) })
	resp, err := post(t, context.Background(), ts.URL, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "2" {
		t.Fatalf("status=%d retry-after=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}

	p := s.Profile()
	p.RateLimitRate, p.ErrorRate, p.ErrorStatus = 0, 1, 502
	if err := s.SetProfile(p); err != nil {
		t.Fatal(err)
	}
	resp, err = post(t, context.Background(), ts.URL, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	p.ErrorRate, p.HangRate = 0, 1
	_ = s.SetProfile(p)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = post(t, ctx, ts.URL, streamBody)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hang: err = %v", err)
	}
	waitFor(t, func() bool { return s.Stats().Cancelled == 1 })
}

func TestMidStreamFaults(t *testing.T) {
	cases := []struct {
		name   string
		set    func(*Profile)
		expect func(t *testing.T, events []string, err error)
	}{
		{"cut", func(p *Profile) { p.CutRate = 1 }, func(t *testing.T, ev []string, err error) {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("cut: err = %v", err)
			}
		}},
		{"malformed", func(p *Profile) { p.MalformedRate = 1 }, func(t *testing.T, ev []string, err error) {
			if len(ev) == 0 || api.InspectChunk([]byte(ev[len(ev)-1])).Valid {
				t.Fatalf("malformed: last event should be invalid JSON: %q", ev)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ts := newMock(t, func(p *Profile) { p.OutputTokens = 6; p.FaultAfterTokens = 3; c.set(p) })
			resp, err := post(t, context.Background(), ts.URL, streamBody)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			rd := sse.NewReader(resp.Body, 1<<20)
			var events []string
			for {
				ev, err := rd.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					c.expect(t, events, err)
					return
				}
				events = append(events, string(ev))
			}
		})
	}
}

func TestStallKeepsConnectionOpen(t *testing.T) {
	s, ts := newMock(t, func(p *Profile) { p.StallRate = 1; p.OutputTokens = 4; p.FaultAfterTokens = 2 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := post(t, ctx, ts.URL, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := sse.NewReader(resp.Body, 1<<20)
	for i := 0; i < 3; i++ { // role chunk + 2 content chunks
		if _, err := rd.Next(); err != nil {
			t.Fatal(err)
		}
	}
	got := make(chan error, 1)
	go func() { _, err := rd.Next(); got <- err }()
	select {
	case err := <-got:
		t.Fatalf("stalled stream returned early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-got
	waitFor(t, func() bool { return s.Stats().Cancelled == 1 && s.Stats().Stalls == 1 })
}

func TestMaxConcurrentOverload(t *testing.T) {
	s, ts := newMock(t, func(p *Profile) { p.MaxConcurrent = 1; p.HangRate = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = post(t, ctx, ts.URL, streamBody) }()
	waitFor(t, func() bool { return s.Stats().Active == 1 })
	resp, err := post(t, context.Background(), ts.URL, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || s.Stats().Overload != 1 {
		t.Fatalf("status=%d stats=%+v", resp.StatusCode, s.Stats())
	}
}

func TestProfileEndpoint(t *testing.T) {
	_, ts := newMock(t, nil)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/_mock/profile", strings.NewReader(`{"error_rate":0.5,"ttft":"10ms"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/_mock/profile", strings.NewReader(`{"error_rate":2}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("invalid profile status = %d", resp.StatusCode)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}
