package gateway

// Lifecycle tests: slow consumers, server timeouts versus long streams,
// graceful shutdown, and goroutine leaks.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/mock"
)

func TestSlowConsumerIsDisconnected(t *testing.T) {
	// The provider produces tokens as fast as it can; the client never reads.
	// Once socket buffers fill, the per-write deadline (300ms) must fire, the
	// stream must end, and the upstream request must be cancelled — instead
	// of buffering without bound or pinning the stream forever.
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.OutputTokens = 50_000_000 }}}, nil)
	conn, err := net.Dial("tcp", h.ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := chatBody("a-only", true, "")
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"+
		"Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", testKey, len(body), body)

	eventually(t, "slow client cut off", func() bool { return h.requests("a-only", outSlowClient) == 1 })
	eventually(t, "upstream cancelled", func() bool {
		s := h.mocks["a"].Stats()
		return s.Cancelled == 1 && s.Active == 0
	})
}

func TestSlowRequestBodyGets408(t *testing.T) {
	h := newHarness(t, []mockSpec{healthy("a")}, func(c *config.Config) {
		c.Timeouts.BodyRead = config.Duration(150 * time.Millisecond)
	})
	conn, err := net.Dial("tcp", h.ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"+
		"Content-Type: application/json\r\nContent-Length: 500\r\n\r\n{\"model\":", testKey)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if e := decodeError(t, resp); resp.StatusCode != 408 || e.Code != "body_read_timeout" {
		t.Fatalf("status=%d err=%+v", resp.StatusCode, e)
	}
}

// startServer runs a real gateway Server (not httptest) so that server
// timeouts and shutdown behave exactly as in production.
func startServer(t *testing.T, h *harness) (*Server, string, chan error) {
	t.Helper()
	h.gw.cfg.Listen = "127.0.0.1:0"
	h.gw.cfg.AdminListen = ""
	srv := NewServer(h.gw)
	pub, adm, err := srv.Listen()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(pub, adm) }()
	return srv, "http://" + pub.Addr().String(), done
}

func postTo(t *testing.T, client *http.Client, base, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestLongStreamOutlivesServerTimeouts(t *testing.T) {
	// read_header, body_read, and idle_conn are all far shorter than the
	// stream; none of them may end it.
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 10; p.OutputTokens = 8 }}},
		func(c *config.Config) {
			c.Timeouts.ReadHeader = config.Duration(100 * time.Millisecond)
			c.Timeouts.BodyRead = config.Duration(100 * time.Millisecond)
			c.Timeouts.IdleConn = config.Duration(100 * time.Millisecond)
		})
	srv, base, _ := startServer(t, h)
	defer srv.Shutdown(context.Background())
	start := time.Now()
	r := readStream(postTo(t, h.client, base, chatBody("a-only", true, "")))
	if !r.done || r.content != 8 {
		t.Fatalf("stream = %+v", r)
	}
	if time.Since(start) < 700*time.Millisecond {
		t.Fatal("stream finished too quickly for this test to be meaningful")
	}
}

func TestGracefulShutdownDrainsInFlightStreams(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 20; p.OutputTokens = 10 }}}, nil)
	srv, base, served := startServer(t, h)

	resp := postTo(t, h.client, base, chatBody("a-only", true, ""))
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil { // first event arrived: stream is committed
		t.Fatal(err)
	}
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- srv.Shutdown(context.Background()) }()
	eventually(t, "readiness turned off", func() bool { return !h.gw.ready.Load() })

	rest, _ := io.ReadAll(br)
	resp.Body.Close()
	if !strings.Contains(string(rest), "data: [DONE]") {
		t.Fatalf("in-flight stream was not allowed to finish:\n%s", rest)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(base, "http://"), 200*time.Millisecond); err == nil {
		t.Fatal("listener still accepting after shutdown")
	}
}

func TestShutdownGraceExpiryEndsStreamsWithErrorEvent(t *testing.T) {
	h := newHarness(t, []mockSpec{{"a", func(p *mock.Profile) { p.TokensPerSecond = 10; p.OutputTokens = 10_000 }}},
		func(c *config.Config) { c.Timeouts.ShutdownGrace = config.Duration(200 * time.Millisecond) })
	srv, base, _ := startServer(t, h)

	resp := postTo(t, h.client, base, chatBody("a-only", true, ""))
	results := make(chan streamResult, 1)
	go func() { results <- readStream(resp) }()
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("shutdown took %v", el)
	}
	r := <-results
	if r.done || r.errEv == nil || r.errEv.Code != "gateway_shutdown" {
		t.Fatalf("stream = %+v errEv=%+v", r, r.errEv)
	}
	eventually(t, "upstream cancelled", func() bool { return h.mocks["a"].Stats().Active == 0 })
}

func TestNoGoroutineLeaksAcrossFailureModes(t *testing.T) {
	h := newHarness(t, []mockSpec{
		{"ok", func(p *mock.Profile) { p.TokensPerSecond = 200; p.OutputTokens = 20 }},
		{"stall", func(p *mock.Profile) { p.StallRate = 1; p.FaultAfterTokens = 2 }},
		{"slow", func(p *mock.Profile) { p.TokensPerSecond = 50; p.OutputTokens = 10_000 }},
		{"hang", func(p *mock.Profile) { p.HangRate = 1 }},
	}, func(c *config.Config) { c.Retry.MaxAttempts = 1 })

	settle := func() int {
		h.client.CloseIdleConnections()
		h.gw.CloseIdleConnections()
		var n int
		for i := 0; i < 100; i++ {
			runtime.GC()
			n = runtime.NumGoroutine()
			time.Sleep(10 * time.Millisecond)
			if m := runtime.NumGoroutine(); m == n {
				break
			}
		}
		return n
	}
	readStream(h.post(context.Background(), chatBody("ok-only", true, ""))) // warm up
	baseline := settle()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		for _, model := range []string{"ok-only", "stall-only", "hang-only"} {
			wg.Add(1)
			go func(model string) {
				defer wg.Done()
				resp, err := h.postErr(context.Background(), chatBody(model, true, ""))
				if err == nil {
					readStream(resp)
				}
			}(model)
		}
		wg.Add(1)
		go func() { // client walks away mid-stream
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			resp, err := h.postErr(ctx, chatBody("slow-only", true, ""))
			if err == nil {
				_, _ = resp.Body.Read(make([]byte, 256))
				cancel()
				resp.Body.Close()
			}
			cancel()
		}()
	}
	wg.Wait()
	eventually(t, "all requests finished", func() bool { return h.gw.m.inflight.Value() == 0 })
	for name, m := range h.mocks {
		eventually(t, name+" upstream requests finished", func() bool { return m.Stats().Active == 0 })
	}

	after := settle()
	if after > baseline+5 {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutines: baseline %d, after %d\n%s", baseline, after, buf[:n])
	}
	t.Logf("goroutines: baseline %d, after 80 mixed requests %d", baseline, after)
}
