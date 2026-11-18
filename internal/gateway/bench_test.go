package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/metrics"
	"github.com/modelmesh/modelmesh/internal/mock"
)

// These benchmarks compare one full request sent directly to the mock with
// the same request sent through the gateway, in-process over loopback HTTP.
// Client and mock costs are identical in both, so the difference in time and
// allocations per request is the gateway's overhead.

type benchEnv struct {
	mockURL, gwURL string
	client         *http.Client
	close          func()
}

func newBenchEnv(b *testing.B, tokens int) *benchEnv {
	b.Helper()
	p := mock.DefaultProfile()
	p.TTFT, p.OutputTokens = 0, tokens
	ms, err := mock.New(p)
	if err != nil {
		b.Fatal(err)
	}
	mts := httptest.NewServer(ms)
	cfg := config.Defaults()
	cfg.Auth.Keys = []config.APIKey{{Name: "bench", Value: testKey}}
	cfg.Providers = []config.Provider{{Name: "mock", BaseURL: mts.URL + "/v1", MaxConcurrent: 1024}}
	cfg.Models = []config.Model{{Name: "m", Targets: []config.Target{{Provider: "mock", Model: "mock-small"}}}}
	// Realistic logging cost: JSON access log lines, written to io.Discard.
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	gw, err := New(&cfg, log, metrics.NewRegistry())
	if err != nil {
		b.Fatal(err)
	}
	gts := httptest.NewServer(gw.Handler())
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, DisableCompression: true}}
	return &benchEnv{mockURL: mts.URL, gwURL: gts.URL, client: client, close: func() {
		client.CloseIdleConnections()
		gts.Close()
		mts.Close()
	}}
}

func benchRequest(b *testing.B, client *http.Client, url, body string) {
	req, _ := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	resp, err := client.Do(req)
	if err != nil {
		b.Fatal(err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || n == 0 {
		b.Fatalf("status %d, %d bytes", resp.StatusCode, n)
	}
}

func runBench(b *testing.B, via string, stream bool, parallel bool) {
	env := newBenchEnv(b, 64)
	defer env.close()
	url, model := env.gwURL, "m"
	if via == "direct" {
		url, model = env.mockURL, "mock-small"
	}
	body := `{"model":"` + model + `","messages":[{"role":"user","content":"benchmark"}]`
	if stream {
		// The gateway always asks upstream for usage; ask the mock directly
		// for the same so both paths do identical upstream work.
		body += `,"stream":true,"stream_options":{"include_usage":true}`
	}
	body += "}"
	benchRequest(b, env.client, url, body) // warm connections
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchRequest(b, env.client, url, body)
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		benchRequest(b, env.client, url, body)
	}
}

func BenchmarkStream64Direct(b *testing.B)             { runBench(b, "direct", true, false) }
func BenchmarkStream64ViaGateway(b *testing.B)         { runBench(b, "gateway", true, false) }
func BenchmarkNonStreamDirect(b *testing.B)            { runBench(b, "direct", false, false) }
func BenchmarkNonStreamViaGateway(b *testing.B)        { runBench(b, "gateway", false, false) }
func BenchmarkStream64DirectParallel(b *testing.B)     { runBench(b, "direct", true, true) }
func BenchmarkStream64ViaGatewayParallel(b *testing.B) { runBench(b, "gateway", true, true) }
