// Command mockllm runs a fault-injecting, OpenAI-compatible mock LLM
// provider for local development, tests, demos, and benchmarks.
//
//	mockllm -listen 127.0.0.1:9001 -ttft 200ms -tps 50 -tokens 64 -error-rate 0.1
//
// The profile can be changed while running:
//
//	curl -X PUT localhost:9001/_mock/profile -d '{"error_rate":0.5}'
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/mock"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mockllm:", err)
		os.Exit(1)
	}
}

func run() error {
	d := mock.DefaultProfile()
	listen := flag.String("listen", "127.0.0.1:9001", "listen address")
	profileFile := flag.String("profile", "", "JSON profile file; when set, the other profile flags are ignored")
	ttft := flag.Duration("ttft", d.TTFT.D(), "time to first token")
	jitter := flag.Duration("ttft-jitter", 0, "uniform random extra TTFT in [0, jitter]")
	tps := flag.Float64("tps", 0, "tokens per second (0 = as fast as possible)")
	tokens := flag.Int("tokens", d.OutputTokens, "output tokens per response")
	errRate := flag.Float64("error-rate", 0, "probability of an HTTP error response")
	errStatus := flag.Int("error-status", d.ErrorStatus, "status code for injected errors")
	rlRate := flag.Float64("rate-limit-rate", 0, "probability of HTTP 429")
	retryAfter := flag.Duration("retry-after", d.RetryAfter.D(), "Retry-After for injected 429s")
	hang := flag.Float64("hang-rate", 0, "probability of never responding")
	stall := flag.Float64("stall-rate", 0, "probability of stalling mid-stream")
	cut := flag.Float64("cut-rate", 0, "probability of aborting the connection mid-stream")
	malformed := flag.Float64("malformed-rate", 0, "probability of an invalid JSON chunk mid-stream")
	faultAfter := flag.Int("fault-after", 0, "token index where mid-stream faults trigger (default tokens/2)")
	maxConc := flag.Int("max-concurrent", 0, "answer 429 beyond this many in-flight requests (0 = unlimited)")
	seed := flag.Uint64("seed", 1, "random seed for reproducible faults")
	flag.Parse()

	p := mock.Profile{
		TTFT: config.Duration(*ttft), TTFTJitter: config.Duration(*jitter), TokensPerSecond: *tps,
		OutputTokens: *tokens, ErrorRate: *errRate, ErrorStatus: *errStatus, RateLimitRate: *rlRate,
		RetryAfter: config.Duration(*retryAfter), HangRate: *hang, StallRate: *stall, CutRate: *cut,
		MalformedRate: *malformed, FaultAfterTokens: *faultAfter, MaxConcurrent: *maxConc, Seed: *seed,
	}
	if *profileFile != "" {
		raw, err := os.ReadFile(*profileFile)
		if err != nil {
			return err
		}
		p = mock.DefaultProfile()
		if err := json.Unmarshal(raw, &p); err != nil {
			return fmt.Errorf("profile %s: %w", *profileFile, err)
		}
	}
	srv, err := mock.New(p)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	hs := &http.Server{Addr: *listen, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("mockllm listening", "addr", *listen, "profile", p)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hs.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	log.Info("mockllm stopped", "stats", srv.Stats())
	return nil
}
