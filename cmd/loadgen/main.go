// Command loadgen is an open-loop load generator for OpenAI-compatible chat
// endpoints. It measures what matters for LLM traffic — time to first token
// and inter-chunk gaps, not only total latency.
//
// Open loop: requests are scheduled at a fixed rate regardless of how fast
// earlier requests complete, and every latency is measured from the request's
// scheduled start time. A closed-loop tool (N workers each waiting for its
// previous response) silently lowers the offered load when the server slows
// down and so under-reports tail latency ("coordinated omission").
//
//	loadgen -url http://127.0.0.1:8080/v1/chat/completions -model fast -rate 200 -duration 30s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelmesh/modelmesh/internal/api"
	"github.com/modelmesh/modelmesh/internal/sse"
)

type options struct {
	url, key, model, label string
	rate                   float64
	duration, timeout      time.Duration
	cancelAfter            time.Duration
	maxInflight, maxTokens int
	stream, jsonOut        bool
}

type result struct {
	status    int
	ttfb      time.Duration // scheduled start -> response headers
	ttft      time.Duration // scheduled start -> first SSE event (or full body)
	total     time.Duration
	maxGap    time.Duration // largest gap between consecutive events
	chunks    int
	errKind   string
	cancelled bool // ended by -cancel-after, as intended
}

func main() {
	var o options
	flag.StringVar(&o.url, "url", "http://127.0.0.1:8080/v1/chat/completions", "chat completions URL")
	flag.StringVar(&o.key, "key", os.Getenv("MODELMESH_API_KEY"), "bearer token (default $MODELMESH_API_KEY)")
	flag.StringVar(&o.model, "model", "fast", "model name")
	flag.StringVar(&o.label, "label", "", "label for this run in the report")
	flag.Float64Var(&o.rate, "rate", 50, "requests per second (open loop)")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "how long to schedule requests")
	flag.DurationVar(&o.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.DurationVar(&o.cancelAfter, "cancel-after", 0, "disconnect each request after this long (tests cancellation)")
	flag.IntVar(&o.maxInflight, "max-inflight", 20000, "requests beyond this many in flight are dropped and counted")
	flag.IntVar(&o.maxTokens, "max-tokens", 0, "max_tokens to request (0 = omit)")
	flag.BoolVar(&o.stream, "stream", true, "use streaming")
	flag.BoolVar(&o.jsonOut, "json", false, "print the summary as JSON")
	flag.Parse()
	if o.rate <= 0 || o.duration <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: -rate and -duration must be positive")
		os.Exit(2)
	}

	sum := run(o)
	if o.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(sum)
		return
	}
	sum.print(os.Stdout)
}

func run(o options) summary {
	tr := &http.Transport{
		Proxy:               nil,
		MaxIdleConnsPerHost: o.maxInflight,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{Transport: tr}
	body := `{"model":` + strconv.Quote(o.model) + `,"messages":[{"role":"user","content":"Say something."}]`
	if o.stream {
		body += `,"stream":true`
	}
	if o.maxTokens > 0 {
		body += `,"max_tokens":` + strconv.Itoa(o.maxTokens)
	}
	body += "}"

	total := int(o.rate * o.duration.Seconds())
	interval := time.Duration(float64(time.Second) / o.rate)
	results := make([]result, 0, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var inflight atomic.Int64
	dropped := 0

	start := time.Now().Add(20 * time.Millisecond)
	for i := 0; i < total; i++ {
		at := start.Add(time.Duration(i) * interval)
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		if inflight.Load() >= int64(o.maxInflight) {
			dropped++
			continue
		}
		inflight.Add(1)
		wg.Add(1)
		go func(at time.Time) {
			defer wg.Done()
			defer inflight.Add(-1)
			r := do(client, o, body, at)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(at)
	}
	scheduleEnd := time.Now()
	wg.Wait()
	wall := time.Since(start)
	tr.CloseIdleConnections()
	return summarize(o, results, total, dropped, wall, scheduleEnd.Sub(start))
}

func do(client *http.Client, o options, body string, at time.Time) result {
	var r result
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	if o.cancelAfter > 0 {
		var c2 context.CancelFunc
		ctx, c2 = context.WithTimeoutCause(ctx, o.cancelAfter, errIntentional)
		defer c2()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, strings.NewReader(body))
	if err != nil {
		r.errKind = "bad_request"
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	if o.key != "" {
		req.Header.Set("Authorization", "Bearer "+o.key)
	}
	resp, err := client.Do(req)
	if err != nil {
		r.errKind, r.cancelled = classify(ctx, err)
		r.total = time.Since(at)
		return r
	}
	defer resp.Body.Close()
	r.status = resp.StatusCode
	r.ttfb = time.Since(at)
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		r.errKind = "http_" + strconv.Itoa(resp.StatusCode)
		r.total = time.Since(at)
		return r
	}
	if !o.stream {
		_, err := io.Copy(io.Discard, resp.Body)
		r.total = time.Since(at)
		r.ttft = r.total
		if err != nil {
			r.errKind, r.cancelled = classify(ctx, err)
		}
		return r
	}

	rd := sse.NewReader(resp.Body, 4<<20)
	last := time.Time{}
	done := false
	for {
		ev, err := rd.Next()
		now := time.Now()
		if err != nil {
			if !done && r.errKind == "" {
				if errors.Is(err, io.EOF) {
					r.errKind = "stream_ended_without_done"
				} else {
					r.errKind, r.cancelled = classify(ctx, err)
				}
			}
			break
		}
		if r.ttft == 0 {
			r.ttft = now.Sub(at)
		} else if gap := now.Sub(last); gap > r.maxGap {
			r.maxGap = gap
		}
		last = now
		if string(ev) == "[DONE]" {
			done = true
			continue
		}
		r.chunks++
		if info := api.InspectChunk(ev); info.ErrorBody {
			var eb api.ErrorBody
			_ = json.Unmarshal(ev, &eb)
			r.errKind = "stream_error_" + eb.Error.Code
		}
	}
	r.total = time.Since(at)
	return r
}

var errIntentional = errors.New("cancel-after reached")

func classify(ctx context.Context, err error) (kind string, intentional bool) {
	if errors.Is(context.Cause(ctx), errIntentional) {
		return "", true
	}
	if ctx.Err() != nil {
		return "client_timeout", false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "connection_cut", false
	}
	return "transport_error", false
}

type percentiles struct {
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p99_9_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

type summary struct {
	Label           string         `json:"label,omitempty"`
	URL             string         `json:"url"`
	Model           string         `json:"model"`
	Stream          bool           `json:"stream"`
	TargetRate      float64        `json:"target_rps"`
	Scheduled       int            `json:"scheduled"`
	Dropped         int            `json:"dropped"`
	Completed       int            `json:"completed_ok"`
	Cancelled       int            `json:"cancelled_as_intended"`
	Errors          map[string]int `json:"errors"`
	SuccessRate     float64        `json:"success_rate"`
	AchievedRPS     float64        `json:"achieved_ok_rps"`
	ScheduleSkewMS  float64        `json:"schedule_overrun_ms"`
	WallSeconds     float64        `json:"wall_seconds"`
	TTFT            percentiles    `json:"ttft"`
	Total           percentiles    `json:"total"`
	MaxGap          percentiles    `json:"max_inter_chunk_gap"`
	ChunksPerStream float64        `json:"mean_chunks_per_stream"`
}

func summarize(o options, rs []result, scheduled, dropped int, wall, scheduleSpan time.Duration) summary {
	s := summary{Label: o.label, URL: o.url, Model: o.model, Stream: o.stream, TargetRate: o.rate,
		Scheduled: scheduled, Dropped: dropped, Errors: map[string]int{}, WallSeconds: wall.Seconds()}
	s.ScheduleSkewMS = float64(scheduleSpan-o.duration) / float64(time.Millisecond)
	var ttft, total, gaps []time.Duration
	chunks := 0
	for _, r := range rs {
		switch {
		case r.cancelled:
			s.Cancelled++
		case r.errKind != "":
			s.Errors[r.errKind]++
		default:
			s.Completed++
			ttft = append(ttft, r.ttft)
			total = append(total, r.total)
			gaps = append(gaps, r.maxGap)
			chunks += r.chunks
		}
	}
	attempted := scheduled - s.Cancelled
	if attempted > 0 {
		s.SuccessRate = float64(s.Completed) / float64(attempted)
	}
	s.AchievedRPS = float64(s.Completed) / wall.Seconds()
	s.TTFT, s.Total, s.MaxGap = pct(ttft), pct(total), pct(gaps)
	if s.Completed > 0 {
		s.ChunksPerStream = float64(chunks) / float64(s.Completed)
	}
	return s
}

func pct(ds []time.Duration) percentiles {
	if len(ds) == 0 {
		return percentiles{}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	at := func(p float64) float64 {
		i := int(p/100*float64(len(ds))+0.999999) - 1
		i = max(0, min(i, len(ds)-1))
		return msf(ds[i])
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return percentiles{P50: at(50), P90: at(90), P95: at(95), P99: at(99), P999: at(99.9),
		Max: msf(ds[len(ds)-1]), Mean: msf(sum / time.Duration(len(ds)))}
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (s summary) print(w io.Writer) {
	fmt.Fprintf(w, "loadgen %s\n", s.Label)
	fmt.Fprintf(w, "  target      %s model=%s stream=%v rate=%.0f/s\n", s.URL, s.Model, s.Stream, s.TargetRate)
	fmt.Fprintf(w, "  requests    scheduled=%d ok=%d cancelled=%d dropped=%d success=%.2f%% achieved=%.1f ok/s\n",
		s.Scheduled, s.Completed, s.Cancelled, s.Dropped, 100*s.SuccessRate, s.AchievedRPS)
	if len(s.Errors) > 0 {
		keys := make([]string, 0, len(s.Errors))
		for k := range s.Errors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprint(w, "  errors     ")
		for _, k := range keys {
			fmt.Fprintf(w, " %s=%d", k, s.Errors[k])
		}
		fmt.Fprintln(w)
	}
	row := func(name string, p percentiles) {
		fmt.Fprintf(w, "  %-11s p50=%8.2f p90=%8.2f p95=%8.2f p99=%8.2f p99.9=%8.2f max=%8.2f ms\n",
			name, p.P50, p.P90, p.P95, p.P99, p.P999, p.Max)
	}
	row("ttft", s.TTFT)
	row("total", s.Total)
	if s.Stream {
		row("max gap", s.MaxGap)
	}
	fmt.Fprintf(w, "  schedule overrun %.1f ms (high values mean the generator itself could not keep up)\n", s.ScheduleSkewMS)
}
