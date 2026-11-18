# Benchmarks

All numbers below were **measured**, on the machine described here, with the code in this
repository. The raw results (JSON per run, plus a text summary) are in
[`benchmark-results/2026-11-18/`](benchmark-results/2026-11-18/). To reproduce them:

```sh
scripts/bench.sh                  # end to end, all scenarios (~9 minutes)
make bench-micro                  # in-process Go benchmarks
```

These are measurements of one small machine, not universal performance claims. Read the
limitations section before quoting any number.

## Environment and method

| | |
|---|---|
| Machine | 2 vCPU VM, Intel Xeon @ 2.10 GHz, Linux 6.18, 7.8 GiB RAM |
| Go | 1.24.7 |
| Placement | `taskset`: the gateway alone on CPU 1; both mocks **and** the load generator on CPU 0, identically in the direct and the through-gateway runs |
| Load | `loadgen`, **open loop**: requests start on a fixed schedule whatever the server does, and every latency is measured from the scheduled start time, so a slow server cannot hide queueing delay (no coordinated omission) |
| Baseline | Every through-gateway run is paired with the same load sent **directly to the mock**. The difference is the gateway's cost. |
| Gateway settings | `configs/bench.json`; info-level JSON access log written to a file (logging cost included) |
| Gateway resources | CPU from `/proc/<pid>/stat` over the run; peak RSS from `VmHWM`; goroutines sampled every 200 ms from `/metrics` |
| TTFT | Time to the first SSE event the client receives |

## 1. Gateway overhead (instant mock, 64-token streams)

The mock answers instantly, so almost everything measured is gateway work plus loopback
networking.

| Rate | TTFT p50, direct → gateway | TTFT p99, direct → gateway | Total p99, direct → gateway | Gateway CPU per request | Gateway peak RSS |
|---|---|---|---|---|---|
| 200/s | 0.97 → 1.18 ms | 1.52 → 2.68 ms | 1.67 → 3.23 ms | 0.51 ms | 15.0 MB |
| 500/s | 0.88 → 1.06 ms | 1.60 → 3.04 ms | 1.75 → 3.32 ms | 0.47 ms | 15.1 MB |
| 1,000/s | 0.81 → 1.00 ms | 1.91 → 5.00 ms | 2.13 → 5.50 ms | 0.43 ms | 15.5 MB |

- **Latency:** the gateway adds about **0.2 ms at the median** and **1–3 ms at p99** to
  first-token latency at these rates.
- **CPU:** about 0.45 ms of gateway CPU per 64-event stream, or roughly 7 µs per event,
  including the access-log line. At 1,000 streams per second that is about 43% of one
  core.
- **Correctness:** every request succeeded in every run.

## 2. Realistic streams

The mock waits 200–300 ms for the first token, then emits 100 tokens at 50 per second, so
each stream lasts about 2.35 s.

| Rate (≈ open streams) | TTFT p50, direct → gateway | TTFT p99, direct → gateway | Total p99, direct → gateway | Gateway CPU per request | Peak RSS | Peak goroutines |
|---|---|---|---|---|---|---|
| 250/s (≈ 590) | 251.3 → 253.0 ms | 300.9 → 301.8 ms | 2,358 → 2,363 ms | 2.1 ms | 75 MB | 2,341 |
| 500/s (≈ 1,180) | 253.5 → 258.8 ms | 304.3 → 321.4 ms | 2,407 → 2,471 ms | 1.6 ms | 144 MB | 4,861 |

With realistic token pacing, events arrive one at a time and can't be batched. Each one
costs an upstream read, a client write, and a flush: roughly 15–20 µs of gateway CPU per
event, a few times the per-event cost in scenario 1.

A **1,000/s** run (≈ 2,350 open streams, ≈ 117,000 upstream events per second) was
attempted and **is not reported**. The single core shared by the load generator and the
mock saturated, and even the *direct* baseline degraded to a TTFT p99 above 13 s. That
run measured the test rig, not the gateway, and it was excluded for that reason.

## 3. Many open streams

Streams last 10 s at 10 tokens per second, with 400 new streams per second, so about
4,000 are open at once.

| | TTFT p50 | TTFT p99 | Total p99 | Gateway peak RSS | Peak goroutines |
|---|---|---|---|---|---|
| Direct | 52.4 ms | 132.9 ms | 10,331 ms | — | — |
| Gateway | 54.2 ms | 126.6 ms | 10,275 ms | **335 MB** | **16,264** |

- **Goroutines:** about **4.1 per open stream**. That is net/http's own cost:
  - the server's connection goroutine;
  - its background read for disconnect detection;
  - the transport's `readLoop`;
  - the transport's `writeLoop`.
- **Memory:** about **80 KB of RSS per open stream**. A heap profile taken during this
  scenario shows the per-connection `bufio` buffers as the largest heap items. Goroutine
  stacks (16,000 × at least 8 KB) and garbage-collector headroom make up most of the rest.
- **Buffer size:** shrinking the SSE read buffer from 16 KiB to 4 KiB changed peak RSS
  only slightly (337 → 332 MB in a back-to-back check), so that buffer was not the lever.
  HTTP/2 to upstreams would remove two goroutines per stream; it is future work.
- **Noise:** the gateway's p99 here (126.6 ms) is *lower* than the direct run's
  (132.9 ms). This is noise from the shared, busy CPU 0, which shows that differences of
  this size are not meaningful on this rig.

## 4. Faults: failover before the commit point, honesty after it

mock-a returns 503 for 20% of requests and breaks 4% of streams mid-way (2% connection
cuts, 2% stalls). mock-b is healthy. The rate is 200 requests per second for 20 s, with
model `fast` (mock-a first, then mock-b).

| | Success | What failed |
|---|---|---|
| Direct to mock-a | **77.2%** (3,086 / 4,000) | 800 × HTTP 503, 52 cut streams, 62 stalled streams (hit the client's 10 s timeout) |
| Through the gateway | **96.9%** (3,875 / 4,000) | 57 `upstream_interrupted` and 68 `upstream_stalled` in-band error events |

Gateway metrics for the run:

```
modelmesh_upstream_attempts_total{provider="mock-a",result="error_503"} 820
modelmesh_upstream_attempts_total{provider="mock-a",result="stream_interrupted"} 125
modelmesh_upstream_attempts_total{provider="mock-a",result="success"} 3055
modelmesh_upstream_attempts_total{provider="mock-b",result="success"} 820
modelmesh_failovers_total{model="fast"} 820
modelmesh_breaker_transitions_total{provider="mock-a",to="open"} 1
```

This is the commit point (ADR 0003), measured:

- **Before the first byte:** every one of the 820 failures was failed over to mock-b, and
  no client saw it.
- **After the first byte:** the 125 mid-stream failures reached clients as explicit error
  events within the idle timeout (5 s). That is better than the direct case, where stalled
  clients waited for their own 10 s timeout. None of them was silently retried.
- **Latency:** TTFT p50 51.3 → 51.7 ms and p99 52.5 → 54.9 ms. Failover after an instant
  503 costs almost nothing.

## 5. Circuit breaker against a black-hole provider

mock-a never answers. mock-b is healthy. `first_token` is 2 s, and the load is 50
requests per second for 20 s.

| | Success | TTFT p50 | TTFT p99 | Gateway CPU per request |
|---|---|---|---|---|
| Breaker disabled (`failure_threshold` 1,000,000) | 100% | **2,052 ms** | 2,055 ms | 1.9 ms |
| Breaker enabled (threshold 5, open 5 s) | 100% | **51.8 ms** | 2,054 ms | 1.6 ms |

- **Without a breaker:** every request waits the full first-token timeout on the dead
  provider before failing over.
- **With a breaker:** only the requests that trip it, plus one half-open probe every 5 s
  (about 1% of requests), pay that 2 s. That is why p99 stays at 2 s while the median
  drops 40×.

## 6. Client cancellation at scale

300 streams per second for 15 s (4,500 total). Each client disconnects 300 ms into a
10-second stream.

| Gateway goroutines before | After | Peak during | mock-a upstream requests still active after |
|---|---|---|---|
| 7 | **7** | 371 | **0** |

Every disconnect cancelled its upstream request, and nothing leaked.

## 7. Load shedding

400 new 10-second streams per second (≈ 4,000 concurrent were offered), against a gateway
limited to `max_concurrent_requests` = 1,000.

| Served | Shed (immediate 503) | TTFT of admitted requests, p50 / p99 | Peak RSS | Peak goroutines |
|---|---|---|---|---|
| 1,000 | 3,000 | 52.0 / 72.0 ms | 97 MB | 4,014 |

Admitted streams kept their normal latency, and memory stayed bounded because excess load
was rejected rather than queued.

## In-process microbenchmarks

`make bench-micro` (`go test -bench`): client, mock, and gateway all run in one process
on loopback, `-count 3`, and representative values are shown.

| Benchmark | Time | Allocated | Allocations |
|---|---|---|---|
| SSE parse, per event | ~35 ns | 0 B | 0 |
| Chunk inspection (`json.Valid` + field scan), per chunk | ~600 ns | 0 B | 0 |
| 64-token stream, direct to mock | ~180 µs | 9.4 KB | 119 |
| 64-token stream, through the gateway | ~360 µs | 26.3 KB | 365 |
| Non-streaming, direct | ~36 µs | 11.7 KB | 156 |
| Non-streaming, through the gateway | ~108 µs | 28.1 KB | 360 |

The gateway adds about 16.8 KB and 246 allocations per streamed request. Most of that is
the second HTTP hop: request and header objects, contexts, and the transport. Per-chunk
JSON validation is the largest CPU item the gateway itself adds. It is kept on purpose
(ADR 0002).

## What the measurements changed

Each of these was found by a benchmark or a test written for the failure matrix, then
fixed and pinned by a regression test.

1. **Breaker success was recorded at stream end.** Failures were recorded instantly and
   successes about 300 ms later. In the faults scenario that produced runs of "5
   consecutive failures" from a provider failing only 20% of the time. mock-a was skipped
   for 98.6% of requests, and its mid-stream faults were hidden by accident. Success is
   now recorded at the commit point, and later breaks are reported with `Breaker.Fail()`.
   The breaker now opens once in the same run instead of four times.
   (`TestBreakerCountsStreamSuccessAtCommit`)
2. **The retry budget also charged failovers.** In the black-hole scenario this produced
   **33.9%** success even though mock-b was healthy. Failover to an untried provider is
   now free, and only repeat attempts spend budget. Success is now 100%.
   (`TestFailoverDoesNotSpendRetryBudget`)
3. **Per-stream read buffers.** A memory profile showed `bufio.NewReaderSize` at 36% of
   all bytes allocated in the streaming benchmark. Pooling SSE readers cut bytes per
   streamed request from 42.9 KB to 26.3 KB.
4. **Flush coalescing could hold an event for 200 ms** behind half of the next event.
   (`TestEventIsNotHeldBehindPartialEvent`)
5. **Upstream connections were not reused** when the provider ended its response
   slightly after `[DONE]`: 10 requests opened 10 connections, and now they open 1.
   (`TestConnectionReusedWhenTerminatorArrivesLate`)
6. **A slow request body hung instead of returning 408.** net/http tried to drain the
   unread body before sending the response. (`TestSlowRequestBodyGets408`)

## Limitations

- **Hardware:** one small 2-vCPU VM. The load generator and mocks share one core, so the
  rig, not the gateway, sets the ceiling on the heavier scenarios.
- **Repetitions:** the end-to-end scenarios ran once per configuration. Their run-to-run
  variance was not quantified. Only the microbenchmarks were repeated (3×).
- **Network:** loopback only. Real deployments add network latency on both hops, which
  makes the gateway's share of total latency even smaller.
- **No comparison with other gateways.** They were not run here, and vendor-published
  figures use different hardware and methods, so they are not comparable with these.
- **Go version:** Go 1.24.7. Go 1.27's faster JSON decoding and allocation changes may
  shift these numbers somewhat.
