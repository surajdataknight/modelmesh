#!/usr/bin/env bash
# Reproducible end-to-end benchmarks. Everything runs locally against mock
# providers; nothing here costs money or touches the network beyond loopback.
#
#   scripts/bench.sh                 # all scenarios
#   scripts/bench.sh overhead        # one scenario
#
# Placement (Linux, when taskset exists): the gateway gets its own CPU; the
# mock providers and the load generator share the remaining CPUs, in both the
# "direct" baseline and the "via gateway" runs, so the comparison is fair.
# See docs/benchmarks.md for the methodology and its limitations.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT="bench-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
export MODELMESH_API_KEYS="bench:sk-bench-local-key"
KEY="sk-bench-local-key"
GW=http://127.0.0.1:8080/v1/chat/completions
MOCK_A=http://127.0.0.1:9001/v1/chat/completions
NCPU=$(nproc)
PIDS=()

# Arrays, not functions: a backgrounded shell function runs in a subshell,
# so $! would name the subshell instead of the process we need to measure
# and kill. taskset execs its command, so $! is the real PID.
if command -v taskset >/dev/null && [ "$NCPU" -ge 2 ]; then
  GW_CPUS="$((NCPU - 1))"
  REST_CPUS="0-$((NCPU - 2))"
  PIN_GW=(taskset -c "$GW_CPUS")
  PIN_REST=(taskset -c "$REST_CPUS")
else
  PIN_GW=(env)
  PIN_REST=(env)
fi

cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
  PIDS=()
  sleep 0.5 # let ports close before the next scenario
}
trap cleanup EXIT

go build -trimpath -o bin/ ./cmd/...

wait_http() { for _ in $(seq 100); do curl -s --noproxy '*' -o /dev/null "$1" && return 0; sleep 0.05; done; echo "timeout waiting for $1" >&2; return 1; }

start_mocks() { # $1 = mock-a flags, $2 = mock-b flags
  # shellcheck disable=SC2086
  "${PIN_REST[@]}" bin/mockllm -listen 127.0.0.1:9001 $1 2>"$OUT/mock-a.log" & PIDS+=($!)
  # shellcheck disable=SC2086
  "${PIN_REST[@]}" bin/mockllm -listen 127.0.0.1:9002 $2 2>"$OUT/mock-b.log" & PIDS+=($!)
  wait_http http://127.0.0.1:9001/healthz; wait_http http://127.0.0.1:9002/healthz
}

start_gateway() { # $1 = config file
  "${PIN_GW[@]}" bin/modelmesh -config "$1" >"$OUT/gateway-$2.log" 2>&1 & GW_PID=$!; PIDS+=($GW_PID)
  wait_http http://127.0.0.1:8080/healthz
}

cpu_seconds() { awk '{print ($14+$15)/100}' "/proc/$1/stat"; }
rss_mb() { awk '/VmRSS/{printf "%.1f", $2/1024}' "/proc/$1/status"; }
hwm_mb() { awk '/VmHWM/{printf "%.1f", $2/1024}' "/proc/$1/status"; }
metric() { curl -s --noproxy '*' http://127.0.0.1:9090/metrics | awk -v m="$1" '$1==m {print $2}'; }

loadgen() { # name url model rate duration [extra flags...]
  local name=$1 url=$2 model=$3 rate=$4 dur=$5; shift 5
  "${PIN_REST[@]}" bin/loadgen -url "$url" -key "$KEY" -model "$model" -rate "$rate" -duration "$dur" -label "$name" -json "$@" >"$OUT/$name.json"
  jq -r '"  \(.label): ok=\(.completed_ok)/\(.scheduled) success=\(.success_rate*100|floor)% ttft p50=\(.ttft.p50_ms)ms p99=\(.ttft.p99_ms)ms total p99=\(.total.p99_ms)ms errors=\(.errors)"' "$OUT/$name.json"
}

# Gateway resource usage over one load run: CPU-ms per completed request,
# peak RSS, and peak goroutines (sampled from /metrics every 200ms).
measure() { # name url model rate duration [extra...]
  local c0 c1 peak=0 g
  c0=$(cpu_seconds "$GW_PID")
  ( while :; do g=$(metric go_goroutines); [ -n "$g" ] && echo "$g"; sleep 0.2; done ) >"$OUT/$1.goroutines" & local sampler=$!
  loadgen "$@"
  kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true
  c1=$(cpu_seconds "$GW_PID")
  peak=$(sort -n "$OUT/$1.goroutines" | tail -1)
  jq --arg cpu "$(echo "$c1 - $c0" | bc -l)" --arg rss "$(hwm_mb "$GW_PID")" --arg gr "$peak" \
    '. + {gateway_cpu_seconds: ($cpu|tonumber), gateway_peak_rss_mb: ($rss|tonumber), gateway_peak_goroutines: ($gr|tonumber),
          gateway_cpu_ms_per_ok_request: (if .completed_ok > 0 then ($cpu|tonumber) * 1000 / .completed_ok else null end)}' \
    "$OUT/$1.json" >"$OUT/$1.tmp" && mv "$OUT/$1.tmp" "$OUT/$1.json"
  jq -r '"    gateway: cpu=\(.gateway_cpu_seconds)s total, \(.gateway_cpu_ms_per_ok_request // "n/a")ms per ok request; peak_rss=\(.gateway_peak_rss_mb)MB peak_goroutines=\(.gateway_peak_goroutines)"' "$OUT/$1.json"
}

scenario_overhead() {
  echo "== overhead: 64-token streams, instant mock (isolates gateway cost)"
  start_mocks "-ttft 0 -tokens 64" "-ttft 0 -tokens 64"
  start_gateway configs/bench.json overhead
  for rate in 200 500 1000; do
    loadgen "direct-$rate" "$MOCK_A" mock-small "$rate" 15s
    measure "gateway-$rate" "$GW" a-only "$rate" 15s
  done
  cleanup
}

scenario_concurrency() {
  echo "== concurrency: realistic streams (TTFT 200-300ms, 50 tok/s, 100 tokens => ~2.3s each)"
  start_mocks "-ttft 200ms -ttft-jitter 100ms -tps 50 -tokens 100" "-ttft 0 -tokens 1"
  start_gateway configs/bench.json concurrency
  for rate in 250 500; do
    loadgen "conc-direct-$rate" "$MOCK_A" mock-small "$rate" 20s
    measure "conc-gateway-$rate" "$GW" a-only "$rate" 20s
  done
  cleanup
}

scenario_manystreams() {
  echo "== many open streams: 10s streams at 10 tok/s, 400 new/s => ~4,000 concurrent"
  start_mocks "-ttft 50ms -tps 10 -tokens 100" "-ttft 0 -tokens 1"
  start_gateway configs/bench.json manystreams
  loadgen "many-direct" "$MOCK_A" mock-small 400 15s
  measure "many-gateway" "$GW" a-only 400 15s
  cleanup
}

scenario_faults() {
  echo "== faults: mock-a returns 503 for 20% of requests and breaks 4% of streams mid-way (cut/stall); mock-b healthy"
  local a="-ttft 50ms -tps 200 -tokens 50 -error-rate 0.2 -cut-rate 0.02 -stall-rate 0.02 -seed 7"
  start_mocks "$a" "-ttft 50ms -tps 200 -tokens 50"
  start_gateway configs/bench.json faults
  loadgen "faults-direct-a" "$MOCK_A" mock-small 200 20s -timeout 10s
  measure "faults-gateway-failover" "$GW" fast 200 20s -timeout 10s
  curl -s --noproxy '*' http://127.0.0.1:9090/metrics | grep -E '^modelmesh_(requests_total|upstream_attempts_total|failovers_total|retries_total|provider_skips_total|breaker_transitions_total)' >"$OUT/faults-metrics.txt" || true
  cleanup
}

scenario_breaker() {
  echo "== breaker: mock-a is a black hole (never answers); first_token timeout is 2s"
  start_mocks "-hang-rate 1" "-ttft 50ms -tps 200 -tokens 20"
  sed 's/"failure_threshold": 5/"failure_threshold": 1000000/' configs/bench.json >"$OUT/bench-nobreaker.json"
  start_gateway "$OUT/bench-nobreaker.json" nobreaker
  measure "breaker-disabled" "$GW" fast 50 20s
  cleanup
  start_mocks "-hang-rate 1" "-ttft 50ms -tps 200 -tokens 20"
  start_gateway configs/bench.json breaker
  measure "breaker-enabled" "$GW" fast 50 20s
  cleanup
}

scenario_cancel() {
  echo "== cancellation: clients disconnect after 300ms from 10s streams"
  start_mocks "-ttft 50ms -tps 20 -tokens 200" "-ttft 0 -tokens 1"
  start_gateway configs/bench.json cancel
  sleep 1
  local before; before=$(metric go_goroutines)
  measure "cancel-gateway" "$GW" a-only 300 15s -cancel-after 300ms
  sleep 1
  local after active
  after=$(metric go_goroutines)
  active=$(curl -s --noproxy '*' http://127.0.0.1:9001/_mock/stats | jq .active)
  echo "    goroutines before=$before after=$after; mock-a active upstream requests after=$active"
  jq --arg b "$before" --arg a "$after" --arg act "$active" \
    '. + {goroutines_before: ($b|tonumber), goroutines_after: ($a|tonumber), upstream_active_after: ($act|tonumber)}' \
    "$OUT/cancel-gateway.json" >"$OUT/c.tmp" && mv "$OUT/c.tmp" "$OUT/cancel-gateway.json"
  cleanup
}

scenario_shed() {
  echo "== load shedding: 4,000 concurrent 10s streams offered to a gateway limited to 1,000"
  start_mocks "-ttft 50ms -tps 10 -tokens 100" "-ttft 0 -tokens 1"
  sed 's/"max_concurrent_requests": 8192/"max_concurrent_requests": 1000/' configs/bench.json >"$OUT/bench-shed.json"
  start_gateway "$OUT/bench-shed.json" shed
  measure "shed-gateway" "$GW" a-only 400 10s
  cleanup
}

{
  echo "host: $(uname -srm), $(nproc) CPUs, $(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo | xargs)"
  echo "go: $(go version)"
  echo "placement: gateway on CPU ${GW_CPUS:-any}; mocks + loadgen on CPUs ${REST_CPUS:-any}"
  echo "date: $(date -Iseconds)"
} | tee "$OUT/environment.txt"

SCENARIOS=("${@:-overhead concurrency manystreams faults breaker cancel shed}")
for s in ${SCENARIOS[*]}; do "scenario_$s"; done
echo "results: $OUT"
