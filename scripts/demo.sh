#!/usr/bin/env bash
# A scripted tour of ModelMesh's behavior using two local mock providers.
# Needs: Go, curl, jq. Costs nothing; everything stays on 127.0.0.1.
set -euo pipefail
cd "$(dirname "$0")/.."

export MODELMESH_API_KEYS="demo:sk-demo-local-key"
KEY="sk-demo-local-key"
GW=http://127.0.0.1:8080
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

c() { curl -s --noproxy '*' "$@"; }
chat() { # model stream [extra curl args...]
  local model=$1 stream=$2; shift 2
  c -H "Authorization: Bearer $KEY" "$GW/v1/chat/completions" \
    -d "{\"model\":\"$model\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}],\"stream\":$stream}" "$@"
}
profile() { c -X PUT "http://127.0.0.1:$1/_mock/profile" -d "$2" >/dev/null; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

go build -o bin/ ./cmd/...
# Demo config: the local setup with shorter timeouts so failures show up quickly.
sed -e 's/"first_token": "10s"/"first_token": "3s"/' -e 's/"idle": "15s"/"idle": "3s"/' \
    -e 's/"open_duration": "10s"/"open_duration": "30s"/' configs/local.json > /tmp/modelmesh-demo.json
bin/mockllm -listen 127.0.0.1:9001 -ttft 100ms -tps 40 -tokens 12 2>/dev/null & PIDS+=($!)
bin/mockllm -listen 127.0.0.1:9002 -ttft 100ms -tps 40 -tokens 12 2>/dev/null & PIDS+=($!)
bin/modelmesh -config /tmp/modelmesh-demo.json >/tmp/modelmesh-demo.log 2>&1 & GWPID=$!; PIDS+=($GWPID)
for _ in $(seq 50); do c -o /dev/null "$GW/healthz" && break; sleep 0.1; done

step "1. A streaming request through the gateway (headers show the routing decision)"
chat fast true -D - | grep -E '^(HTTP|X-Modelmesh|data)' | head -6
echo "   …"

step "2. Client disconnect reaches the provider: curl gives up after 0.5s of a 30s stream"
profile 9001 '{"tokens_per_second":2,"output_tokens":60}'
before=$(c http://127.0.0.1:9001/_mock/stats | jq .cancelled)
chat single true --max-time 0.5 -o /dev/null || true
sleep 0.2
after=$(c http://127.0.0.1:9001/_mock/stats | jq .cancelled)
echo "   mock-a cancelled requests: $before -> $after; gateway in-flight now: $(c http://127.0.0.1:9090/metrics | awk '$1=="modelmesh_inflight_requests"{print $2}')"
profile 9001 '{"tokens_per_second":40,"output_tokens":12}'

step "3. Failover before the commit point: mock-a now returns 503 for every request"
profile 9001 '{"error_rate":1}'
chat fast false -D - -o /dev/null | grep -iE '^x-modelmesh-(provider|attempts)'

step "4. Circuit breaker: after 5 consecutive failures mock-a is skipped without an attempt"
for _ in 1 2 3 4; do chat fast false -o /dev/null; done
c http://127.0.0.1:9090/admin/providers | jq -c '.providers[] | {name, breaker}'
chat fast false -D - -o /dev/null | grep -iE '^x-modelmesh-(provider|attempts)'

step "5. A model whose only provider has an open breaker fails fast (no upstream call)"
chat single false -D - | grep -E '^(HTTP|Retry-After|\{)'

step "6. Failure after the commit point: mock-b stalls mid-stream; the client gets an in-band error, no retry"
profile 9002 '{"stall_rate":1,"fault_after_tokens":4}'
chat fast true | grep -v "^$" | tail -2
profile 9002 '{"stall_rate":0,"tokens_per_second":10,"output_tokens":15}'

step "7. Metrics (excerpt)"
c http://127.0.0.1:9090/metrics | grep -E '^modelmesh_(requests_total|failovers_total|breaker_transitions_total|upstream_attempts_total)' | head -14

step "8. Graceful shutdown with a stream in flight: the stream finishes, then the process exits"
chat fast true > /tmp/modelmesh-demo-stream.txt &
STREAM=$!
sleep 0.4
kill -TERM "$GWPID"
wait "$STREAM" || true
echo "   events received: $(grep -c '^data: {' /tmp/modelmesh-demo-stream.txt); last line: $(grep -v '^$' /tmp/modelmesh-demo-stream.txt | tail -1)"
wait "$GWPID" 2>/dev/null || true
grep -E '"shutdown (started|complete)"' /tmp/modelmesh-demo.log | jq -c '{msg}'
echo
echo "Access log: /tmp/modelmesh-demo.log"
