# ModelMesh developer tasks. Everything runs locally and costs nothing.
GO       ?= go
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)
export MODELMESH_API_KEYS ?= dev:sk-dev-local-key

.PHONY: all build test race vet fmt-check lint cover bench-micro bench demo run run-mocks docker compose-up clean ci

all: lint race build

build: ## Build modelmesh, mockllm, loadgen into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/ ./cmd/...

test: ## Unit and integration tests
	$(GO) test ./...

race: ## Tests with the race detector
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

lint: fmt-check vet

cover: ## Coverage report (coverage.out)
	$(GO) test -coverprofile=coverage.out ./internal/...
	$(GO) tool cover -func=coverage.out | tail -1

bench-micro: ## In-process benchmarks: SSE parsing, chunk inspection, gateway overhead
	$(GO) test -run xxx -bench . -benchmem ./internal/...

bench: build ## End-to-end benchmarks against mock providers (Linux)
	scripts/bench.sh

demo: build ## Scripted tour of failover, breaker, in-band errors, cancellation, shutdown
	scripts/demo.sh

run-mocks: build ## Two mock providers on :9001 and :9002 (Ctrl-C to stop)
	bin/mockllm -listen 127.0.0.1:9001 -ttft 150ms -tps 40 -tokens 40 & \
	bin/mockllm -listen 127.0.0.1:9002 -ttft 150ms -tps 40 -tokens 40 & \
	wait

run: build ## Gateway with configs/local.json
	bin/modelmesh -config configs/local.json

docker: ## Container image (untested in the original build environment)
	docker build -t modelmesh:$(VERSION) .

compose-up: ## Gateway + two mocks in containers
	docker compose up --build

ci: lint race build

clean:
	rm -rf bin coverage.out bench-results
