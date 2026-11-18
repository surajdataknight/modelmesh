# ADR 0008: Go version floor is 1.24

- Status: Accepted (2026-11-18). This supersedes the earlier proposal to target Go 1.27.

## Context

Go 1.27 (August 2026) is current and adds features that would help:

- `httptest.NewTestServer` on an in-memory network for `testing/synctest`;
- a generally available goroutine leak profile;
- `encoding/json` backed by the v2 implementation;
- automatic draining of HTTP/1 response bodies on close.

The environment this was built and tested in had Go 1.24.7 and no network access to fetch
a newer toolchain.

## Decision

- `go.mod` declares `go 1.24`, and nothing requires a newer release.
- The code uses Go 1.24 APIs only: `http.ResponseController`, context causes,
  `math/rand/v2`, `log/slog`, `testing.B.Loop`, and `slog.DiscardHandler`.
- Leak checks use goroutine counts and resource gauges instead of the 1.27 leak profile.

## Consequences

- The code builds unchanged on 1.27. The results in `docs/benchmarks.md` were produced
  with 1.24.7.
- On 1.27, the explicit drain after `[DONE]` (ADR 0004) overlaps with the standard
  library's new automatic drain. It is harmless and still needed on older toolchains.
