# ADR 0001: Scope — a single-team, single-binary gateway built at ₹0

- Status: Accepted (2026-11-18)

## Context

ModelMesh is a portfolio and learning project with a hard rule: building, testing,
benchmarking, and demonstrating it must cost nothing beyond hardware the author already
owns. Two deployment shapes were considered:

- **A.** One team runs ModelMesh beside its own services. There is one trust domain, a
  static config, and a handful of API keys.
- **B.** A platform team runs one shared gateway for many internal apps. That needs per-app
  quotas, tenant isolation, usage attribution, an admin API, and live configuration.

## Decision

Build **A**. Concretely:

- Ship one Go binary (`modelmesh`), a separate mock provider (`mockllm`), and a load
  generator (`loadgen`).
- Keep all state in memory on a single node: breaker state, cooldowns, rate limits, and
  the retry budget. There is no database, Redis, or cluster.
- Use API keys from an environment variable or a file. Optional names (`name:key`) appear
  in logs, so different services of the same team can be told apart.
- Make every feature developable and testable against `mockllm`. Real providers stay
  optional (ADR 0007).

## Consequences

- Rate limits and breaker state are per-instance. Running several replicas multiplies
  per-key limits and makes each replica learn provider health on its own. This is
  documented, not hidden.
- Moving to B later means adding tenant identity, persistent usage accounting, and a
  config API. The places where B would attach already exist: the request context carries
  the authenticated key, and metrics have bounded labels.
- Scope stays focused on the hard Go problems: stream lifecycle, cancellation, and
  failure semantics.
