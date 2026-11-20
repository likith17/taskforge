# Implementation plan

A milestone is done when the workspace builds, `cargo fmt --check` and
`cargo clippy -- -D warnings` are clean, and the milestone's tests pass. The
next milestone does not start before that.

## Milestone 1 — Working vertical slice ✅

- [x] Cargo workspace: `domain`, `storage`, `api`, `worker`, binary
- [x] PostgreSQL via Docker Compose
- [x] Migration 0001: `jobs` table, constraints, indexes, transition trigger
- [x] `POST /jobs`, `GET /jobs/{id}`, `/healthz`, `/readyz`
- [x] Worker that claims (`FOR UPDATE SKIP LOCKED`) and executes predefined
      job kinds (`echo`, `sum`, `sleep`, `fail`)
- [x] Status, result and error persisted; settle fenced by (worker, attempt)
- [x] Unit and integration tests — 50 passing
- [x] Design docs: brief, architecture, ADRs, decisions, failure analysis,
      benchmark methodology

Evidence: `cargo test --workspace` (50 tests), clippy clean, and the manual
smoke run in the README (submit → succeeded; failing job → dead; unknown kind
→ 422; queued job survives a hard kill and runs after restart).

## Milestone 2 — Reliability

- [ ] Migration 0002: `max_attempts`, `failures`, `idempotency_key`,
      `request_hash`, `cancel_requested`, `timeout_ms`, `job_attempts`
- [ ] Concurrent worker pool (bounded by a semaphore)
- [ ] Heartbeats: fenced lease renewal every `lease / 3`; self-fencing
- [ ] Scheduler role: reaper for expired leases (`SKIP LOCKED`)
- [ ] Retry policy: exponential backoff + jitter (pure, property-tested)
- [ ] Dead-letter after exhaustion; `POST /jobs/{id}/retry`
- [ ] Idempotency keys on `POST /jobs`
- [ ] `GET /jobs` (list/filter) and `POST /jobs/{id}/cancel`, with the
      cancel-vs-complete race rule from ARCHITECTURE §6
- [ ] Bounded retry of the settle write (closes F12)
- [ ] Tests: worker killed mid-job is recovered; zombie result rejected;
      cancel/complete race; duplicate-submission storm

## Milestone 3 — Production engineering

- [ ] Prometheus metrics (`/metrics`): queue depth, throughput, failures,
      claim/settle latency
- [ ] Tracing spans across submit → claim → execute → settle
- [ ] Graceful shutdown with a drain deadline; per-job timeouts
- [ ] Backpressure: bounded concurrency, pool sizing, submit-side limits
- [ ] Least-privilege database roles (migration vs application)
- [ ] Fault-injection suite (FAILURE_ANALYSIS §4) with real process kills
- [ ] Load tests and the baseline comparison (BENCHMARKS.md)
- [ ] GitHub Actions: fmt, clippy, tests against a PostgreSQL service

## Milestone 4 — Demonstration

- [ ] Minimal dashboard: job status, queue depth, throughput, failures
- [ ] Dockerfile and full Compose deployment (API + N workers)
- [ ] Benchmark results written up
- [ ] Scripted failure-recovery demo
- [ ] Final README pass and API examples
