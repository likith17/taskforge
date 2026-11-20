# TaskForge — Project Brief

## Problem

Applications constantly need to do work outside the request that triggered it:
send an email, resize an upload, call a slow third-party API, run a nightly
report. Doing that work inline makes requests slow and loses it when the
process restarts. Handing it to an in-memory queue loses it on a crash.

A background job system has to answer one question correctly under failure:
**"once I have accepted this job, what exactly do I promise?"** Most of the
engineering is in the failure cases — a worker dying mid-job, a database
failover during a status update, two workers racing for one job, a client
retrying a submission whose response it never saw.

TaskForge is a small job platform built to make those promises explicit and
testable, in the spirit of Celery, Sidekiq and Temporal but deliberately much
smaller.

## Stakeholders

| Who | What they need |
|---|---|
| Application developer (producer) | Submit a job in one HTTP call and trust it will run; look up its outcome. |
| Handler author | A simple function signature, clear retry semantics, and to be told plainly that a handler may run more than once. |
| Operator | One binary, one database, health endpoints, logs and metrics that explain what is stuck and why. |
| Reviewer of this portfolio | Evidence: tests that inject the failures, measurements instead of adjectives, and documented trade-offs. |

## Use cases

1. Fire-and-forget work that must survive restarts (emails, webhooks).
2. Delayed work ("send the reminder in 24 hours").
3. Prioritised work (interactive jobs ahead of bulk backfills).
4. Work against flaky dependencies (retry with backoff, then dead-letter).
5. Operational recovery: a worker node disappears and its jobs finish elsewhere.

Out of scope: multi-step workflows with durable state between steps
(Temporal's territory), cron-style recurring schedules, cross-region
deployment, and multi-tenant isolation.

## Baseline

Two baselines, used for different purposes:

- **Correctness baseline:** the naive queue most projects start with — `SELECT`
  the oldest queued row, then `UPDATE` it to running, with no row locking, no
  leases and no fencing. It is what TaskForge's guarantees are measured
  against: under concurrency it double-claims, and after a crash it strands
  jobs.
- **Performance baseline:** the same naive claim made safe with a plain
  `FOR UPDATE` (blocking) instead of `SKIP LOCKED`, run through the same
  harness. See [BENCHMARKS.md](BENCHMARKS.md).

An earlier Python prototype of the same idea lives in the parent directory. It
informed several decisions (see [DECISIONS.md](DECISIONS.md)) but is not a
benchmark baseline: it differs in language, architecture and feature set at
once, so a comparison would not isolate anything.

## Constraints

- Rust (stable), Tokio, Axum, PostgreSQL via SQLx. Redis only if a measured
  need justifies it — so far none has (ADR 0001).
- Runs from `docker compose` plus one binary on a developer laptop.
- Minimal dependencies; every one must earn its place.
- Only registered, predefined handlers execute. A job is a name plus JSON data,
  never code or a command line.
- Correctness over feature count: no milestone starts until the previous one
  builds, lints clean and passes its tests.

## Success metrics

Correctness (must hold in automated tests, including fault injection):

| Metric | Target |
|---|---|
| Jobs lost after acceptance (`201` returned) | 0 |
| Jobs with more than one committed result | 0 |
| Invalid state transitions persisted | 0 (rejected by the database itself) |
| Stale worker updates applied | 0 |
| Duplicate jobs from a retried submission with an idempotency key | 0 (from milestone 2) |

Operational (measured and reported, not asserted in advance):

- Throughput in jobs/second and how it scales with worker count.
- P50/P95/P99 queue wait time and per-job overhead.
- Time to recover a job after its worker is killed.
- Duplicate *execution attempts* under fault injection (expected to be > 0:
  delivery is at-least-once, and the number is reported honestly).

Portfolio: another developer can clone the repository, start the system,
submit jobs, kill a worker and watch recovery by following the README alone.
