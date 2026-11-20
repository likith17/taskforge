# ADR 0002 — Polling first; notifications only as a measured optimisation

Status: accepted for Milestone 1 · to be revisited with benchmark data in Milestone 3

## Context

An idle worker has to find out that a job has arrived. It can ask repeatedly
(polling) or be told (PostgreSQL `LISTEN/NOTIFY`).

## Options

**Polling.** The worker claims in a loop; when a claim returns nothing it
sleeps `poll_interval` (250 ms by default). When a claim succeeds it loops
again immediately, so a busy queue is drained without any sleeping.

- Simple; no extra connection; nothing to reconnect after an outage.
- Idle cost: one cheap indexed query per worker per interval.
- Latency: up to one interval for a job arriving at an idle worker.

**LISTEN/NOTIFY.** The submitter issues `NOTIFY`; workers hold a listening
connection and wake at once.

- Near-zero pickup latency when idle.
- Each listener pins a connection outside the pool (and does not work through
  transaction-mode poolers such as PgBouncer).
- Notifications are not durable: one sent while a listener is reconnecting is
  lost. So polling is still required as a fallback — notifications can only
  ever be an optimisation on top of it.
- Does not help delayed jobs, which become due without any event.

## Decision

Milestone 1 uses polling only. It is the mechanism that must exist anyway, and
it is sufficient for correctness.

`LISTEN/NOTIFY` will be added only if the queue-wait measurements in
BENCHMARKS.md show that idle pickup latency matters for the target workloads,
and then strictly as a wake-up hint with polling retained.

## Consequences

- Worst-case idle pickup latency equals `TASKFORGE_POLL_INTERVAL_MS`.
- Idle workers generate a steady trickle of queries; the count is predictable
  (`workers / interval`).
- The earlier Python prototype in this repository used long-polling plus
  `NOTIFY`. It woke idle workers without waiting for a poll, at the cost of a
  reconnect loop and a missed-notification race that needed careful handling. That experience is
  the reason this design starts without it.
