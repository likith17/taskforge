# Architecture

## Components

```
  producers                                   operators / Prometheus
     | POST /jobs (Idempotency-Key)                | GET /jobs, /metrics
     v                                             v
 +------------------------- taskforge serve --------------------------+
 |  Axum router (generic over JobStore) · validation · /metrics        |
 +---------------------------------+-----------------------------------+
                                   | SQL (one statement per transition)
                                   v
 +---------------------------------------------------------------------+
 |                         PostgreSQL                                   |
 |  jobs            the queue: state, lease, fencing token, result      |
 |  job_attempts    one row per delivery: worker, outcome, error        |
 |  constraints + trigger: illegal states and transitions are refused   |
 +------^-------------------------^--------------------------^----------+
        | claim (SKIP LOCKED)     | heartbeat (own pool)     | reap expired leases
        | complete/fail/release   |                          | purge finished jobs
 +------+-------------------------+------+          +--------+----------------+
 |  taskforge worker  (x N processes)    |          | taskforge scheduler     |
 |  claim loop · semaphore (backpressure)|          | (optional: every worker |
 |  handler tasks · per-job lease timer  |          |  also runs a reaper)    |
 |  built-in reaper · /metrics           |          +-------------------------+
 +---------------------------------------+
```

PostgreSQL is the only stateful component and the only coordinator: there is no
broker and no leader election. API, worker and scheduler processes are stateless
and can be killed, restarted and scaled independently. Any number of each may
run at once.

## Life of a job

```
 submit ──► queued ──claim──► running ──complete──► succeeded
              ▲  │              │
              │  │              ├── fail, retryable, attempts left ──► queued (run_at = now + backoff)
              │  │              ├── fail, fatal or out of attempts  ──► dead ──re-drive──► queued
              │  │              ├── lease expired (worker gone)     ──► same rule as fail
              │  │              ├── cancel acknowledged             ──► cancelled
              │  │              └── released (graceful shutdown)    ──► queued (no failure counted)
              │  └── cancel ──► cancelled
```

1. **Submit.** Validated in the domain crate, fingerprinted, inserted as
   `queued`. Durable once `201` is returned.
2. **Claim.** One statement picks the best due job (priority, then `run_at`)
   with `FOR UPDATE SKIP LOCKED`, sets `running`, increments `attempt` (the
   fencing token), writes `lease_expires_at = now() + lease`, and opens a
   `job_attempts` row.
3. **Run.** The handler runs in its own task. Three things can stop it: its
   `timeout_secs`, a cancellation seen by the heartbeat, or its own lease timer.
4. **Heartbeat.** Every `lease/3` the worker renews all its leases in one
   statement, through a dedicated pool. The response lists leases it no longer
   holds and jobs someone asked to cancel.
5. **Settle.** `complete`, `fail`, `release` or `acknowledge_cancel`, each a
   single `UPDATE ... WHERE id AND state='running' AND lease_owner AND attempt`
   that also closes the attempt row. If the claim is stale, nothing is written.
   Writes are retried through database outages for up to one lease; repeats are
   safe because of the same guard.
6. **Recover.** Reapers move `running` rows whose lease has expired back through
   the failure rule (retry with backoff, or dead-letter), using `SKIP LOCKED` so
   each lease is handled once.

## Why two attempts never run at the same time

```
 worker A    claim sent ─┐                         A's local deadline (0.9 × lease)
                         │  ◄─────── lease ───────►│     │
 database                └► lease written ────────────────────► expiry ──► reaper may re-queue
                                                         ▲
                                                 A has stopped by here
```

The worker's deadline is measured from when it *sent* the claim or renewal,
which is no later than when the database wrote it. Fencing at 90% leaves a
margin for clock-rate drift and abort latency. Even if a handler somehow
outlived its lease, its result would be rejected by the fencing token. The
guarantee does not cover handlers that never yield (CPU-bound or blocking
code), because abort only lands at an `.await`.

## Code structure

```
 domain      pure rules: JobState, decide_failure, RetryPolicy, validation,
   ▲         request fingerprint, Clock, Jitter                   (no I/O)
   │
 storage     trait JobStore ── PgJobStore  (all SQL; DB clock; Jitter injected)
   ▲                       └── MemJobStore (Clock + Jitter injected; pure rules)
   │         conformance tests: same scenarios, both stores
   │
 api · worker · scheduler      generic over S: JobStore
   ▲
 taskforge (binary)            config, roles, pools, chaos job kind
 chaos (binary)                real-process fault injection and benchmarks
```

The worker, reaper and API never touch SQL. They depend on the `JobStore`
contract, so a deterministic simulation can run them on `MemJobStore` with a
`ManualClock` and a seeded `SplitMix64`, and give each worker its own
`LocalTime` timer. DECISIONS.md 3.1 explains why the
PostgreSQL store expresses the rules in SQL rather than calling the domain
functions, and how the conformance tests keep the two in step.

## Data model

`jobs` (migrations `0001`–`0003`):

| Column | Notes |
|---|---|
| `id` uuid | UUIDv7 from the API |
| `queue`, `kind`, `payload` jsonb, `priority` | Validated input |
| `state` | `CHECK` + transition trigger |
| `run_at` | Earliest claim time; moved forward by backoff |
| `attempt` | Fencing token; trigger forbids decrease |
| `failures`, `max_attempts`, `timeout_secs` | Retry budget and per-attempt limit |
| `lease_owner`, `lease_expires_at` | Set exactly while `running` (`CHECK`) |
| `idempotency_key` (unique, partial), `request_hash` | Exactly-once submission |
| `cancel_requested` | Pending cancellation for a running job |
| `result`, `last_error` | `result` only when `succeeded` (`CHECK`) |
| `created_at`, `updated_at`, `started_at`, `finished_at` | `finished_at` exactly in terminal states (`CHECK`) |

Indexes: claim path `(queue, priority DESC, run_at, id) WHERE state='queued'`;
reaper `(lease_expires_at) WHERE state='running'`; purge
`(finished_at) WHERE finished_at IS NOT NULL`; listing `(state, id DESC)`;
idempotency `UNIQUE (idempotency_key) WHERE idempotency_key IS NOT NULL`.

`job_attempts (job_id, attempt)` records one row per delivery: worker,
`started_at`, `finished_at`, outcome and error. An outcome of `NULL` means the
attempt is in flight.
