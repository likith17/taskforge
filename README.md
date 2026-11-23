# TaskForge

A fault-tolerant background job scheduler in Rust, on PostgreSQL. Producers
submit jobs over HTTP; any number of worker processes run them. Workers and the
scheduler can be killed at any moment without losing a job or committing one
twice; the chaos experiments below do exactly that, hundreds of times per run.
The API keeps no state either: a client whose request was cut off retries it
with the same `Idempotency-Key`.

The engineering ideas come from Celery, Sidekiq and Temporal. The scope is
deliberately much smaller: one binary, one database, no broker.

## The problem, and what TaskForge promises

Background work has to survive the things that happen to background work:
workers crash mid-job, networks partition, clients retry requests whose response
they never saw, two workers race for the same job. A job system is defined by
what it promises when those happen.

**TaskForge promises:**

- An accepted job is never lost. It is durable once `POST /jobs` returns `201`.
- A job is never held by two workers at once, and two attempts never run at the
  same time (for handlers that reach an `.await`; see Known limitations).
- **At most one result is ever committed per job.**
- A retried or duplicated submission with the same `Idempotency-Key` creates one
  job.
- A job whose worker dies is recovered, retried with backoff, and dead-lettered
  with its last error once its attempts are used up.

**It does not promise that a handler runs only once, and no system can.** A
worker can finish the work and die before recording it. That is
indistinguishable from dying before starting, so the job must run again or risk
being lost. Execution is therefore **at-least-once**, and results are
exactly-once. Handlers with external side effects get a stable job id to use as
an idempotency key downstream. The chaos experiments measure both halves:
re-executions happen (5–6% of jobs under heavy killing), and duplicate
committed results stay at zero. See [DECISIONS.md §1.1](DECISIONS.md).

## Architecture

```
  producers ── POST /jobs (Idempotency-Key) ──┐      ┌── GET /jobs, /metrics ── operators
                                              v      v
                     +---------------- taskforge serve ----------------+
                     |  Axum API · validation · fingerprint · metrics   |
                     +--------------------------+-----------------------+
                                                | one SQL statement per transition
                                                v
 +-------------------------------------------------------------------------------+
 |  PostgreSQL   jobs (queue, lease, fencing token, result)  ·  job_attempts      |
 |               CHECK constraints + transition trigger refuse illegal states    |
 +------^-----------------------^-------------------------------^----------------+
        | claim: SKIP LOCKED     | heartbeat: own small pool      | reap expired leases,
        | settle: fenced UPDATE  |                                | purge old jobs
 +------+-----------------------+---------+             +-------+----------------+
 | taskforge worker  (x N processes)      |             | taskforge scheduler    |
 | semaphore-bounded slots · handler tasks|             | (optional; every worker|
 | per-job lease timer · built-in reaper  |             |  also reaps)           |
 +----------------------------------------+             +------------------------+
```

- **PostgreSQL is the queue and the only coordinator.** A job's state, lease and
  result live in one row, so every transition is one atomic statement, and
  constraints refuse invalid rows. There is no broker and no leader.
- **Claiming** is one statement with `FOR UPDATE SKIP LOCKED`. Concurrent
  claimers skip each other's locked rows instead of waiting for them or
  double-claiming.
- **Leases, not failure detection.** A claimed job holds a time-limited lease
  that the worker renews every lease/3. Nothing ever decides "worker X is dead".
  A reaper only asks "has this lease expired?", on the database clock.
- **Fencing.** Each claim increments `attempt`. Every settle is an `UPDATE`
  guarded by `(id, state='running', lease_owner, attempt)`, so a worker that lost
  its lease cannot overwrite its successor's result, however late it reports.
- **Self-fencing.** Each job has its own timer. A worker that cannot renew stops
  the handler at 90% of the lease, measured from when the last renewal was
  *sent*: before the database's expiry, the earliest a reaper could hand the job
  to someone else.

More in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Code layout:

```
crates/
  domain/     pure rules: JobState transitions, decide_failure, RetryPolicy,
              validation, request fingerprints, Clock, Jitter      (no I/O)
  storage/    trait JobStore; PgJobStore (all SQL) and MemJobStore;
              migrations; conformance tests run against both
  api/        Axum router, generic over JobStore
  worker/     claim loop, heartbeats, self-fencing, timeouts, handlers;
              all worker timing goes through the LocalTime trait
  scheduler/  lease reaper and purger
  metrics/    Prometheus metrics
  taskforge/  the binary: serve | worker | scheduler | all | migrate
  chaos/      real-process chaos and benchmark harness
```

**Logic separated from I/O.** The rules are pure functions in `domain`. All I/O
sits behind traits, so a deterministic simulation can replace each piece:

| Seam | Production | Test / simulation |
|---|---|---|
| `JobStore` (storage) | `PgJobStore` | `MemJobStore` |
| `Clock` (the store's time) | PostgreSQL `now()` | `ManualClock` |
| `Jitter` (randomness) | `SplitMix64` seeded from entropy | `FixedJitter`, `SplitMix64::seeded` |
| `LocalTime` (worker timers) | `TokioTime` | a timer running at its own rate, to test clock drift |

Job ids can also be supplied (`NewJob::with_id`) instead of generated from the
wall clock, and the worker iterates its jobs in a fixed order. The worker, reaper
and API are generic over `JobStore`. One set of conformance scenarios runs
against both stores, so the SQL and the pure rules cannot drift apart. See
[DECISIONS.md §3.1](DECISIONS.md).

## Job states

```
 queued ──claim──► running ──complete──► succeeded
   ▲  │              ├─ fail (retryable, attempts left) / lease expired ──► queued + backoff
   │  │              ├─ fail (fatal, or attempts used up)              ──► dead ──re-drive──► queued
   │  │              ├─ cancel acknowledged                            ──► cancelled
   │  │              └─ released on graceful shutdown                  ──► queued (no failure)
   │  └── cancel ──► cancelled
```

`pending` and `leased` from the original brief are `queued` and `running`:
claiming and starting are one atomic step. A failed attempt is recorded in
`job_attempts`; the job itself goes back to `queued` with a later `run_at`, or to
`dead`. See [DECISIONS.md §1.4](DECISIONS.md).

## Running it

Needs Rust 1.85+ and Docker.

```bash
docker compose up -d db
```

```bash
cargo run --release -p taskforge -- all
```

That runs the API on `127.0.0.1:8080`, a reaper and one worker, applying
migrations first. Submit a job:

```bash
curl -i -X POST http://127.0.0.1:8080/jobs -H "content-type: application/json" -H "Idempotency-Key: demo-1" -d '{"kind":"sum","payload":{"numbers":[40,2]}}'
```

Then fetch it and its history (use the `id` from the response):

```bash
curl http://127.0.0.1:8080/jobs/<id>
```

```bash
curl http://127.0.0.1:8080/jobs/<id>/attempts
```

Built-in kinds: `echo`, `sum`, `sleep` (`{"ms": 250}`), `fail`. The full API,
including list, cancel, retry and `/metrics`, is in [docs/API.md](docs/API.md).

### Watch a worker die and its job recover

Run the binary directly rather than through `cargo run`, so that killing it kills
the worker and not `cargo`. Build it first (and stop `taskforge all` if it is
still running, since it uses the same port):

```bash
cargo build --release -p taskforge
```

Then, in three terminals, start the API and two named workers with a 3-second
lease. These commands use bash syntax; in PowerShell, set variables with
`$env:TASKFORGE_LEASE_MS="3000"` first. `DATABASE_URL` is not set for you outside
`cargo`, so it is given explicitly:

```bash
DATABASE_URL=postgres://taskforge:taskforge@127.0.0.1:5434/taskforge ./target/release/taskforge serve
```

```bash
DATABASE_URL=postgres://taskforge:taskforge@127.0.0.1:5434/taskforge TASKFORGE_LEASE_MS=3000 TASKFORGE_WORKER_ID=worker-a ./target/release/taskforge worker
```

```bash
DATABASE_URL=postgres://taskforge:taskforge@127.0.0.1:5434/taskforge TASKFORGE_LEASE_MS=3000 TASKFORGE_WORKER_ID=worker-b ./target/release/taskforge worker
```

Submit a 20-second job:

```bash
curl -s -X POST http://127.0.0.1:8080/jobs -H "content-type: application/json" -d '{"kind":"sleep","payload":{"ms":20000}}'
```

`GET /jobs/<id>` shows which worker holds it in `lease_owner`. Kill that worker
the hard way: `kill -9` on its process, or End task in Task Manager. `Ctrl+C`
would be a graceful shutdown, which hands the job back instead of abandoning it.

Within a few seconds (one lease, plus a reaper interval and a short backoff),
`GET /jobs/<id>/attempts` shows attempt 1 as `lease_expired`, and attempt 2
running on the other worker. In two recorded runs, attempt 1 was marked
`lease_expired` 2–3 s after the kill and attempt 2 had started by 3–4 s. The same
thing, automated and checked, is `taskforge-chaos chaos` (below).

### Configuration

Environment variables (see [.env.example](.env.example)):

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | `cargo run` and `cargo test` default to the Compose database (`.cargo/config.toml`) |
| `TASKFORGE_DB_MAX_CONNECTIONS` | `10` | Main pool; workers add a separate 2-connection pool for heartbeats |
| `TASKFORGE_BIND` | `127.0.0.1:8080` | Loopback: there is no authentication |
| `TASKFORGE_QUEUES` | `default` | Comma-separated |
| `TASKFORGE_CONCURRENCY` | `4` | Jobs per worker process at once |
| `TASKFORGE_LEASE_MS` | `30000` | Renewed every lease/3 |
| `TASKFORGE_POLL_INTERVAL_MS` | `250` | Idle polling |
| `TASKFORGE_RETRY_BASE_MS` / `_CAP_MS` | `1000` / `300000` | Backoff |
| `TASKFORGE_WORKER_REAPER` | `true` | Workers also reap expired leases |
| `TASKFORGE_REAP_INTERVAL_MS` | `1000` | |
| `TASKFORGE_RETENTION_SECS` | `604800` | Finished jobs purged after this; `0` keeps them |
| `TASKFORGE_STATEMENT_TIMEOUT_MS` | `5000` | Per SQL statement |
| `TASKFORGE_SHUTDOWN_GRACE_SECS` | `30` | Drain time on `SIGTERM`/`Ctrl+C` |
| `TASKFORGE_METRICS_BIND` | unset | `/metrics` for worker/scheduler processes |
| `TASKFORGE_WORKER_ID` | generated | Must be unique per process |
| `TASKFORGE_MAX_BODY_BYTES` | `66560` | Request body limit (a 64 KiB payload plus the envelope) |
| `TASKFORGE_LOG_FORMAT` | `text` | `json` for structured logs |
| `TASKFORGE_CHAOS_HANDLER` | `false` | Registers the test-only `chaos` job kind; for the harness only |

## Testing

```bash
cargo test --workspace
```

**142 tests**: unit and property tests (state machine, backoff, failure
decisions, fingerprints, config, worker timers), integration tests against a
fresh PostgreSQL database per test, and in-process fault injection (crashed and
partitioned workers, lost leases, cancel races, database outages,
duplicate-submission storms). The database must be running
(`docker compose up -d db`). The CI workflow (`.github/workflows/ci.yml`) runs
formatting, lints and these tests, then a short chaos run with real processes.

Real-process chaos and benchmarks:

```bash
cargo build --release -p taskforge -p taskforge-chaos
```

```bash
./target/release/taskforge-chaos chaos --jobs 1000 --workers 8 --kill-every-ms 500 --scheduler-kill-every-ms 3000
```

## Measured results

From [EXPERIMENTS.md](EXPERIMENTS.md): one Windows laptop, with PostgreSQL in
Docker on the same machine. Every number was measured, from the database's
timestamps or the workers' own metrics; none is estimated. The runs used the
code as of commit `d063ec1`. A later commit (`4ff10dc`) routed worker timing
through `LocalTime` with no intended behaviour change; it passes the tests but
has not been re-measured.

**Chaos: 12 runs, 4 configurations, 3 seeds each.** Worker processes were
hard-killed (`TerminateProcess`, the Windows equivalent of `SIGKILL`) on a timer,
627 orphaned attempts in total:

- **C1–C3:** 1,000 jobs on 8 workers × 2 slots, a worker killed every 500 ms.
  C2 and C3 also kill the scheduler every 3 s; in C3 workers reap too.
- **C4:** 40 long jobs on 48 slots, so nothing waits in a queue.

| | Result |
|---|---|
| Completion rate (healthy jobs) | **100%** in all 12 runs |
| Duplicate committed results | **0** in all 12 runs |
| Lost jobs / stuck leases / open attempts | 0 / 0 / 0 |
| Deliberately failing jobs dead-lettered | 50 of 50 in every C1–C3 run |
| Jobs re-executed (at-least-once) | 50–56 of 950 per C1–C3 run (5–6%) |
| Jobs whose *unprotected* side effect ran twice | 0–6 per run |
| Re-queue after a worker crash (2 s lease; C1, C3) | mean 1.94–1.99 s, p95 ≤ 2.22 s |
| …when the only reaper is also killed (C2) | p95 up to 3.00 s; 2.11 s when workers reap too (C3) |
| Restart after a crash, spare capacity (C4) | mean 2.16–2.53 s, p95 2.71–3.72 s |
| Restart after a crash, saturated queue (C1–C3) | mean 7.2–7.6 s, p95 13–15 s (waits behind the backlog) |

**Throughput.** A preloaded backlog, single-slot worker processes, median of 3
runs:

| Worker processes | No-op jobs/s | 10 ms I/O jobs/s |
|---|---|---|
| 1 | 205 | 32 |
| 4 | 701 | 129 |
| 16 | 1,509 | 513 |

I/O-bound jobs scale linearly (15.9× at 16). No-op jobs show the scheduler's own
cost: one worker completes a job every 4.9 ms (a claim and a settle, each a
database write), and scaling flattens to 7.4× at 16 as claims contend.

**Baseline: claim strategies** (bare table, 5,000 jobs, 16 concurrent claimers):

| Strategy | Claims/s | Double claims (3 runs) |
|---|---|---|
| naive `SELECT` then `UPDATE` | 263 | 76,890 |
| `SELECT ... FOR UPDATE` | 653 | 0 |
| `FOR UPDATE SKIP LOCKED` (TaskForge) | 2,353 | 0 |

## Known limitations

- **At-least-once execution.** Handlers can run more than once; external side
  effects need an idempotency key (the job id) or must be naturally idempotent.
- **Handlers that never yield cannot be stopped.** Timeouts, cancellation and
  self-fencing take effect at the next `.await`. A CPU-bound or blocking handler
  keeps running after losing its lease. Its result is still rejected, but its
  side effects are not prevented.
- **Recovered jobs go to the back of the queue.** Their new `run_at` sorts after
  everything already waiting, so under saturation restart takes far longer than
  recovery (≈7 s against ≈2 s in the experiments).
- **Throughput is bounded by PostgreSQL writes.** Every claim and settle is a
  write. See the throughput table.
- **Polling.** Idle pickup latency is up to `TASKFORGE_POLL_INTERVAL_MS`. There is
  no `LISTEN/NOTIFY` wake-up.
- **No authentication, authorization, TLS or rate limiting.** The API binds to
  loopback by default. Do not expose it as is.
- **One database role for everything.** Workers never migrate, so a DML-only role
  would work, but none is set up.
- **Idempotency keys expire with their job** (7-day retention by default).
- **No workflows, cron schedules or job dependencies.**
- **Self-fencing assumes clocks run at nearly the same rate.** A worker stops
  at 90% of its lease on its own timer, which is safe only while that timer runs
  no more than about 10% slow relative to the database's clock.
- **No deterministic simulation yet.** The seams exist (`JobStore`,
  `MemJobStore`, `Clock`, `Jitter`, `LocalTime`, settable job ids); the harness
  that drives them does not.
- **Measurements come from a single laptop** with the database in a VM on the
  same machine. CPU and memory use were not measured.

## Further reading

- [DECISIONS.md](DECISIONS.md): every significant choice, the alternatives, and
  four approaches that did not work
- [EXPERIMENTS.md](EXPERIMENTS.md): every chaos and benchmark run, with
  configuration and results
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md),
  [docs/API.md](docs/API.md),
  [docs/FAILURE_ANALYSIS.md](docs/FAILURE_ANALYSIS.md),
  [docs/PROJECT_BRIEF.md](docs/PROJECT_BRIEF.md),
  [docs/ADR/](docs/ADR/)
