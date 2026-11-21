# Design decisions

Each entry gives the decision, the alternatives considered, and why. The four
longest have a fuller record in [docs/ADR/](docs/ADR/). Section 4 lists
approaches that did not work, because the reasons are more useful than the fixes.

## 1. Guarantees

### 1.1 At-least-once execution, exactly-once results

**Decision.** A job's handler may run more than once. At most one result is ever
committed, and an accepted job is never lost.

**Why not "never runs a job twice".** A worker can finish the work and die
before recording that it did. From the outside that is indistinguishable from a
worker that died before starting. The system then has two choices: run the job
again (at-least-once), or never run it again and risk losing it (at-most-once).
No protocol avoids both; it only chooses which one to give up. TaskForge gives
up "runs once" to keep "never lost".

**What is guaranteed instead, and how:**

| Guarantee | Mechanism |
|---|---|
| An accepted job is durable | The row is committed before `201` is returned |
| A job is leased to one attempt at a time | `FOR UPDATE SKIP LOCKED` claim; `CHECK` ties the lease to `running` |
| At most one committed result per job | Fencing token on every settle (1.3); `succeeded` is final (trigger) |
| A retried submission creates one job | Idempotency key + unique index (2.5) |
| Abandoned jobs are recovered | Lease expiry + reaper (1.2) |
| Two attempts never *run* at the same time | Worker self-fences before its lease can expire (2.3) |

**Duplicate submission vs duplicate side effect.** These are different
problems. Duplicate submissions are prevented by the platform (idempotency keys).
Duplicate side effects (a handler charging a card twice) cannot be prevented by
the platform alone. Handlers get a stable `JobContext::job_id` to pass
downstream as an idempotency key, or must be naturally idempotent. The chaos
harness measures both: re-executions happen (that is the at-least-once part),
and committed results are never duplicated.

### 1.2 Leases, not failure detection

**Decision.** Recovery is driven only by lease expiry. Nothing ever decides
"worker X is dead".

**Alternatives.** Worker registry with heartbeats and a timeout (a failure
detector); a coordination service (etcd/ZooKeeper sessions).

**Why.** "Is this worker alive?" has no reliable answer in an asynchronous
network: a slow worker and a dead one look the same. "Has this lease expired?"
has an exact answer on one clock, the database's. This is a visibility timeout,
as in SQS, plus renewal. A worker that was only slow is handled by fencing
(1.3), not by guessing.

### 1.3 The attempt number is the fencing token

**Decision.** Every claim increments `attempt`. Every settle is
`UPDATE ... WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND attempt = $3`.

**Why.** After a lease expires and the job is re-claimed, the old worker may
still wake up and report. Without fencing it would overwrite the new attempt's
result. With fencing its `UPDATE` matches no row and it gets `StaleClaim`. The
token only moves forward (enforced by trigger). The `Claim` type has
crate-private fields, so code outside the storage crate can receive a claim but
never construct one.

### 1.4 Job states

**Decision.** Five states: `queued`, `running`, `succeeded`, `dead`,
`cancelled`. The requested six map onto them like this:

| Requested | TaskForge | Why not a separate state |
|---|---|---|
| pending | `queued` | — |
| leased | `running` | Claiming and starting are one atomic statement; there is no moment when a job is leased but not running. A separate state would add a transition with nothing in it. |
| running | `running` | — |
| succeeded | `succeeded` | — |
| failed | `queued` with `failures > 0`, plus a `failed` row in `job_attempts` | A failed *attempt* is an event, not a resting place: the job is either waiting to retry (`queued` with a future `run_at`) or finished (`dead`). The per-attempt history keeps every failure and its error. |
| dead | `dead` | — |

`cancelled` was added for cancellation. Legal transitions:
`queued→running|cancelled`, `running→succeeded|queued|dead|cancelled`,
`dead→queued` (operator re-drive). Nothing else.

### 1.5 The state machine is enforced twice

**Decision.** `JobState::can_transition_to` in the domain crate, *and* a
PostgreSQL trigger that rejects any other transition, any non-`queued` insert
and any decrease of the fencing token. A test checks all 20 state pairs and
asserts the two agree. [ADR 0004](docs/ADR/0004-state-machine-enforced-twice.md).

**Why.** The bug being guarded against is in application code: a future query
missing its guard, or a hand-written `UPDATE` during an incident. The database
refuses it even then.

## 2. Mechanisms

### 2.1 PostgreSQL is the queue (no Redis)

**Alternatives.** Redis lists/streams (Sidekiq, Celery); a message broker
(RabbitMQ, Kafka).

**Why.** A job's existence, state, lease and result live in one row, so every
transition is one atomic statement and constraints can refuse invalid rows.
With Redis the same guarantees need two stores kept consistent, which is where
queues typically lose or duplicate work. The cost is throughput, measured in
[EXPERIMENTS.md](EXPERIMENTS.md). [ADR 0001](docs/ADR/0001-postgresql-as-the-queue.md).

### 2.2 Claiming with `FOR UPDATE SKIP LOCKED`

| Option | Correct? | Under concurrency |
|---|---|---|
| `SELECT` then `UPDATE`, no lock | **No**: two workers read the same row and both run it | — |
| `SELECT ... FOR UPDATE` | Yes | Every claimer waits behind the first one's lock on the same "next" row |
| Optimistic `UPDATE ... WHERE state = 'queued'`, retry on 0 rows | Yes | Claimers collide on the same row and retry; wasted round trips grow with workers |
| Advisory locks | Yes | Locks live outside the row; easy to leak or forget |
| **`FOR UPDATE SKIP LOCKED`** | Yes | Each claimer skips rows others have locked and takes the next one: no waiting, no collisions |

The claim is one statement (CTE): choose, lock, mark running, bump the token,
start the lease, open the attempt record. A partial index on `queued` rows keeps
it cheap however many finished jobs accumulate.

### 2.3 Workers fence themselves before the lease can expire

**Decision.** Each job has its own timer. When 90% of the lease has passed since
the last successful renewal was *sent*, the handler is aborted.

**Why "sent".** The local deadline is measured from before the request left, so
it falls before the moment the database wrote the lease. The database's expiry
is the earliest a reaper can hand the job to someone else, so the old worker has
already stopped by then. The 10% margin covers clock-rate drift between machines
and the time an abort takes to land.

**What it replaced.** The deadline used to be checked on each heartbeat tick, so
a worker could notice up to a third of a lease late, leaving a window in which
two workers ran the same job. Test:
`a_partitioned_worker_stops_before_its_lease_can_expire`.

**Limit.** Abort takes effect at the handler's next `.await`. A CPU-bound or
blocking handler cannot be stopped from outside (see README, Known limitations).

### 2.4 Heartbeats get their own connection pool

**Decision.** In the binary, lease renewals use a separate 2-connection pool.

**Why.** If claims and settles exhaust the main pool, a renewal waiting for a
connection lets a healthy job's lease expire, and the job runs twice. A
dedicated pool cannot be starved by the other traffic.

### 2.5 Idempotent submission with a request fingerprint

**Decision.** `Idempotency-Key` header, unique partial index,
`INSERT ... ON CONFLICT DO NOTHING`. A repeated key returns the original job
(`200`, `Idempotent-Replay: true`). The same key with a different request is
`409`. "Different" is decided by comparing SHA-256 fingerprints of the canonical
(key-sorted) request, stored at insert.

**Alternatives.** Compare the request against the stored row (the first
implementation; see 4.2); no conflict check at all (silently returns someone
else's job when a client reuses a key by mistake).

**Consequence.** Keys are honoured until the job is purged (7 days by default).
After that the same key creates a new job.

### 2.6 Retries: exponential backoff with equal jitter

**Decision.** After the n-th failure, delay = `c/2 + U(0,1)·c/2` with
`c = min(cap, base·2^(n−1))`. Defaults: base 1 s, cap 300 s, `max_attempts` 5
(per job, 1–25).

**Alternatives.** Full jitter (`U(0,1)·c`), which spreads load best but can
retry almost instantly. No jitter, which makes jobs that failed together come
back together.

**Why equal jitter.** It keeps a guaranteed minimum pause before hitting a
failing dependency again, while still spreading retries out. Property tests
check the bounds and monotonicity.

### 2.7 A lost lease counts as a failure

**Decision.** When the reaper takes a job back, it counts as a failed attempt:
backoff applies, and `max_attempts` can dead-letter it.

**Why.** A job that kills its worker every time (out-of-memory, a crash in a
native library) must not cycle forever through the fleet. **Trade-off:** the
backoff adds 0.5–1 × base to recovery time. Measured in EXPERIMENTS.md.

### 2.8 Every worker process also reaps

**Decision.** `taskforge worker` runs a reaper by default
(`TASKFORGE_WORKER_REAPER`). `taskforge scheduler` runs one on its own.

**Alternatives.** A single dedicated scheduler with leader election.

**Why.** The reaper keeps no state, and `SKIP LOCKED` makes any number of them
safe: each expired lease is locked by exactly one. Running one per worker means
recovery cannot depend on a separate process being up, and there is no leader to
elect. **Cost:** one cheap indexed query per worker per second. Experiment C3
measures the difference.

### 2.9 Polling, not LISTEN/NOTIFY

**Decision.** Idle workers poll (250 ms default). A worker that just claimed
something looks again immediately. [ADR 0002](docs/ADR/0002-polling-vs-notifications.md).

**Why.** Notifications are not durable, so polling has to exist as a fallback
anyway, and they do nothing for delayed jobs. **Cost:** up to one poll interval
of pickup latency on an idle system.

### 2.10 One binary, several roles; workers talk to PostgreSQL

**Decision.** `taskforge serve | worker | scheduler | all | migrate`. Workers
use the database directly. [ADR 0003](docs/ADR/0003-one-binary-several-roles.md).

**Alternative.** Workers go through the HTTP API (the Python prototype). That
adds a network hop and a second failure mode to every claim, heartbeat and
settle, and every worker endpoint then has to be idempotent against lost
responses.

### 2.11 Every statement has a timeout

**Decision.** `statement_timeout` (5 s default) on every connection; SQLSTATE
`57014` is treated as "unavailable, retry".

**Why.** A statement stuck on a lock or an overloaded server could otherwise
hold a worker past its lease. With a timeout the statement fails, the worker sees
an error it can retry, and the lease logic stays in charge.

### 2.12 Finished jobs are purged

**Decision.** The reaper deletes finished jobs older than the retention period
(7 days), in batches, with their history.

**Why.** Without it the table grows forever, and every idempotency key is kept
forever. The trade-off is in 2.5.

## 3. Structure

### 3.1 Scheduling logic separated from I/O

**Decision.** Three seams, so a deterministic simulation can replace all I/O:

| Seam | Production | Test / simulation |
|---|---|---|
| `JobStore` trait (storage) | `PgJobStore` | `MemJobStore` |
| `Clock` trait (time) | database `now()` inside `PgJobStore` | `ManualClock` in `MemJobStore` |
| `Jitter` trait (randomness) | `SplitMix64::from_entropy()` | `FixedJitter`, `SplitMix64::seeded(n)` |

The rules themselves are pure functions in the domain crate: `decide_failure`,
`RetryPolicy::delay` and `JobState::can_transition_to`. The worker, reaper and
API are generic over `S: JobStore`. An API test runs the real router on
`MemJobStore` with no database.

**Why the PostgreSQL store does not call `decide_failure`.** Doing so would mean
read-lock-decide-write (`SELECT FOR UPDATE`, decide in Rust, `UPDATE`): two round
trips while holding a row lock. Instead the store expresses the same rule as a
SQL `CASE`, so each transition stays one statement. The risk is that the two
copies drift apart. The **conformance tests** close that gap: 8 scenarios run
against both stores with a fixed jitter and must produce identical states,
counters, histories and delays (to the microsecond PostgreSQL stores).

**Why the PostgreSQL store does not take a `Clock`.** Its deadlines are compared
with the database clock on purpose: a single clock means no clock-skew bugs
between machines. Passing `now` in from each process would bring skew back.
Worker *loops* use Tokio's timer, which tests can already pause and advance
(`start_paused`), so a separate clock trait there would add indirection without
new capability. A full simulation (madsim-style) would swap the runtime instead.

**Why `impl Future + Send` rather than `async fn` or `async-trait`.** Generic
code spawns store futures onto Tokio, which needs them to be `Send`; an `async
fn` in a trait cannot promise that. Writing `impl Future<Output = _> + Send`
says so in the signature, and avoids the `async-trait` crate (boxing on every
call). The cost is that the trait cannot be used as `dyn JobStore`, which nothing
needs.

### 3.2 Crates

| Crate | Responsibility |
|---|---|
| `domain` | Pure rules: states, transitions, validation, retry policy, decisions, Clock, Jitter. No I/O. |
| `storage` | `JobStore` trait, `PgJobStore` (every SQL statement), `MemJobStore`, migrations, conformance tests |
| `api` | Axum router, generic over `JobStore` |
| `worker` | Handler registry, worker loop, heartbeats, self-fencing |
| `scheduler` | Reaper and purger |
| `metrics` | Prometheus registry and metric definitions |
| `taskforge` | The binary: configuration, roles, the opt-in `chaos` job kind |
| `chaos` | Real-process chaos and benchmark harness |

## 4. Approaches that did not work

### 4.1 `localhost` in the database URL: a 200× slower test suite

The storage tests took 188 s instead of under 1 s. Each test opens fresh
connections; on Windows `localhost` resolves to IPv6 `::1` first, nothing listens
there (the container is published on IPv4 loopback), and each connection waited
about 2 s before falling back. Fixed by using `127.0.0.1`. The same delay would
hit every new pool connection in production on an affected host, exactly when a
pool is being rebuilt after an outage.

### 4.2 Detecting idempotency conflicts by comparing rows, and a test that proved nothing

The first implementation compared a repeated request with the stored job's
fields. The audit flagged that `jsonb` normalises numbers, and the first
regression test used the payload `{"n": 1e2}`.

**That test passed against the buggy code.** serde writes the float `1e2` as
`100.0`, and `jsonb` returns `100.0` unchanged, so nothing broke. Checking
directly against PostgreSQL showed the real trigger: the float `1e16`, which serde
writes as `1e16`. `jsonb` returns it as `10000000000000000`, serde reads that back
as an *integer*, and the integer is not equal to the float. The test was changed
to `1e16`, confirmed to fail with the old comparison, and passes with
fingerprints.

The lesson worth keeping: run a regression test against the old code before
trusting it.

### 4.3 Heartbeat interval from an assumed lease (Python prototype)

The prototype's worker assumed a 30 s lease until the server said otherwise, and
slept 10 s before its first heartbeat. Against a 0.6 s server-side lease, healthy
jobs were reaped, re-run and finally dead-lettered. In TaskForge the lease has a
single owner, the worker's configuration, and every interval derives from it.

### 4.4 Chaos harness "duplicates" that were not duplicates

The first chaos run aborted with `idempotency key "job-0" was already used for a
different request`. The harness drew a fresh random `work_ms` for the repeated
submission, so the second request really was different, and TaskForge was right
to refuse it. The harness now builds each request once and sends identical bytes
twice. This is the kind of client bug the conflict check exists to catch.

## 5. Dependencies

| Crate | Why it is needed |
|---|---|
| `tokio` | Async runtime |
| `axum` | HTTP API |
| `sqlx` (postgres, runtime-checked queries) | Database driver, pool, migrations, per-test databases |
| `serde`, `serde_json` | Wire format, payloads |
| `uuid` (v7) | Time-ordered job ids |
| `chrono` (no default features) | Timestamps with SQLx/Serde support |
| `thiserror`, `anyhow` | Typed errors in libraries; context in binaries |
| `tracing`, `tracing-subscriber` | Structured (optionally JSON) logs |
| `sha2` | Request fingerprints. Already in the tree via `sqlx-postgres`, so no new code is compiled |
| `prometheus-client` | Metrics exposition. The Prometheus project's own client; no HTTP server of its own |
| `proptest` (dev) | Property tests for backoff and decisions |
| `tower`, `http-body-util` (dev) | Driving the router in tests |

Deliberately not added: `rand` (SplitMix64 is ~15 lines, checked against its
reference output), `async-trait` (see 3.1), a CLI parser, an HTTP client for the
harness (one hand-written `GET /metrics`), `dotenv`.

## 6. Smaller choices

| Choice | Reason |
|---|---|
| UUIDv7 ids generated by the API | Time-ordered (index-friendly, stable tiebreak, keyset pagination) and known before the insert |
| `state` as `text` + `CHECK`, not an enum | Adding a state is an ordinary transactional migration |
| Runtime-checked SQL (`query_as`) | Compile-time macros need a live database or a committed cache at build time; every statement is exercised by integration tests instead |
| `#[sqlx::test]` | A fresh migrated database per test: isolated and parallel |
| `JobSubmission` rejects unknown fields | A client relying on an option this version lacks gets a `422`, not silence |
| Handlers run in their own task | A panic fails the job, not the worker |
| All deadlines on the database clock | No clock-skew failure mode between machines |
| Rust 1.85 with `rust-version` set | The toolchain installed here; the MSRV-aware resolver keeps dependencies compatible |
