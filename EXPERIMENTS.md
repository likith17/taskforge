# Experiments

Every number here was measured by `taskforge-chaos` and is reproducible with
the command shown. Raw results, including per-run configuration, are in
[experiments/results/](experiments/results/). Nothing is estimated. Where a
quantity could not be measured, this document says so.

## Environment

| | |
|---|---|
| Machine | Laptop: Intel Core Ultra 9 185H (16 cores / 22 threads), 31.5 GB RAM, Samsung NVMe SSD |
| OS | Windows 11 Home (build 26200) |
| Database | PostgreSQL 16.16 (`postgres:16-alpine`) in Docker Desktop 29.6.1 (WSL2 VM: 22 CPUs, 16 GB). Defaults: `synchronous_commit = on`, `shared_buffers = 128MB` |
| Build | Rust 1.85.0, `--release` (thin LTO) |
| Topology | Everything on one machine: harness, worker processes and the database VM share the CPU |

Consequences for reading the numbers: absolute throughput is pessimistic
compared with a dedicated database host, and the workers compete with
PostgreSQL for the same cores. The numbers are good for comparing
configurations against each other on this machine, not for capacity planning
elsewhere.

## Chaos: workers and the scheduler killed mid-job

### Method

`taskforge-chaos chaos` creates a fresh database and submits N jobs of kind
`chaos`. **Every job is submitted twice, concurrently, with the same idempotency
key.** Each job sleeps a random 20–420 ms, writing an execution ledger row
before and after. Every 20th job fails deliberately and has `max_attempts = 3`.
Healthy jobs have `max_attempts = 25`, so kills alone cannot dead-letter them.

It then starts W worker processes (2 execution slots each) and one
`taskforge scheduler` process, and every `kill-every-ms` kills a random worker
with `Child::kill` (`TerminateProcess` on Windows, `SIGKILL` on Unix): no
cleanup runs. A replacement with a new worker id starts immediately. In C2 and
C3 the scheduler process is also killed every 3 s and restarted 1 s later.

Right after each kill, while the dead process's rows cannot change, the harness
reads which jobs it held and the database clock (`clock_timestamp()`). When every
job is terminal it checks:

| Check | How |
|---|---|
| No duplicate jobs from duplicate submissions | jobs in DB = N, and N submissions were answered as duplicates |
| Every job terminal; no stuck leases; no open attempts | no `queued`/`running` rows, no `lease_owner`, no attempt with `outcome IS NULL` |
| Every healthy job succeeded; every failing job dead-lettered | counts by state and `payload.fail` |
| **Exactly one committed result per job** | no job has two `succeeded` rows in `job_attempts` |
| Every result is the right one | `result = {"n": payload.n}` for every job |
| Every orphan recovered | each orphaned attempt has a next attempt, or was taken back and dead-lettered by the reaper |

It also measures, from the ledger and the attempt history:

- **re-executions**: healthy jobs whose handler *started* more than once
  (at-least-once delivery made visible);
- **unprotected double side effects**: jobs whose handler *finished* its work
  more than once. This is what a non-idempotent side effect would have suffered:
  the worker died after the work but before the result was committed;
- **re-queue latency**: kill → the reaper takes the job back (the orphaned
  attempt's `lease_expired` timestamp). This is the recovery mechanism itself;
- **restart latency**: kill → the job's next attempt starts. This adds the retry
  backoff and any wait behind other queued jobs.

Configuration common to C1–C3: 1,000 jobs, 8 workers × 2 slots, a kill every
500 ms, lease 2 s, reaper interval 250 ms, retry base 250 ms / cap 2 s, poll
50 ms.

### Results

| Run | Kills (worker / sched.) | Orphaned attempts | Succeeded | Dead-lettered | Healthy completion | Duplicate committed results | Re-executed healthy jobs | Unprotected double effects | Stuck leases | Re-queue mean / p95 (s) | Restart mean / p95 (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| C1 seed 1 | 37 / 0 | 64 | 950 | 50 | 100% | **0** | 54 | 3 | 0 | 1.971 / 2.184 | 7.613 / 13.757 |
| C1 seed 2 | 36 / 0 | 64 | 950 | 50 | 100% | **0** | 54 | 2 | 0 | 1.987 / 2.146 | 7.274 / 13.328 |
| C1 seed 3 | 40 / 0 | 65 | 950 | 50 | 100% | **0** | 51 | 2 | 0 | 1.990 / 2.218 | 7.598 / 14.578 |
| C2 seed 1 | 38 / 6 | 64 | 950 | 50 | 100% | **0** | 52 | 4 | 0 | 2.130 / 2.756 | 7.302 / 13.298 |
| C2 seed 2 | 38 / 6 | 65 | 950 | 50 | 100% | **0** | 54 | 2 | 0 | 2.184 / 2.921 | 7.204 / 13.322 |
| C2 seed 3 | 37 / 6 | 66 | 950 | 50 | 100% | **0** | 56 | 3 | 0 | 2.213 / 2.998 | 7.521 / 13.718 |
| C3 seed 1 | 36 / 6 | 65 | 950 | 50 | 100% | **0** | 56 | 0 | 0 | 1.938 / 2.108 | 7.387 / 13.707 |
| C3 seed 2 | 35 / 6 | 62 | 950 | 50 | 100% | **0** | 50 | 4 | 0 | 1.935 / 2.106 | 7.269 / 13.195 |
| C3 seed 3 | 40 / 7 | 64 | 950 | 50 | 100% | **0** | 52 | 6 | 0 | 1.940 / 2.112 | 7.387 / 13.540 |
| C4 seed 1 | 23 / 0 | 14 | 40 | 0 | 100% | **0** | 7 | 0 | 0 | 1.822 / 1.995 | 2.163 / 2.707 |
| C4 seed 2 | 33 / 0 | 21 | 40 | 0 | 100% | **0** | 9 | 0 | 0 | 1.905 / 2.195 | 2.528 / 3.722 |
| C4 seed 3 | 25 / 0 | 13 | 40 | 0 | 100% | **0** | 7 | 1 | 0 | 1.915 / 2.108 | 2.314 / 3.076 |

- **C1**: worker kills only; the scheduler is never killed and is the only reaper.
- **C2**: also kills the scheduler every 3 s, and it stays down 1 s. It is still
  the only reaper.
- **C3**: as C2, but every worker also runs a reaper (the default configuration).
- **C4**: spare capacity: 40 jobs of 20–4,020 ms on 8 workers × 6 slots (48 slots),
  so no job waits behind another. A kill every 700 ms, no deliberately failing jobs.

**All 12 runs passed every check.** Across 627 orphaned attempts, no job was lost
or stuck, and no job had more than one committed result.

### What the results say

1. **Completion is 100% and duplicate committed results are 0 in every run.**
   Lost jobs: 0. Stuck leases: 0. Open attempts: 0. Every duplicate submission
   was answered with the original job (jobs in database = 1,000 every time).
2. **Re-executions are not zero, and that is the honest number.** In C1–C3,
   50–56 of 950 healthy jobs (5–6%) started more than once. In 0–6 jobs per
   run, the handler had finished its work but the worker died before the
   result was committed, so an unprotected side effect would have happened twice.
   That is at-least-once delivery. The committed result was still unique each
   time. Handlers with external side effects must use the job id as an
   idempotency key (DECISIONS 1.1).
3. **Recovery itself takes about one lease.** Re-queue latency has a mean of
   1.94–1.99 s and p95 ≤ 2.22 s with a 2 s lease and a 250 ms reaper interval
   (C1, C3). It can be a little under 2 s because the lease runs from the
   worker's last renewal, which happened before the kill.
4. **A dead scheduler delays recovery by its downtime, unless workers reap
   too.** With the scheduler as the only reaper and down 1 s in every 3 (C2),
   re-queue p95 rises to 2.76–3.00 s. With reapers in every worker (C3), it is
   back to 2.11 s although the scheduler dies just as often. That is the reason
   workers reap by default (DECISIONS 2.8).
5. **"Kill → running again" is dominated by queueing when the system is
   saturated.** In C1–C3 restart latency has a mean of about 7.4 s and p95 of
   13–15 s. That is not slow recovery: a recovered job's new `run_at` is later
   than everything already queued, so it waits for the backlog (1,000 jobs on
   16 slots) to drain. With spare capacity (C4) restart latency has a mean of
   2.16–2.53 s and p95 of 2.7–3.7 s: re-queue plus the backoff (125–250 ms for a
   first failure) plus a poll interval. Re-queued jobs going to the back of the
   line is a known limitation (README).

Reproduce (needs `docker compose up -d db` and
`cargo build --release -p taskforge -p taskforge-chaos`):

```bash
./target/release/taskforge-chaos chaos --name c1 --jobs 1000 --workers 8 --concurrency 2 --kill-every-ms 500 --seed 1
```

```bash
./target/release/taskforge-chaos chaos --name c2 --jobs 1000 --workers 8 --concurrency 2 --kill-every-ms 500 --scheduler-kill-every-ms 3000 --scheduler-down-ms 1000 --seed 1
```

```bash
./target/release/taskforge-chaos chaos --name c3 --jobs 1000 --workers 8 --concurrency 2 --kill-every-ms 500 --scheduler-kill-every-ms 3000 --scheduler-down-ms 1000 --worker-reaper true --seed 1
```

```bash
./target/release/taskforge-chaos chaos --name c4 --jobs 40 --workers 8 --concurrency 6 --work-ms-max 4000 --kill-every-ms 700 --fail-every 0 --seed 1
```

The seed fixes job durations and which worker is killed. Kill *timing* relative
to the jobs still varies between runs, so runs with the same seed are similar
but not identical.

## Throughput: 1, 4 and 16 worker processes

### Method

`taskforge-chaos bench` creates a fresh database and loads a backlog of jobs
*before* any worker starts. It then starts W `taskforge worker` processes, each
with **one execution slot** (`TASKFORGE_CONCURRENCY=1`), so W is the number of
jobs in flight, and waits until every job has succeeded. Each configuration was
run 3 times on a fresh database.

- **Throughput** = jobs / (last `finished_at` − first `started_at`), both on the
  database clock, so worker start-up is excluded.
- **Claim latency** = mean of the workers' own `taskforge_claim_duration_seconds`
  histogram (sum / count), scraped from each worker's `/metrics`. This is the
  round trip of one claim statement as the worker sees it.
- **Job held** = `finished_at − started_at` per job: handler run time plus the
  settle statement, i.e. how long a job occupies a slot.

Workloads: `noop` (the `echo` handler, which returns at once: pure scheduler
overhead) and `io` (the `sleep` handler, 10 ms: an I/O-bound job).

### Results

| Workload | Workers | Throughput, jobs/s: median (min–max of 3) | Scaling vs 1 worker | Claim latency mean (ms, per run) | Job held p50 / p95 (ms, run 1) |
|---|---|---|---|---|---|
| noop | 1 | **205** (164–209) | 1.0× | 3.6, 2.8, 2.9 | 3 / 4 |
| noop | 4 | **701** (677–723) | 3.4× | 3.3, 3.2, 3.5 | 3 / 4 |
| noop | 16 | **1,509** (1,471–1,518) | 7.4× | 6.1, 6.1, 6.3 | 6 / 8 |
| io 10 ms | 1 | **32** (30–32) | 1.0× | 3.3, 4.0, 3.0 | 28 / 29 |
| io 10 ms | 4 | **129** (129–130) | 4.0× | 3.3, 3.3, 3.3 | 28 / 29 |
| io 10 ms | 16 | **513** (512–513) | 15.9× | 4.2, 4.9, 4.1 | 28 / 29 |

Sample size: 3,000 jobs per noop run and 2,000 per io run, 3 runs each.
Every job succeeded in every run.

### What the results say

1. **For I/O-bound jobs, throughput scales linearly with workers** (4.0× at 4,
   15.9× at 16). The database is not the bottleneck at this rate; the handlers
   are.
2. **For no-op jobs the scheduler's own cost shows.** One worker completes a
   job every 4.9 ms (1/205 s). The claim round trip alone averages about 3 ms as
   the worker sees it; the rest is the `complete` statement. (The "job held"
   interval starts at the claim's database timestamp, so it overlaps the claim
   round trip and the two do not simply add up.)
   Scaling flattens to 7.4× at 16 workers as claims contend: claim latency
   doubles from ~3 ms to ~6 ms. On this machine the workers, the harness and
   PostgreSQL share the same CPU, so the knee is partly the laptop's.
3. **Per-job overhead is 3–4 ms at p50/p95** with 1–4 workers, rising to 6–8 ms
   at 16 under contention. This includes a synchronous-commit write to disk for
   every settle.

### Anomalies, reported as measured

- **The first noop run with 1 worker was slower** (164 jobs/s against 209 and
  205). It was the first run after the database container started. I did not
  investigate further; the median ignores it.
- **The 10 ms sleep holds a slot for 28 ms.** `sleep` asks Tokio for 10 ms. On
  Windows, timers have a coarse default resolution (about 15.6 ms), which
  probably explains most of the gap, but I did not verify it. This also means
  `io` throughput here understates what a 10 ms job would achieve on Linux.

Reproduce:

```bash
./target/release/taskforge-chaos bench --name b1 --workload noop --jobs 3000 --workers-list 1,4,16 --repeats 3
```

```bash
./target/release/taskforge-chaos bench --name b2 --workload io --io-ms 10 --jobs 2000 --workers-list 1,4,16 --repeats 3
```

## Baseline: claim strategies compared

### Method

`taskforge-chaos claims` isolates the claim itself, on a bare table
`q(id, state)` holding 5,000 rows, with no TaskForge code involved. C concurrent
claimers (one connection each) loop "take the next queued row, mark it done,
record the claim in a `claims` table" until the queue is empty. Three
strategies:

| Strategy | Statements per claim |
|---|---|
| `naive` | `SELECT id ... WHERE state='queued' ORDER BY id LIMIT 1`, then `UPDATE ... SET state='done' WHERE id=$1` (no lock) |
| `for_update` | the same inside a transaction, with `SELECT ... FOR UPDATE` |
| `skip_locked` | one statement: `WITH next AS (SELECT ... FOR UPDATE SKIP LOCKED) UPDATE ... RETURNING` (what TaskForge does) |

Double claims = rows in `claims` − distinct ids, counted exactly. 3 runs per
cell, fresh database each.

### Results

| Strategy | Claimers | Claims/s: median (min–max) | Double claims (3 runs) |
|---|---|---|---|
| naive | 1 | 209 (190–216) | 0 |
| naive | 4 | 351 (344–353) | **15,702** |
| naive | 16 | 263 (60–265) | **76,890** |
| for_update | 1 | 214 (188–218) | 0 |
| for_update | 4 | 691 (690–695) | 0 |
| for_update | 16 | 653 (647–663) | 0 |
| skip_locked | 1 | 291 (273–292) | 0 |
| skip_locked | 4 | 1,017 (994–1,027) | 0 |
| skip_locked | 16 | **2,353** (2,317–2,395) | 0 |

### What the results say

1. **The naive claim is wrong, not just slow.** With 4 claimers each job was
   claimed about twice (5,193–5,282 double claims per 5,000 jobs); with 16,
   about six times (25,586–25,702 per 5,000). In a job system, every double
   claim is a duplicate execution.
2. **`FOR UPDATE` is correct but stops scaling.** Every claimer queues behind the
   lock on the same "next" row: 691 claims/s with 4 claimers, 653 with 16.
3. **`SKIP LOCKED` keeps scaling.** Claimers take different rows instead of
   waiting: 2,353 claims/s with 16 claimers, **3.6× `FOR UPDATE`** and with zero
   double claims. With 1 claimer it is also faster (291 against about 210),
   because it is one statement instead of two.
4. **Anomaly:** one naive run with 16 claimers took 83 s (60 claims/s) instead of
   about 19 s. I did not determine why; the median excludes it.

Reproduce:

```bash
./target/release/taskforge-chaos claims --name b3 --jobs 5000 --claimers-list 1,4,16 --repeats 3
```

## Not measured

- **CPU and memory use** of workers and PostgreSQL. The harness does not sample
  them.
- **PostgreSQL query latency** from the server side (`pg_stat_statements` is not
  enabled in the Compose image). Claim latency is measured client-side only.
- **Delayed-job scheduling accuracy** (actual start − `run_at`) and
  **deliberately failing workloads at scale** as separate benchmarks. Failing
  jobs are part of every chaos run (50 per run, all dead-lettered), but their
  throughput cost was not measured on its own.
- **Anything on a dedicated database host or on Linux.** All numbers are from
  one Windows laptop.
