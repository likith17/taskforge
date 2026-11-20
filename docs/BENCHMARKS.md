# Benchmarks

**No performance results exist yet.** This document defines how they will be
produced so that the numbers, when they arrive in Milestone 3, are
reproducible and comparable. Nothing here should be read as a claim about how
fast TaskForge is.

The only measurement taken so far is test-suite wall time (see
[FAILURE_ANALYSIS.md §3.1](FAILURE_ANALYSIS.md)), which is not a performance
benchmark.

## Questions the benchmarks must answer

1. How many jobs per second does one worker process, and how does that scale
   with worker count? Where does it stop scaling, and why?
2. How long does a job wait in the queue (P50/P95/P99) at a given arrival rate?
3. What does TaskForge add to a job's own run time (claim + settle overhead)?
4. How long after a worker is killed does its job start running elsewhere?
5. What do the safety mechanisms cost (transition trigger, fencing guard)?

## Baseline

A deliberately simple implementation sharing the same schema, handlers and
harness, so that only the claiming strategy differs:

- **Baseline A — blocking claim:** `SELECT … FOR UPDATE` without `SKIP LOCKED`,
  then `UPDATE`, in one transaction. Correct, but claimers queue behind each
  other.
- **Baseline B — unsafe claim:** `SELECT` then `UPDATE` with no locking. Run
  only to *count* the double-claims it produces; its throughput is not a
  meaningful comparison because it does duplicate work.

## Workloads

| Name | Handler | Purpose |
|---|---|---|
| `noop` | `echo` with a small payload | Pure system overhead; upper bound on throughput. |
| `cpu` | a short CPU-bound computation (to be added) | Contention between handlers and the runtime. |
| `io` | `sleep` 10–50 ms | I/O-bound work; shows the benefit of concurrency. |
| `delayed` | `echo` with `delay_ms` spread over a window | Scheduling accuracy: actual start − `run_at`. |
| `failing` | `fail` at a fixed rate | Cost of the retry/dead-letter path. |

## Metrics and how each is obtained

| Metric | Source |
|---|---|
| Throughput (jobs/s) | Terminal-state row count over the measurement window. |
| Queue wait | `started_at − max(run_at, created_at)` per job, from the table. |
| Execution overhead | `(finished_at − started_at) − handler time`; handler time reported in the result. |
| Recovery time | Kill timestamp to the next `started_at` of the affected job. |
| Query latency | `pg_stat_statements` mean/percentiles for claim, complete, insert. |
| CPU / memory | Process sampling of worker, API and PostgreSQL container at 1 Hz. |
| Duplicate execution attempts | Handler writes one ledger row per run; count runs > 1 per job. |
| Invalid transitions | Count of trigger rejections in the PostgreSQL log (expected: 0). |

Percentiles are computed from all per-job samples in the window, not from
averages of averages.

## Method

1. Fresh database and migrated schema per run; PostgreSQL restarted between
   configurations.
2. Pre-load or open-loop submission at a fixed arrival rate, depending on the
   question (pre-load for throughput, open-loop for wait time, so that slow
   processing does not slow the arrival rate and hide queueing).
3. Warm-up period discarded; fixed measurement window.
4. Each configuration repeated at least 5 times; report median and range.
5. Worker counts 1, 2, 4, 8, 16; release build (`--release`).
6. Micro-benchmarks (Criterion) only for pure functions where it makes sense,
   e.g. backoff computation and payload validation.

## What every report will state

- Hardware (CPU model, cores, RAM, disk type), OS, and that PostgreSQL runs in
  Docker Desktop on the same machine — which makes absolute numbers
  pessimistic and unrepresentative of a dedicated database host.
- PostgreSQL version and any non-default settings (`synchronous_commit`
  in particular).
- Rust version, build profile, commit hash.
- Workload definition, job count, arrival rate, measurement window.
- Sample size and number of repetitions.
- Known limitations of the measurement.

## Results

_None yet._
