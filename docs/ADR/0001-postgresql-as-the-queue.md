# ADR 0001 — PostgreSQL as the queue (no Redis)

Status: accepted · Milestone 1

## Context

Jobs need a durable home, and workers need a way to take one job each without
colliding. The two usual answers are a Redis-backed queue (Sidekiq, Celery's
common setup) and a relational table used as a queue.

## Options

| | PostgreSQL table | Redis lists/streams |
|---|---|---|
| Durability | Synchronous WAL by default; an acknowledged write survives a crash. | Depends on AOF settings; `everysec` can lose the last second. |
| Atomic claim | `FOR UPDATE SKIP LOCKED` in one statement. | `BRPOPLPUSH`/Lua script, or stream consumer groups. |
| Job state and queue | Same row, same transaction: cannot disagree. | Queue in Redis, status usually elsewhere: two systems to keep consistent. |
| Rich queries (list, filter, dashboard) | SQL. | Needs secondary structures maintained by hand. |
| Constraints as a safety net | CHECKs, unique indexes, triggers. | None; all invariants live in application code. |
| Raw throughput | Thousands of jobs/s per node is realistic; every claim is a write. | Higher; in-memory. |
| Operations | One system. | A second system to run, monitor and back up. |

## Decision

Use a PostgreSQL table as the queue. Do not use Redis.

The project's priority is correctness under failure. Keeping a job's existence,
status, lease and result in one row means every transition is a single atomic
statement, and the database can refuse invalid states on its own. With Redis,
the same guarantees require coordinating two stores, which is where queue
implementations usually lose or duplicate work.

## Consequences

- Throughput is bounded by PostgreSQL write capacity. That limit will be
  *measured* (EXPERIMENTS.md), not assumed.
- Finished rows accumulate; a retention job is needed (planned).
- High churn on one table means autovacuum tuning matters at scale.
- Redis stays a possible later addition for one specific job — wake-up
  notifications or rate limiting — if measurements show a need.
