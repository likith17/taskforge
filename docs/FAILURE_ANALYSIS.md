# Failure analysis

What can go wrong, what TaskForge does about it today, and what it does not
do yet. "Tested" means an automated test injects the failure.

## 1. Failure modes at Milestone 1

| # | Failure | Behaviour today | Tested | Fix |
|---|---|---|---|---|
| F1 | Two workers claim at the same instant | Each gets a different job (`SKIP LOCKED`). | yes — `concurrent_claimers_never_receive_the_same_job` (12 claimers, 60 jobs) | — |
| F2 | Process killed while jobs are queued | Jobs are rows; they are still there on restart and run. | manually — smoke run in README; automated in M2 | automate |
| F3 | A worker reports under a claim it no longer holds | Rejected with `StaleClaim`; nothing written. | yes — `stale_or_foreign_claims_cannot_settle_a_job`, `losing_the_claim_mid_run_discards_the_outcome` | — |
| F4 | A late failure report after success (or the reverse) | Rejected; the first committed outcome stands. | yes — `a_settled_job_cannot_be_settled_again` | — |
| F5 | Application bug attempts an illegal transition | Database trigger refuses it. | yes — all 20 state pairs | — |
| F6 | Handler returns an error | Job is dead-lettered with the message. **No retry.** | yes | M2: retries with backoff |
| F7 | Handler panics | Job dead-lettered as "handler panicked"; worker continues. | yes | — |
| F8 | Job kind has no handler | Rejected at submit (`422`); if it reaches the queue anyway, dead-lettered unrun. | yes (both paths) | — |
| F9 | Database unreachable — API | `503` + `Retry-After`; `/readyz` fails, `/healthz` stays up. | yes — `health_endpoints` | — |
| F10 | Database unreachable — idle worker | Worker logs, backs off exponentially (cap 10 s), recovers by itself. | yes — `worker_survives_a_database_outage` | — |
| **F11** | **Worker dies while running a job** | **Job stays `running` forever.** Nothing recovers it. | no | **M2: heartbeats + lease reaper** |
| **F12** | **Database fails between handler finishing and the outcome being written** | The outcome is lost and the job stays `running` (same as F11). The work was done but is not recorded. | no | **M2: bounded retry of the settle, then lease recovery (the job re-runs: at-least-once)** |
| F13 | Client retries a submission whose response was lost | **Two jobs are created.** | yes — documented by `two_identical_submissions_are_two_jobs` | M2: idempotency keys |
| F14 | Handler runs longer than the lease | Nothing happens in M1 (no reaper). Once a reaper exists this would cause a second concurrent execution. | no | M2: heartbeat renewal + self-fencing |
| F15 | Handler never returns | The (single) worker is blocked indefinitely. | no | M2: per-job timeout |

F11 and F12 are the important gaps: **at Milestone 1 a job can be stranded in
`running`.** The lease columns and fencing token already exist and are
written on every claim, so Milestone 2 adds recovery without changing the
claim/settle protocol.

Manual recovery today, if needed:

```sql
-- Re-queue jobs whose lease ran out (legal transition: running -> queued).
UPDATE jobs SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL
WHERE state = 'running' AND lease_expires_at < now();
```

## 2. Security limitations

| Area | Status |
|---|---|
| Authentication / authorization | **None.** Anyone who can reach the port can submit and read jobs. The API binds to `127.0.0.1` by default for this reason. |
| TLS | None (API and database connection). Intended to run behind a terminating proxy; database TLS is a SQLx feature flag away. |
| Database privileges | One role owns the schema and is used by every process. Planned: a migration role and a DML-only application role. |
| Rate limiting / quotas | None. Body size is limited (65 KiB) but request rate is not. |
| Arbitrary code execution | Not possible by design: only handlers compiled into the binary run; payloads are data. |
| Sensitive data | Payloads, results and error messages are stored in plain text and returned by `GET /jobs/{id}`. Handlers must not put secrets in them. Payloads are never logged; error messages are. |

## 3. Unsuccessful approaches

Recorded as they happened, because the reasons are more useful than the fixes.

### 3.1 `localhost` in the database URL — a 200× slowdown

**What was tried.** The default `DATABASE_URL` used `localhost`, and
`docker-compose.yml` published PostgreSQL on `127.0.0.1` only.

**What happened.** Every test passed, but the storage suite took **188 s**
(13 tests), the API suite 11 s and the worker suite 24 s. Two tests ran for
over a minute each. The code under test was not slow: each test opens fresh
connections, and on Windows `localhost` resolves to IPv6 `::1` first. Nothing
listens there, and the connection attempt takes about two seconds to fail
before the IPv4 address is tried — per connection.

**Decision.** Default URLs use `127.0.0.1`, matching the published address.
The same suites then ran in **0.93 s, 0.52 s and 0.68 s**; the whole workspace
test run takes about 5 s. The alternative (publishing the port on both
families) was rejected because it widens what the container listens on to
work around a client-side default.

**Why it matters beyond tests.** The same two seconds would be added to every
new pool connection in production on an affected host — invisible in
steady state, painful exactly when a pool is rebuilding after an outage.

### 3.2 Deriving the heartbeat interval from an assumed lease (Python prototype)

**What was tried.** In the earlier prototype, the worker started with a
default lease length (30 s), heartbeated every third of that, and learned the
server's real lease length only from responses.

**What happened.** With a short server-side lease (0.6 s in a test), the
worker slept ~10 s before its first heartbeat. The lease expired, the job was
re-queued and re-run, the same thing happened again, and after five attempts
a perfectly healthy long-running job was dead-lettered. Found by
`test_heartbeats_keep_a_long_task_leased`.

**Decision for TaskForge.** The lease length has exactly one owner. The worker
is configured with it, passes it in the claim, and derives its renewal
interval from that same value — there is no second copy to disagree with
(Milestone 2).

## 4. Fault-injection plan *(M2–M3)*

| Fault | Injection | Assertion |
|---|---|---|
| Worker process killed mid-job | `kill -9` / `TerminateProcess` on a worker subprocess | Job completes on another worker; exactly one committed result; recovery time recorded. |
| Database connection dropped mid-settle | Proxy or `pg_terminate_backend` between handler and `complete` | Outcome eventually recorded or job re-run; never two results. |
| Lease expires while handler still running | Short lease, slow handler, heartbeats suppressed | First attempt's result rejected as stale. |
| Cancel racing completion | Fire both concurrently, many iterations | Final state is `succeeded` or `cancelled`, never changes afterwards. |
| Duplicate submission storm | N concurrent requests with one idempotency key | Exactly one row. |
| Whole-system restart under load | Stop everything mid-batch, restart | Every accepted job reaches a terminal state. |
