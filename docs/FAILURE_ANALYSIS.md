# Failure analysis

What can go wrong, what TaskForge does, and which test injects it. "Chaos"
means the real-process harness (`taskforge-chaos`, see EXPERIMENTS.md); the rest
are automated tests in `cargo test`.

## Failure modes

| # | Failure | Behaviour | Evidence |
|---|---|---|---|
| F1 | Concurrent claims | Each claimer gets different jobs (`SKIP LOCKED`) | `concurrent_claimers_never_receive_the_same_job` (12 claimers, 60 jobs); `several_workers_share_a_queue_without_running_anything_twice` |
| F2 | Worker killed mid-job | Lease expires; reaper re-queues with backoff; another worker runs it | `a_crashed_workers_job_is_recovered_and_finished_by_another`; chaos C1–C3 (real `SIGKILL`/`TerminateProcess`) |
| F3 | Worker partitioned or frozen, still running | Stops its own handler before the lease can expire; its late writes are fenced off | `a_partitioned_worker_stops_before_its_lease_can_expire`; `a_worker_that_cannot_renew_its_lease_stops_itself`; `expired_lease_is_recovered_and_the_zombie_worker_is_fenced_off` |
| F4 | Stale or forged report | `StaleClaim`, nothing written | `stale_or_foreign_claims_cannot_settle_a_job`; conformance `lease_expiry_recovers_the_job_and_fences_the_old_worker` |
| F5 | Illegal transition from a code bug | Database trigger refuses it | `database_trigger_enforces_exactly_the_domain_state_machine` (all 20 pairs) |
| F6 | Handler error | Retry with jittered backoff, dead-letter after `max_attempts`; fatal errors dead-letter at once | `transient_failures_are_retried_until_the_job_succeeds`; `a_job_that_always_fails_is_dead_lettered_after_max_attempts`; conformance `failure_backoff_and_dead_letter` |
| F7 | Handler panics | Retryable failure; worker continues | `panicking_handler_fails_the_job_but_not_the_worker` |
| F8 | Handler hangs | Stopped at `timeout_secs`, retried | `a_handler_that_overruns_its_timeout_is_stopped_and_retried` |
| F9 | Job that kills every worker it lands on | Each lost lease counts as a failure; dead-lettered after `max_attempts` | `a_job_that_keeps_killing_its_workers_is_eventually_dead_lettered` |
| F10 | Scheduler (reaper) killed | Reaper keeps no state; a restarted one, or the reaper inside any worker, carries on | `concurrent_reapers_recover_each_lease_exactly_once`; chaos C2, C3 |
| F11 | Database down: API | `503` + `Retry-After`; `/readyz` fails, `/healthz` stays up | `health_endpoints` |
| F12 | Database down: worker | Claims back off; outcomes retried for up to one lease, then left to lease recovery | `worker_survives_a_database_outage`; `settle_is_retried_through_a_database_outage` |
| F13 | Database fails between handler finishing and the write | Write retried; if still failing, the lease expires and the job runs again (at-least-once) | `settle_retry_gives_up_at_the_deadline`; chaos (kills land in this window) |
| F14 | Statement hangs (locks, overload) | `statement_timeout` (5 s) fails it; treated as retryable | `is_unavailable` maps SQLSTATE 57014 |
| F15 | Client retries a submission | Idempotency key: one job; different request with the same key: `409` | `duplicate_submission_storm_creates_exactly_one_job` (40 concurrent); `concurrent_duplicate_requests_create_one_job` (HTTP); conformance `idempotent_submission`; chaos (every job submitted twice) |
| F16 | Cancel races completion | First committed write wins; the other is rejected; final state never changes | `cancel_racing_completion_ends_in_exactly_one_final_state` (40 rounds); `cancel_racing_a_claim_never_leaves_a_cancelled_job_running` |
| F17 | Graceful shutdown with jobs in flight | Finish within the grace period, then release (no failure counted) | `graceful_shutdown_lets_short_jobs_finish_and_hands_long_ones_back` |
| F18 | Main pool saturated | Heartbeats use a separate pool, so leases are still renewed | Design (DECISIONS 2.4); no automated test |
| F19 | No reaper running at all | Expired leases pile up; gauge `taskforge_expired_unreaped_leases` rises | Workers reap by default; conformance `purge_and_stats` checks the gauge source |

## What is not covered

- **Handlers that never yield.** Abort, timeout and self-fencing take effect at
  the next `.await`. A CPU-bound or blocking handler keeps running after its
  lease is lost. Its result is still rejected by the fencing token, but its side
  effects are not prevented.
- **Side effects outside the database.** At-least-once delivery means a
  non-idempotent handler can repeat an external effect. Chaos runs count how
  often this would have happened (`jobs_whose_unprotected_side_effect_ran_twice`).
- **PostgreSQL itself failing over or losing data.** Out of scope. Durability is
  whatever the PostgreSQL deployment provides (`synchronous_commit = on` here).
- **Clock-rate drift beyond 10% of a lease.** Self-fencing assumes the worker's
  clock and the database's run at nearly the same rate.

## Security limitations

| Area | Status |
|---|---|
| Authentication / authorization | **None.** The API binds to `127.0.0.1` by default for this reason |
| TLS | None, for the API or the database connection |
| Database privileges | One role owns the schema and is used by every process. Workers never run migrations, so a DML-only role would work, but none is set up |
| Rate limiting | None; body size is limited |
| Code execution | Only handlers compiled into the binary run; a job is a name plus JSON. The `chaos` kind is off unless `TASKFORGE_CHAOS_HANDLER=true` |
| Sensitive data | Payloads, results and errors are stored in plain text and returned by the API; handlers must not put secrets in them. Payloads are not logged |
