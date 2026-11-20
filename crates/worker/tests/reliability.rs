//! Milestone 2 fault injection: real worker loops, a real reaper and a real
//! database, with workers crashing, hanging and being cancelled.
//!
//! "Crash" here means aborting the worker's task tree: no handler finishes,
//! nothing is released or reported. To the database that is indistinguishable
//! from `kill -9` on the process - the worker just stops talking. (Killing
//! real OS processes is exercised by the demo in the README and is automated
//! in Milestone 3.)

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{config, eventually, fast_retries, fetch, start, submit, submit_with, wait_for_state};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use taskforge_domain::{AttemptOutcome, JobId, JobState};
use taskforge_scheduler::{Reaper, ReaperConfig};
use taskforge_storage::{JobStore, PgJobStore};
use taskforge_worker::{HandlerError, HandlerRegistry, JobContext, Worker, WorkerConfig};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

const SHORT_LEASE: Duration = Duration::from_millis(400);

fn short_lease(worker_id: &str) -> WorkerConfig {
    WorkerConfig {
        lease: SHORT_LEASE,
        ..config(worker_id)
    }
}

/// A reaper checking every 25 ms, stopped when the returned sender is dropped.
fn start_reaper(store: &PgJobStore) -> (watch::Sender<bool>, JoinHandle<()>) {
    let reaper = Reaper::new(
        store.clone(),
        ReaperConfig {
            interval: Duration::from_millis(25),
            batch: 100,
            ..ReaperConfig::default()
        },
    );
    let (stop, stopped) = watch::channel(false);
    (stop, tokio::spawn(async move { reaper.run(stopped).await }))
}

/// Sets a flag when dropped: proof that a handler's future was torn down.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A handler that signals when it starts and then never finishes.
fn hang(started: Arc<Notify>, stopped: Arc<AtomicBool>) -> HandlerRegistry {
    HandlerRegistry::new().register("job", move |_ctx: JobContext, _payload: Value| {
        let started = started.clone();
        let guard = SetOnDrop(stopped.clone());
        async move {
            let _guard = guard;
            started.notify_one();
            std::future::pending::<Result<Value, HandlerError>>().await
        }
    })
}

fn finishes_as(name: &'static str) -> HandlerRegistry {
    HandlerRegistry::new().register("job", move |ctx: JobContext, _payload: Value| async move {
        Ok(json!({ "finished_by": name, "attempt": ctx.attempt }))
    })
}

async fn attempt_log(store: &PgJobStore, id: JobId) -> Vec<(String, Option<AttemptOutcome>)> {
    store
        .attempts(id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| (a.worker_id, a.outcome))
        .collect()
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn runs_jobs_concurrently_but_never_more_than_the_limit(pool: PgPool) {
    let store = PgJobStore::new(pool);
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let registry = {
        let (running, peak) = (running.clone(), peak.clone());
        HandlerRegistry::new().register("work", move |_ctx: JobContext, _payload: Value| {
            let (running, peak) = (running.clone(), peak.clone());
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(null))
            }
        })
    };
    let mut ids = Vec::new();
    for _ in 0..16 {
        ids.push(submit(&store, "work", json!({})).await.id);
    }

    let worker = Worker::new(
        store.clone(),
        registry,
        WorkerConfig {
            concurrency: 4,
            ..config("pool")
        },
    );
    let handle = start(worker.clone());
    for id in ids {
        wait_for_state(&store, id, JobState::Succeeded).await;
    }
    handle.shutdown().await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        4,
        "used all four slots, and no more"
    );
    assert_eq!(worker.in_flight(), 0);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn transient_failures_are_retried_until_the_job_succeeds(pool: PgPool) {
    let store = fast_retries(PgJobStore::new(pool));
    let registry =
        HandlerRegistry::new().register("flaky", |ctx: JobContext, _payload: Value| async move {
            if ctx.attempt < 3 {
                Err(HandlerError::new(format!(
                    "timeout on attempt {}",
                    ctx.attempt
                )))
            } else {
                Ok(json!({ "succeeded_on_attempt": ctx.attempt }))
            }
        });
    let job = submit(&store, "flaky", json!({})).await;

    let handle = start(Worker::new(store.clone(), registry, config("w1")));
    let done = wait_for_state(&store, job.id, JobState::Succeeded).await;
    handle.shutdown().await;

    assert_eq!(done.result, Some(json!({ "succeeded_on_attempt": 3 })));
    assert_eq!(done.failures, 2);
    assert!(done.last_error.is_none(), "cleared by the success");
    let history = store.attempts(job.id).await.unwrap();
    assert_eq!(
        history.iter().map(|a| a.outcome).collect::<Vec<_>>(),
        [
            Some(AttemptOutcome::Failed),
            Some(AttemptOutcome::Failed),
            Some(AttemptOutcome::Succeeded)
        ]
    );
    assert_eq!(history[0].error.as_deref(), Some("timeout on attempt 1"));
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn a_job_that_always_fails_is_dead_lettered_after_max_attempts(pool: PgPool) {
    let store = fast_retries(PgJobStore::new(pool));
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = {
        let runs = runs.clone();
        HandlerRegistry::new().register("broken", move |_ctx: JobContext, _payload: Value| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Err::<Value, _>(HandlerError::new("still broken"))
            }
        })
    };
    let job = submit_with(&store, "broken", json!({}), |s| s.max_attempts = Some(3)).await;

    let handle = start(Worker::new(store.clone(), registry, config("w1")));
    let dead = wait_for_state(&store, job.id, JobState::Dead).await;
    // Give a buggy implementation the chance to run it a fourth time.
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.shutdown().await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        3,
        "ran exactly max_attempts times"
    );
    assert_eq!(dead.failures, 3);
    assert_eq!(dead.last_error.as_deref(), Some("still broken"));
    assert_eq!(fetch(&store, job.id).await.state, JobState::Dead);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn heartbeats_keep_a_long_job_leased_well_past_one_lease(pool: PgPool) {
    let store = PgJobStore::new(pool);
    let (_stop_reaper, _reaper) = start_reaper(&store);
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = {
        let runs = runs.clone();
        HandlerRegistry::new().register("long", move |_ctx: JobContext, _payload: Value| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(SHORT_LEASE * 4).await; // four lease lengths
                Ok(json!("finished"))
            }
        })
    };
    let job = submit(&store, "long", json!({})).await;

    let handle = start(Worker::new(store.clone(), registry, short_lease("w1")));
    let done = wait_for_state(&store, job.id, JobState::Succeeded).await;
    handle.shutdown().await;

    // The reaper was watching the whole time and never had cause to step in.
    assert_eq!(done.attempt, 1);
    assert_eq!(done.failures, 0);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn a_crashed_workers_job_is_recovered_and_finished_by_another(pool: PgPool) {
    let store = fast_retries(PgJobStore::new(pool));
    let started = Arc::new(Notify::new());
    let torn_down = Arc::new(AtomicBool::new(false));
    let job = submit(&store, "job", json!({})).await;

    // The doomed worker picks the job up and is killed mid-run.
    let doomed = start(Worker::new(
        store.clone(),
        hang(started.clone(), torn_down.clone()),
        short_lease("doomed"),
    ));
    started.notified().await;
    doomed.crash().await;

    // Nothing was reported: the job still looks like it is running.
    let orphan = fetch(&store, job.id).await;
    assert_eq!(orphan.state, JobState::Running);
    assert_eq!(orphan.lease_owner.as_deref(), Some("doomed"));
    let crashed_at = tokio::time::Instant::now();

    // Only now do a reaper and a second worker exist.
    let (_stop_reaper, _reaper) = start_reaper(&store);
    let survivor = start(Worker::new(
        store.clone(),
        finishes_as("survivor"),
        short_lease("survivor"),
    ));
    let done = wait_for_state(&store, job.id, JobState::Succeeded).await;
    let recovery = crashed_at.elapsed();
    survivor.shutdown().await;

    assert_eq!(
        done.result,
        Some(json!({ "finished_by": "survivor", "attempt": 2 }))
    );
    assert_eq!(done.failures, 1);
    assert_eq!(
        attempt_log(&store, job.id).await,
        [
            ("doomed".to_owned(), Some(AttemptOutcome::LeaseExpired)),
            ("survivor".to_owned(), Some(AttemptOutcome::Succeeded)),
        ]
    );
    // Recovery is bounded by lease + reaper interval + backoff, not open-ended.
    assert!(
        recovery < SHORT_LEASE + Duration::from_secs(3),
        "took {recovery:?}"
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn a_worker_that_cannot_renew_its_lease_stops_itself(pool: PgPool) {
    // A partitioned worker: alive and running the handler, but cut off from
    // the database. It must not keep working alongside its replacement.
    let store = fast_retries(PgJobStore::new(pool.clone()));
    let partitioned_pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_millis(200))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let partitioned_store = fast_retries(PgJobStore::new(partitioned_pool.clone()));

    let started = Arc::new(Notify::new());
    let torn_down = Arc::new(AtomicBool::new(false));
    let job = submit(&store, "job", json!({})).await;

    let zombie_worker = Worker::new(
        partitioned_store,
        hang(started.clone(), torn_down.clone()),
        short_lease("zombie"),
    );
    let zombie = start(zombie_worker.clone());
    started.notified().await;
    partitioned_pool.close().await; // the network goes away

    // The rest of the system carries on and finishes the job elsewhere.
    let (_stop_reaper, _reaper) = start_reaper(&store);
    let healthy = start(Worker::new(
        store.clone(),
        finishes_as("healthy"),
        short_lease("healthy"),
    ));
    let done = wait_for_state(&store, job.id, JobState::Succeeded).await;

    // Meanwhile the zombie noticed it could not renew, and stopped its handler
    // by itself - without being told, since nobody could reach it.
    eventually(
        "the zombie to stop its handler",
        Duration::from_secs(5),
        || async { torn_down.load(Ordering::SeqCst) && zombie_worker.in_flight() == 0 },
    )
    .await;

    assert_eq!(
        done.result,
        Some(json!({ "finished_by": "healthy", "attempt": 2 }))
    );
    assert_eq!(
        attempt_log(&store, job.id).await,
        [
            ("zombie".to_owned(), Some(AttemptOutcome::LeaseExpired)),
            ("healthy".to_owned(), Some(AttemptOutcome::Succeeded)),
        ]
    );
    healthy.shutdown().await;
    zombie.crash().await;
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn cancelling_a_running_job_stops_its_handler(pool: PgPool) {
    let store = PgJobStore::new(pool);
    let started = Arc::new(Notify::new());
    let torn_down = Arc::new(AtomicBool::new(false));
    let job = submit(&store, "job", json!({})).await;

    let handle = start(Worker::new(
        store.clone(),
        hang(started.clone(), torn_down.clone()),
        short_lease("w1"),
    ));
    started.notified().await;
    assert!(!torn_down.load(Ordering::SeqCst));

    store.request_cancel(job.id).await.unwrap();
    // Delivered by the next heartbeat (a third of the lease), then acknowledged.
    let cancelled = wait_for_state(&store, job.id, JobState::Cancelled).await;
    handle.shutdown().await;

    assert!(torn_down.load(Ordering::SeqCst), "the handler was stopped");
    assert_eq!(cancelled.failures, 0, "being cancelled is not a failure");
    assert!(cancelled.result.is_none());
    assert_eq!(
        attempt_log(&store, job.id).await,
        [("w1".to_owned(), Some(AttemptOutcome::Cancelled))]
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn graceful_shutdown_lets_short_jobs_finish_and_hands_long_ones_back(pool: PgPool) {
    let store = PgJobStore::new(pool);
    let both_started = Arc::new(tokio::sync::Semaphore::new(0));
    let registry = {
        let quick_started = both_started.clone();
        let slow_started = both_started.clone();
        HandlerRegistry::new()
            .register("quick", move |_ctx: JobContext, _payload: Value| {
                let started = quick_started.clone();
                async move {
                    started.add_permits(1);
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Ok(json!("finished during the grace period"))
                }
            })
            .register("slow", move |_ctx: JobContext, _payload: Value| {
                let started = slow_started.clone();
                async move {
                    started.add_permits(1);
                    std::future::pending::<Result<Value, HandlerError>>().await
                }
            })
    };
    let quick = submit(&store, "quick", json!({})).await;
    // One attempt only: if a release cost a failure, this job would die.
    let slow = submit_with(&store, "slow", json!({}), |s| s.max_attempts = Some(1)).await;

    let handle = start(Worker::new(
        store.clone(),
        registry,
        WorkerConfig {
            shutdown_grace: Duration::from_millis(500),
            ..config("w1")
        },
    ));
    let _both = both_started.acquire_many(2).await.unwrap();
    handle.shutdown().await; // returns only when the worker has fully stopped

    let quick = fetch(&store, quick.id).await;
    assert_eq!(quick.state, JobState::Succeeded);
    assert_eq!(
        quick.result,
        Some(json!("finished during the grace period"))
    );

    let slow = fetch(&store, slow.id).await;
    assert_eq!(
        slow.state,
        JobState::Queued,
        "back in the queue for another worker"
    );
    assert_eq!(slow.failures, 0);
    assert!(slow.lease_owner.is_none());
    assert!(
        slow.run_at <= slow.updated_at,
        "runnable immediately, no backoff"
    );
    assert_eq!(
        attempt_log(&store, slow.id).await,
        [("w1".to_owned(), Some(AttemptOutcome::Released))]
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn several_workers_share_a_queue_without_running_anything_twice(pool: PgPool) {
    const JOBS: usize = 150;
    let store = PgJobStore::new(pool.clone());
    let executed = Arc::new(Mutex::new(Vec::new()));
    let registry = {
        let executed = executed.clone();
        HandlerRegistry::new().register("unit", move |ctx: JobContext, payload: Value| {
            let executed = executed.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                executed.lock().unwrap().push(ctx.job_id);
                Ok(payload)
            }
        })
    };
    for i in 0..JOBS {
        submit(&store, "unit", json!({ "i": i })).await;
    }

    let mut workers = Vec::new();
    for w in 0..3 {
        workers.push(start(Worker::new(
            store.clone(),
            registry.clone(),
            WorkerConfig {
                concurrency: 8,
                ..config(&format!("w{w}"))
            },
        )));
    }
    eventually("all jobs to succeed", Duration::from_secs(30), || async {
        let done: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state = 'succeeded'")
            .fetch_one(&pool)
            .await
            .unwrap();
        done as usize == JOBS
    })
    .await;
    for worker in workers {
        worker.shutdown().await;
    }

    // Copy out, so no lock is held across the `.await` below.
    let executed = executed.lock().unwrap().clone();
    assert_eq!(
        executed.len(),
        JOBS,
        "with no failures, each job runs exactly once"
    );
    assert_eq!(executed.iter().collect::<HashSet<_>>().len(), JOBS);
    let (attempts, by_workers): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(DISTINCT worker_id) FROM job_attempts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts as usize, JOBS);
    assert!(by_workers > 1, "the work was actually shared");
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn every_job_completes_exactly_once_while_workers_keep_crashing(pool: PgPool) {
    const JOBS: usize = 80;
    let store = fast_retries(PgJobStore::new(pool.clone()));
    let (_stop_reaper, _reaper) = start_reaper(&store);

    let executions = Arc::new(AtomicUsize::new(0));
    let registry = {
        let executions = executions.clone();
        HandlerRegistry::new().register("unit", move |_ctx: JobContext, payload: Value| {
            let executions = executions.clone();
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(payload)
            }
        })
    };
    for i in 0..JOBS {
        // A generous budget: a job may be unlucky several kills in a row.
        submit_with(&store, "unit", json!({ "i": i }), |s| {
            s.max_attempts = Some(25)
        })
        .await;
    }

    let spawn_worker = |n: usize| {
        start(Worker::new(
            store.clone(),
            registry.clone(),
            WorkerConfig {
                concurrency: 4,
                ..short_lease(&format!("w{n}"))
            },
        ))
    };
    let mut workers: Vec<_> = (0..3).map(spawn_worker).collect();

    // Kill a worker mid-flight every 120 ms, six times, replacing it each time.
    for round in 0..6 {
        tokio::time::sleep(Duration::from_millis(120)).await;
        workers.remove(0).crash().await;
        workers.push(spawn_worker(3 + round));
    }

    eventually(
        "every job to succeed despite the crashes",
        Duration::from_secs(60),
        || async {
            let done: i64 =
                sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state = 'succeeded'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            done as usize == JOBS
        },
    )
    .await;
    for worker in workers {
        worker.shutdown().await;
    }

    // Nothing lost, nothing stuck, nothing dead-lettered.
    let unfinished: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state <> 'succeeded'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unfinished, 0);

    // Exactly one committed result per job, and every result is the right one.
    let (succeeded_attempts, jobs_with_success, correct_results): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM job_attempts WHERE outcome = 'succeeded'),
                (SELECT count(DISTINCT job_id) FROM job_attempts WHERE outcome = 'succeeded'),
                (SELECT count(*) FROM jobs WHERE result = payload)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(succeeded_attempts as usize, JOBS);
    assert_eq!(jobs_with_success as usize, JOBS);
    assert_eq!(correct_results as usize, JOBS);

    // At-least-once, made visible: the crashes forced some jobs to run again.
    let (recovered, open): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE outcome = 'lease_expired'),
                count(*) FILTER (WHERE outcome IS NULL)
         FROM job_attempts",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        recovered > 0,
        "the kills should have orphaned at least one job"
    );
    assert_eq!(open, 0, "no attempt was left dangling");
    let executions = executions.load(Ordering::SeqCst);
    assert!(executions > JOBS, "{executions} executions for {JOBS} jobs");
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn a_handler_that_overruns_its_timeout_is_stopped_and_retried(pool: PgPool) {
    let store = fast_retries(PgJobStore::new(pool));
    let registry =
        HandlerRegistry::new().register("job", |ctx: JobContext, _payload: Value| async move {
            if ctx.attempt == 1 {
                std::future::pending::<()>().await; // hangs forever
            }
            Ok(json!({ "attempt": ctx.attempt }))
        });
    let job = submit_with(&store, "job", json!({}), |s| s.timeout_secs = Some(1)).await;

    let started = tokio::time::Instant::now();
    let handle = start(Worker::new(store.clone(), registry, config("w1")));
    let done = wait_for_state(&store, job.id, JobState::Succeeded).await;
    handle.shutdown().await;

    assert_eq!(done.result, Some(json!({ "attempt": 2 })));
    let history = store.attempts(job.id).await.unwrap();
    assert_eq!(history[0].outcome, Some(AttemptOutcome::Failed));
    assert_eq!(history[0].error.as_deref(), Some("timed out after 1s"));
    // Stopped at the timeout, not at the (60 s) lease.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn a_partitioned_worker_stops_before_its_lease_can_expire(pool: PgPool) {
    // The guarantee that matters: by the time the database considers the lease
    // expired (and a reaper may hand the job to someone else), the old worker
    // has already stopped. Two copies must never run at the same time.
    //
    // Only durations are compared - local ones with local, database ones with
    // database - because the host clock and the clock inside the Docker VM can
    // be offset from each other.
    let store = PgJobStore::new(pool.clone());
    let partitioned_pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_millis(100))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let stopped_at = Arc::new(Mutex::new(None));
    let registry = {
        let stopped_at = stopped_at.clone();
        HandlerRegistry::new().register("job", move |_ctx: JobContext, _payload: Value| {
            let stopped_at = stopped_at.clone();
            async move {
                struct Mark(Arc<Mutex<Option<tokio::time::Instant>>>);
                impl Drop for Mark {
                    fn drop(&mut self) {
                        *self.0.lock().unwrap() = Some(tokio::time::Instant::now());
                    }
                }
                let _mark = Mark(stopped_at);
                std::future::pending::<Result<Value, HandlerError>>().await
            }
        })
    };
    let job = submit(&store, "job", json!({})).await;

    let worker = Worker::new(
        PgJobStore::new(partitioned_pool.clone()),
        registry,
        short_lease("partitioned"),
    );
    // Taken before the worker exists, so no earlier than its claim.
    let before_claim = tokio::time::Instant::now();
    let handle = start(worker.clone());
    wait_for_state(&store, job.id, JobState::Running).await;
    partitioned_pool.close().await; // cut off: no more renewals

    eventually(
        "the handler to be stopped",
        Duration::from_secs(5),
        || async { stopped_at.lock().unwrap().is_some() },
    )
    .await;
    let ran_for = stopped_at.lock().unwrap().unwrap() - before_claim;
    // How long the database granted, from the claim to the (possibly renewed)
    // expiry. A reaper could take the job back only after this.
    let held = fetch(&store, job.id).await;
    let granted = (held.lease_expires_at.unwrap() - held.started_at.unwrap())
        .to_std()
        .unwrap();
    assert!(
        ran_for < granted,
        "handler ran {ran_for:?}, but the lease was only granted for {granted:?}"
    );
    handle.crash().await;
}
