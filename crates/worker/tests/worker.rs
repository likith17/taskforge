//! The worker against a real (per-test) database: the single-job path.
//! Concurrency, heartbeats and crash recovery are in `reliability.rs`.

mod common;

use std::time::Duration;

use common::{fetch, submit, worker_with};
use serde_json::{Value, json};
use sqlx::PgPool;
use taskforge_domain::JobState;
use taskforge_storage::JobStore;
use taskforge_worker::{HandlerError, HandlerRegistry, JobContext, Outcome, handlers};
use tokio::sync::watch;

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn runs_a_job_and_persists_its_result(pool: PgPool) {
    let store = JobStore::new(pool);
    let worker = worker_with(&store, handlers::builtin());
    let job = submit(&store, "sum", json!({"numbers": [40, 2]})).await;

    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Succeeded(job.id))
    );

    let done = fetch(&store, job.id).await;
    assert_eq!(done.state, JobState::Succeeded);
    assert_eq!(done.result, Some(json!({"sum": 42.0, "count": 2})));
    assert_eq!(done.attempt, 1);
    assert!(done.last_error.is_none() && done.lease_owner.is_none());
    assert!(done.started_at.unwrap() <= done.finished_at.unwrap());

    // Nothing left to do.
    assert_eq!(worker.run_once().await.unwrap(), None);
    assert_eq!(worker.in_flight(), 0);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn handler_receives_the_job_context(pool: PgPool) {
    let store = JobStore::new(pool);
    let registry =
        HandlerRegistry::new().register("inspect", |ctx: JobContext, payload: Value| async move {
            Ok(json!({
                "job_id": ctx.job_id,
                "kind": ctx.kind,
                "queue": ctx.queue,
                "attempt": ctx.attempt,
                "payload": payload,
            }))
        });
    let worker = worker_with(&store, registry);
    let job = submit(&store, "inspect", json!({"x": 1})).await;

    worker.run_once().await.unwrap();
    assert_eq!(
        fetch(&store, job.id).await.result,
        Some(json!({
            "job_id": job.id, "kind": "inspect", "queue": "default", "attempt": 1,
            "payload": {"x": 1},
        }))
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn retryable_failure_requeues_and_fatal_failure_dead_letters(pool: PgPool) {
    async fn fatal(_ctx: JobContext, _payload: Value) -> Result<Value, HandlerError> {
        Err(HandlerError::fatal("this input can never work"))
    }

    let store = JobStore::new(pool);
    let worker = worker_with(&store, handlers::builtin().register("fatal", fatal));

    // The built-in `fail` kind fails retryably.
    let flaky = submit(&store, "fail", json!({"message": "upstream said no"})).await;
    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Retrying(flaky.id))
    );
    let requeued = fetch(&store, flaky.id).await;
    assert_eq!(requeued.state, JobState::Queued);
    assert_eq!(requeued.failures, 1);
    assert_eq!(
        requeued.last_error.as_deref(),
        Some("upstream said no (attempt 1)")
    );
    assert!(requeued.run_at > requeued.updated_at, "delayed by backoff");

    let hopeless = submit(&store, "fatal", json!({})).await;
    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Dead(hopeless.id))
    );
    let dead = fetch(&store, hopeless.id).await;
    assert_eq!(dead.state, JobState::Dead);
    assert_eq!(dead.failures, 1, "no retries were spent on it");
    assert!(dead.result.is_none() && dead.finished_at.is_some());
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn unregistered_kind_is_never_executed(pool: PgPool) {
    let store = JobStore::new(pool);
    let worker = worker_with(&store, handlers::builtin());
    // Reaches the queue only because this test bypasses the API's kind check.
    let job = submit(&store, "shell", json!({"cmd": "rm -rf /"})).await;

    // Dead-lettered immediately: retrying cannot make a handler appear.
    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Dead(job.id))
    );
    let dead = fetch(&store, job.id).await;
    assert_eq!(dead.state, JobState::Dead);
    assert_eq!(
        dead.last_error.as_deref(),
        Some(r#"no handler registered for job kind "shell""#)
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn panicking_handler_fails_the_job_but_not_the_worker(pool: PgPool) {
    async fn explode(_ctx: JobContext, _payload: Value) -> Result<Value, HandlerError> {
        panic!("handler bug");
    }

    let store = JobStore::new(pool);
    let registry = handlers::builtin().register("explode", explode);
    let worker = worker_with(&store, registry);
    let bad = submit(&store, "explode", json!({})).await;
    let good = submit(&store, "echo", json!({"still": "alive"})).await;

    // A panic is treated as a retryable failure.
    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Retrying(bad.id))
    );
    assert_eq!(
        fetch(&store, bad.id).await.last_error.as_deref(),
        Some("handler panicked")
    );

    // The same worker carries on with the next job.
    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::Succeeded(good.id))
    );
    assert_eq!(
        fetch(&store, good.id).await.result,
        Some(json!({"still": "alive"}))
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn losing_the_claim_mid_run_discards_the_outcome(pool: PgPool) {
    // While the handler runs, the lease expires and the reaper takes the job
    // back. The handler's result must not be recorded.
    let store = JobStore::new(pool.clone());
    let reaper_store = store.clone();
    let registry =
        HandlerRegistry::new().register("slow", move |ctx: JobContext, _payload: Value| {
            let pool = pool.clone();
            let store = reaper_store.clone();
            async move {
                sqlx::query(
                    "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
                )
                .bind(ctx.job_id.as_uuid())
                .execute(&pool)
                .await
                .unwrap();
                assert_eq!(store.reap_expired(10).await.unwrap().requeued, 1);
                Ok(json!("too late"))
            }
        });
    let worker = worker_with(&store, registry);
    let job = submit(&store, "slow", json!({})).await;

    assert_eq!(
        worker.run_once().await.unwrap(),
        Some(Outcome::LostClaim(job.id))
    );
    let after = fetch(&store, job.id).await;
    assert_eq!(
        after.state,
        JobState::Queued,
        "waiting for its next attempt"
    );
    assert!(after.result.is_none());
    assert_eq!(after.failures, 1);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn run_loop_drains_the_queue_and_stops_on_shutdown(pool: PgPool) {
    let store = JobStore::new(pool);
    let worker = worker_with(&store, handlers::builtin());
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(async move { worker.run(stopped).await });

    let mut ids = Vec::new();
    for i in 0..10 {
        ids.push(submit(&store, "echo", json!({"i": i})).await.id);
    }
    // Submitted after the worker went idle: picked up by polling.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut done = 0;
            for &id in &ids {
                done += usize::from(fetch(&store, id).await.state == JobState::Succeeded);
            }
            if done == ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all jobs should finish");

    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("worker should stop promptly")
        .unwrap();
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn worker_survives_a_database_outage(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let worker = worker_with(&store, handlers::builtin());

    pool.close().await;
    let err = worker.run_once().await.unwrap_err();
    assert!(err.is_unavailable(), "{err}");

    // The loop keeps running through the outage and still obeys shutdown.
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(async move { worker.run(stopped).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!running.is_finished());
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("worker should stop promptly")
        .unwrap();
}
