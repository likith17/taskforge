//! Milestone 2: idempotency, retries, lease recovery, cancellation.
//! Same setup as `tests.rs`: one fresh database per test.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use taskforge_domain::{
    AttemptOutcome, IdempotencyKey, JobId, JobState, JobSubmission, NewJob, RetryPolicy,
};

use crate::{CancelOutcome, Claim, JobStore, ListFilter, StoreError, Submitted};

const LEASE: Duration = Duration::from_secs(60);

fn queues() -> Vec<String> {
    vec!["default".to_owned()]
}

fn new_job(edit: impl FnOnce(&mut JobSubmission)) -> NewJob {
    let mut submission = JobSubmission {
        kind: "echo".to_owned(),
        ..Default::default()
    };
    edit(&mut submission);
    NewJob::try_from(submission).expect("valid test submission")
}

fn keyed(key: &str, edit: impl FnOnce(&mut JobSubmission)) -> NewJob {
    new_job(edit).with_idempotency_key(IdempotencyKey::parse(key).unwrap())
}

/// Pretend every leaseholder has been silent for longer than its lease.
async fn expire_leases(pool: &PgPool) {
    sqlx::query(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE state = 'running'",
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Skip any retry backoff.
async fn make_due(pool: &PgPool) {
    sqlx::query("UPDATE jobs SET run_at = now() WHERE state = 'queued'")
        .execute(pool)
        .await
        .unwrap();
}

async fn claim_one(store: &JobStore, worker: &str) -> Claim {
    store
        .claim_next(worker, &queues(), LEASE)
        .await
        .unwrap()
        .expect("a job should be claimable")
        .claim
}

async fn outcomes(store: &JobStore, id: JobId) -> Vec<Option<AttemptOutcome>> {
    store
        .attempts(id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.outcome)
        .collect()
}

// ------------------------------------------------------------- idempotency

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn same_key_and_request_returns_the_original_job(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let payload = json!({"order": 8841});

    let first = store
        .submit(&keyed("order-8841", |s| s.payload = Some(payload.clone())))
        .await
        .unwrap();
    // A retry is a *new* NewJob with a new id; only the key ties them together.
    let second = store
        .submit(&keyed("order-8841", |s| s.payload = Some(payload.clone())))
        .await
        .unwrap();

    let Submitted::Created(original) = first else {
        panic!("first submission should create")
    };
    assert_eq!(second, Submitted::Duplicate(original.clone()));
    assert_eq!(original.idempotency_key.as_deref(), Some("order-8841"));

    // Still the same job after it has run: the key outlives the queue.
    let claim = claim_one(&store, "w1").await;
    store.complete(&claim, &json!("done")).await.unwrap();
    let third = store
        .submit(&keyed("order-8841", |s| s.payload = Some(payload)))
        .await
        .unwrap();
    assert!(third.is_duplicate());
    assert_eq!(third.job().id, original.id);
    assert_eq!(third.job().state, JobState::Succeeded);

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn same_key_with_a_different_request_is_rejected(pool: PgPool) {
    let store = JobStore::new(pool);
    store
        .submit(&keyed("k", |s| s.payload = Some(json!({"amount": 100}))))
        .await
        .unwrap();

    type Edit = Box<dyn FnOnce(&mut JobSubmission)>;
    let different: [Edit; 4] = [
        Box::new(|s| s.payload = Some(json!({"amount": 999}))),
        Box::new(|s| {
            s.payload = Some(json!({"amount": 100}));
            s.kind = "sum".to_owned();
        }),
        Box::new(|s| {
            s.payload = Some(json!({"amount": 100}));
            s.priority = Some(9);
        }),
        Box::new(|s| {
            s.payload = Some(json!({"amount": 100}));
            s.max_attempts = Some(1);
        }),
    ];
    for edit in different {
        let err = store.submit(&keyed("k", edit)).await.unwrap_err();
        assert!(
            matches!(&err, StoreError::IdempotencyConflict { key } if key == "k"),
            "{err}"
        );
    }

    // Jobs without a key never collide with anything.
    let a = store.insert(&new_job(|_| {})).await.unwrap();
    let b = store.insert(&new_job(|_| {})).await.unwrap();
    assert_ne!(a.id, b.id);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn duplicate_submission_storm_creates_exactly_one_job(pool: PgPool) {
    const CLIENTS: usize = 40;
    let store = JobStore::new(pool.clone());

    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .submit(&keyed("storm", |s| s.payload = Some(json!({"n": 1}))))
                .await
                .unwrap()
        }));
    }
    let mut created = 0;
    let mut ids = HashSet::new();
    for task in tasks {
        let submitted = task.await.unwrap();
        created += usize::from(!submitted.is_duplicate());
        ids.insert(submitted.job().id);
    }

    assert_eq!(created, 1, "exactly one request created the job");
    assert_eq!(ids.len(), 1, "every request was answered with that job");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

// ------------------------------------------------------------------ retries

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn retryable_failure_requeues_with_jittered_exponential_backoff(pool: PgPool) {
    let policy = RetryPolicy::new(Duration::from_secs(10), Duration::from_secs(25));
    let store = JobStore::new(pool.clone()).with_retry_policy(policy);
    let job = store
        .insert(&new_job(|s| s.max_attempts = Some(10)))
        .await
        .unwrap();

    // Ceilings 10s, 20s, then capped at 25s; the delay lies in the upper half.
    for (failure, ceiling) in [(1, 10.0), (2, 20.0), (3, 25.0), (4, 25.0)] {
        make_due(&pool).await;
        let claim = claim_one(&store, "w1").await;
        let after = store.fail(&claim, "upstream 503", true).await.unwrap();

        assert_eq!(after.state, JobState::Queued);
        assert_eq!(after.failures, failure);
        assert_eq!(after.last_error.as_deref(), Some("upstream 503"));
        assert!(after.lease_owner.is_none() && after.finished_at.is_none());

        let delay = (after.run_at - after.updated_at).num_milliseconds() as f64 / 1000.0;
        assert!(
            (ceiling / 2.0..=ceiling).contains(&delay),
            "failure {failure}: delay {delay}s not in [{}, {ceiling}]",
            ceiling / 2.0
        );
        // Not runnable until the backoff has passed.
        assert!(
            store
                .claim_next("w1", &queues(), LEASE)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(store.get(job.id).await.unwrap().unwrap().attempt, 4);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn retries_are_spread_out_not_synchronised(pool: PgPool) {
    let policy = RetryPolicy::new(Duration::from_secs(100), Duration::from_secs(100));
    let store = JobStore::new(pool.clone()).with_retry_policy(policy);
    for _ in 0..20 {
        store.insert(&new_job(|_| {})).await.unwrap();
    }
    let claimed = store.claim("w1", &queues(), LEASE, 20).await.unwrap();
    let mut run_ats = HashSet::new();
    for c in &claimed {
        run_ats.insert(store.fail(&c.claim, "boom", true).await.unwrap().run_at);
    }
    // 20 jobs failing together must not all come back at the same instant.
    assert!(
        run_ats.len() >= 15,
        "only {} distinct retry times",
        run_ats.len()
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn exhausted_retries_dead_letter_and_an_operator_can_redrive(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let job = store
        .insert(&new_job(|s| s.max_attempts = Some(3)))
        .await
        .unwrap();

    let mut last = job.clone();
    for n in 1..=3 {
        make_due(&pool).await;
        let claim = claim_one(&store, "w1").await;
        last = store
            .fail(&claim, &format!("boom {n}"), true)
            .await
            .unwrap();
    }
    assert_eq!(last.state, JobState::Dead);
    assert_eq!(last.failures, 3);
    assert_eq!(last.last_error.as_deref(), Some("boom 3"));
    assert!(last.finished_at.is_some());
    assert!(
        store
            .claim_next("w1", &queues(), LEASE)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![Some(AttemptOutcome::Failed); 3]
    );

    // Re-drive: back in the queue with a fresh budget; history is kept.
    let revived = store.redrive(job.id).await.unwrap();
    assert_eq!(revived.state, JobState::Queued);
    assert_eq!(revived.failures, 0);
    assert!(revived.finished_at.is_none());
    assert_eq!(revived.last_error.as_deref(), Some("boom 3"));

    let claim = claim_one(&store, "w1").await;
    assert_eq!(claim.attempt(), 4, "the fencing token keeps counting");
    store.complete(&claim, &json!("finally")).await.unwrap();

    // Only dead jobs can be re-driven.
    let err = store.redrive(job.id).await.unwrap_err();
    assert!(matches!(
        err,
        StoreError::InvalidState {
            state: JobState::Succeeded,
            action: "retry",
            ..
        }
    ));
    assert!(matches!(
        store.redrive(JobId::new()).await,
        Err(StoreError::NotFound(_))
    ));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn non_retryable_failure_skips_the_remaining_budget(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store
        .insert(&new_job(|s| s.max_attempts = Some(10)))
        .await
        .unwrap();
    let claim = claim_one(&store, "w1").await;

    let dead = store.fail(&claim, "invalid input", false).await.unwrap();
    assert_eq!(dead.state, JobState::Dead);
    assert_eq!(dead.failures, 1);
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![Some(AttemptOutcome::Failed)]
    );
}

// ----------------------------------------------------------- lease recovery

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn expired_lease_is_recovered_and_the_zombie_worker_is_fenced_off(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let job = store.insert(&new_job(|_| {})).await.unwrap();
    let zombie = claim_one(&store, "w1").await;

    // Nothing to do while the lease is alive.
    assert_eq!(store.reap_expired(100).await.unwrap().total(), 0);

    expire_leases(&pool).await;
    let reaped = store.reap_expired(100).await.unwrap();
    assert_eq!((reaped.requeued, reaped.dead, reaped.cancelled), (1, 0, 0));

    let requeued = store.get(job.id).await.unwrap().unwrap();
    assert_eq!(requeued.state, JobState::Queued);
    assert_eq!(requeued.failures, 1);
    assert!(requeued.lease_owner.is_none());
    assert!(requeued.last_error.unwrap().contains("lease expired"));
    assert!(
        requeued.run_at > requeued.updated_at,
        "retried with backoff"
    );

    make_due(&pool).await;
    let successor = claim_one(&store, "w2").await;
    assert_eq!(successor.attempt(), zombie.attempt() + 1);

    // w1 was only slow, not dead. Everything it tries now is refused:
    // completing, failing, releasing, and even renewing its lease.
    for attempt in [
        store.complete(&zombie, &json!({"from": "zombie"})).await,
        store.fail(&zombie, "zombie error", true).await,
        store.release(&zombie).await,
    ] {
        assert!(
            matches!(
                attempt,
                Err(StoreError::StaleClaim {
                    current_attempt: 2,
                    ..
                })
            ),
            "{attempt:?}"
        );
    }
    let beat = store
        .heartbeat("w1", &[zombie.clone()], LEASE)
        .await
        .unwrap();
    assert_eq!(beat.lost, vec![job.id]);

    let done = store
        .complete(&successor, &json!({"from": "w2"}))
        .await
        .unwrap();
    assert_eq!(done.result, Some(json!({"from": "w2"})));
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![
            Some(AttemptOutcome::LeaseExpired),
            Some(AttemptOutcome::Succeeded)
        ]
    );
    let history = store.attempts(job.id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .map(|a| a.worker_id.as_str())
            .collect::<Vec<_>>(),
        ["w1", "w2"]
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn concurrent_reapers_recover_each_lease_exactly_once(pool: PgPool) {
    const JOBS: u32 = 30;
    let store = JobStore::new(pool.clone());
    for _ in 0..JOBS {
        store.insert(&new_job(|_| {})).await.unwrap();
    }
    assert_eq!(
        store
            .claim("w1", &queues(), LEASE, JOBS)
            .await
            .unwrap()
            .len(),
        JOBS as usize
    );
    expire_leases(&pool).await;

    let mut reapers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        reapers.push(tokio::spawn(async move {
            store.reap_expired(100).await.unwrap()
        }));
    }
    let mut requeued = 0;
    for reaper in reapers {
        requeued += reaper.await.unwrap().requeued;
    }
    assert_eq!(requeued, u64::from(JOBS));

    // Had any lease been reaped twice, some job would show two failures.
    let (max_failures, attempts): (i32, i64) = sqlx::query_as(
        "SELECT max(failures), (SELECT count(*) FROM job_attempts WHERE outcome = 'lease_expired')
         FROM jobs",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(max_failures, 1);
    assert_eq!(attempts, i64::from(JOBS));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn a_job_that_keeps_killing_its_workers_is_eventually_dead_lettered(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let job = store
        .insert(&new_job(|s| s.max_attempts = Some(2)))
        .await
        .unwrap();

    for expected in [JobState::Queued, JobState::Dead] {
        make_due(&pool).await;
        claim_one(&store, "w1").await;
        expire_leases(&pool).await;
        store.reap_expired(100).await.unwrap();
        assert_eq!(store.get(job.id).await.unwrap().unwrap().state, expected);
    }
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![Some(AttemptOutcome::LeaseExpired); 2]
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn heartbeat_renews_held_leases_and_reports_lost_and_cancelled_ones(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    for _ in 0..3 {
        store.insert(&new_job(|_| {})).await.unwrap();
    }
    let claims: Vec<Claim> = store
        .claim("w1", &queues(), Duration::from_secs(10), 3)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.claim)
        .collect();
    let (kept, lost, cancelled) = (&claims[0], &claims[1], &claims[2]);
    let before = store
        .get(kept.job_id())
        .await
        .unwrap()
        .unwrap()
        .lease_expires_at
        .unwrap();

    // `lost` expires and is taken over by another worker.
    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(lost.job_id().as_uuid())
        .execute(&pool)
        .await
        .unwrap();
    store.reap_expired(100).await.unwrap();
    make_due(&pool).await;
    claim_one(&store, "w2").await;
    // `cancelled` gets a cancellation request.
    store.request_cancel(cancelled.job_id()).await.unwrap();

    let beat = store
        .heartbeat("w1", &claims, Duration::from_secs(600))
        .await
        .unwrap();
    assert_eq!(beat.lost, vec![lost.job_id()]);
    assert_eq!(beat.cancel_requested, vec![cancelled.job_id()]);

    let after = store
        .get(kept.job_id())
        .await
        .unwrap()
        .unwrap()
        .lease_expires_at
        .unwrap();
    assert!(
        after > before + chrono::Duration::seconds(500),
        "lease was extended"
    );
    // The other worker's lease on the lost job was not touched by w1's heartbeat.
    let stolen = store.get(lost.job_id()).await.unwrap().unwrap();
    assert_eq!(stolen.lease_owner.as_deref(), Some("w2"));
    assert!(stolen.lease_expires_at.unwrap() < before + chrono::Duration::seconds(500));

    assert_eq!(
        store.heartbeat("w1", &[], LEASE).await.unwrap(),
        Default::default()
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn release_requeues_at_once_without_spending_a_failure(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store
        .insert(&new_job(|s| s.max_attempts = Some(1)))
        .await
        .unwrap();
    let claim = claim_one(&store, "w1").await;

    let released = store.release(&claim).await.unwrap();
    assert_eq!(released.state, JobState::Queued);
    assert_eq!(released.failures, 0);

    // max_attempts is 1, yet the job is immediately runnable again.
    let again = claim_one(&store, "w2").await;
    assert_eq!(again.attempt(), 2);
    store.complete(&again, &json!(null)).await.unwrap();
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![
            Some(AttemptOutcome::Released),
            Some(AttemptOutcome::Succeeded)
        ]
    );
}

// ------------------------------------------------------------- cancellation

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn cancelling_a_queued_job_is_immediate_and_repeatable(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store.insert(&new_job(|_| {})).await.unwrap();

    let CancelOutcome::Cancelled(cancelled) = store.request_cancel(job.id).await.unwrap() else {
        panic!("a queued job should be cancelled at once")
    };
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(cancelled.cancel_requested && cancelled.finished_at.is_some());
    assert_eq!(cancelled.attempt, 0, "it never ran");
    assert!(
        store
            .claim_next("w1", &queues(), LEASE)
            .await
            .unwrap()
            .is_none()
    );

    // Asking again gives the same answer and changes nothing.
    assert_eq!(
        store.request_cancel(job.id).await.unwrap(),
        CancelOutcome::Cancelled(cancelled)
    );
    assert!(matches!(
        store.request_cancel(JobId::new()).await,
        Err(StoreError::NotFound(_))
    ));
    // Cancelled is final: not even a re-drive brings it back.
    assert!(matches!(
        store.redrive(job.id).await,
        Err(StoreError::InvalidState {
            state: JobState::Cancelled,
            ..
        })
    ));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn cancelling_a_running_job_waits_for_the_worker_to_acknowledge(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store.insert(&new_job(|_| {})).await.unwrap();
    let claim = claim_one(&store, "w1").await;

    // Acknowledging a cancellation nobody asked for is refused.
    assert!(matches!(
        store.acknowledge_cancel(&claim).await,
        Err(StoreError::StaleClaim {
            current_state: JobState::Running,
            ..
        })
    ));

    let CancelOutcome::Requested(running) = store.request_cancel(job.id).await.unwrap() else {
        panic!("a running job can only be asked to stop")
    };
    assert_eq!(running.state, JobState::Running);
    assert!(running.cancel_requested);

    let beat = store
        .heartbeat("w1", &[claim.clone()], LEASE)
        .await
        .unwrap();
    assert_eq!(beat.cancel_requested, vec![job.id]);

    let cancelled = store.acknowledge_cancel(&claim).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(cancelled.failures, 0);
    assert_eq!(
        outcomes(&store, job.id).await,
        vec![Some(AttemptOutcome::Cancelled)]
    );

    // Too late for a result now.
    assert!(matches!(
        store.complete(&claim, &json!("late")).await,
        Err(StoreError::StaleClaim {
            current_state: JobState::Cancelled,
            ..
        })
    ));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn a_result_committed_before_the_acknowledgement_wins(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store.insert(&new_job(|_| {})).await.unwrap();
    let claim = claim_one(&store, "w1").await;
    store.request_cancel(job.id).await.unwrap();

    // The handler finished before the worker noticed the request.
    let done = store.complete(&claim, &json!("made it")).await.unwrap();
    assert_eq!(done.state, JobState::Succeeded);

    assert!(matches!(
        store.acknowledge_cancel(&claim).await,
        Err(StoreError::StaleClaim {
            current_state: JobState::Succeeded,
            ..
        })
    ));
    assert_eq!(
        store.request_cancel(job.id).await.unwrap(),
        CancelOutcome::AlreadyFinished(done)
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn a_cancelled_job_is_not_retried_when_its_attempt_fails_or_expires(pool: PgPool) {
    let store = JobStore::new(pool.clone());

    let failing = store.insert(&new_job(|_| {})).await.unwrap();
    let claim = claim_one(&store, "w1").await;
    store.request_cancel(failing.id).await.unwrap();
    let after = store.fail(&claim, "boom", true).await.unwrap();
    assert_eq!(after.state, JobState::Cancelled);

    let abandoned = store.insert(&new_job(|_| {})).await.unwrap();
    claim_one(&store, "w1").await;
    store.request_cancel(abandoned.id).await.unwrap();
    expire_leases(&pool).await;
    let reaped = store.reap_expired(100).await.unwrap();
    assert_eq!((reaped.requeued, reaped.dead, reaped.cancelled), (0, 0, 1));

    for id in [failing.id, abandoned.id] {
        assert_eq!(
            store.get(id).await.unwrap().unwrap().state,
            JobState::Cancelled
        );
        assert_eq!(
            outcomes(&store, id).await,
            vec![Some(AttemptOutcome::Cancelled)]
        );
    }
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn cancel_racing_completion_ends_in_exactly_one_final_state(pool: PgPool) {
    let store = JobStore::new(pool);

    for round in 0..40 {
        let job = store.insert(&new_job(|_| {})).await.unwrap();
        let claim = claim_one(&store, "w1").await;

        let result = json!(round);

        // Race 1: the result against the cancellation request.
        let (completed, cancel) = tokio::join!(
            store.complete(&claim, &result),
            store.request_cancel(job.id),
        );
        // A request alone never stops a running job, so the result always lands.
        let done = completed.unwrap();
        assert_eq!(done.state, JobState::Succeeded);
        assert!(matches!(
            cancel.unwrap(),
            CancelOutcome::Requested(_) | CancelOutcome::AlreadyFinished(_)
        ));

        // Race 2: the result against the worker's acknowledgement.
        let job = store.insert(&new_job(|_| {})).await.unwrap();
        let claim = claim_one(&store, "w1").await;
        store.request_cancel(job.id).await.unwrap();
        let (completed, acknowledged) = tokio::join!(
            store.complete(&claim, &result),
            store.acknowledge_cancel(&claim),
        );
        let winner = match (&completed, &acknowledged) {
            (Ok(job), Err(StoreError::StaleClaim { .. })) => job.state,
            (Err(StoreError::StaleClaim { .. }), Ok(job)) => job.state,
            other => panic!("round {round}: exactly one must win, got {other:?}"),
        };
        assert!(matches!(winner, JobState::Succeeded | JobState::Cancelled));

        // Whoever won, the outcome is final and consistent.
        let settled = store.get(job.id).await.unwrap().unwrap();
        assert_eq!(settled.state, winner);
        assert_eq!(settled.result.is_some(), winner == JobState::Succeeded);
        let expected = if winner == JobState::Succeeded {
            AttemptOutcome::Succeeded
        } else {
            AttemptOutcome::Cancelled
        };
        assert_eq!(outcomes(&store, job.id).await, vec![Some(expected)]);
    }
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn cancel_racing_a_claim_never_leaves_a_cancelled_job_running(pool: PgPool) {
    let store = JobStore::new(pool);

    for round in 0..40 {
        let job = store.insert(&new_job(|_| {})).await.unwrap();
        let queues = queues();
        let (claimed, cancel) = tokio::join!(
            store.claim_next("w1", &queues, LEASE),
            store.request_cancel(job.id),
        );
        let after = store.get(job.id).await.unwrap().unwrap();
        match (claimed.unwrap(), cancel.unwrap()) {
            // Cancel won: the job never started.
            (None, CancelOutcome::Cancelled(_)) => {
                assert_eq!(after.state, JobState::Cancelled);
                assert_eq!(after.attempt, 0);
            }
            // Claim won: the worker holds it and will be told to stop.
            (Some(c), CancelOutcome::Requested(_)) => {
                assert_eq!(after.state, JobState::Running);
                assert!(after.cancel_requested);
                store.acknowledge_cancel(&c.claim).await.unwrap();
            }
            other => panic!("round {round}: inconsistent outcome {other:?}"),
        }
    }
}

// ------------------------------------------------------------------ listing

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn list_filters_and_pages_newest_first(pool: PgPool) {
    let store = JobStore::new(pool);
    let mut ids = Vec::new();
    for i in 0..7 {
        let job = store
            .insert(&new_job(|s| {
                s.kind = if i % 2 == 0 { "echo" } else { "sum" }.to_owned();
                s.queue = Some(if i < 5 { "default" } else { "bulk" }.to_owned());
            }))
            .await
            .unwrap();
        ids.push(job.id);
        // UUIDv7 orders by millisecond; keep the creation order unambiguous.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let claim = claim_one(&store, "w1").await; // oldest default job -> running
    store.complete(&claim, &json!(1)).await.unwrap();

    let all = |limit| ListFilter {
        limit,
        ..Default::default()
    };
    let listed: Vec<JobId> = store
        .list(&all(100))
        .await
        .unwrap()
        .iter()
        .map(|j| j.id)
        .collect();
    let newest_first: Vec<JobId> = ids.iter().rev().copied().collect();
    assert_eq!(listed, newest_first);

    // Pages of three, walking the cursor.
    let mut paged = Vec::new();
    let mut before = None;
    loop {
        let page = store.list(&ListFilter { before, ..all(3) }).await.unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 3);
        before = page.last().map(|j| j.id);
        paged.extend(page.into_iter().map(|j| j.id));
    }
    assert_eq!(paged, newest_first);

    let count = |filter: ListFilter| {
        let store = store.clone();
        async move { store.list(&filter).await.unwrap().len() }
    };
    assert_eq!(
        count(ListFilter {
            state: Some(JobState::Succeeded),
            ..all(100)
        })
        .await,
        1
    );
    assert_eq!(
        count(ListFilter {
            state: Some(JobState::Queued),
            ..all(100)
        })
        .await,
        6
    );
    assert_eq!(
        count(ListFilter {
            queue: Some("bulk".to_owned()),
            ..all(100)
        })
        .await,
        2
    );
    assert_eq!(
        count(ListFilter {
            kind: Some("sum".to_owned()),
            ..all(100)
        })
        .await,
        3
    );
    assert_eq!(
        count(ListFilter {
            kind: Some("sum".to_owned()),
            queue: Some("bulk".to_owned()),
            ..all(100)
        })
        .await,
        1
    );
    assert_eq!(count(all(0)).await, 0);
}
