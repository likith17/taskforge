//! Integration tests against a real PostgreSQL.
//!
//! `#[sqlx::test]` gives every test its own freshly migrated database, so
//! tests are isolated and run in parallel. Needs `DATABASE_URL` to point at a
//! server where the role may create databases (`docker compose up -d db`).

use std::collections::HashSet;
use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use taskforge_domain::{JobId, JobState, JobSubmission, NewJob};

use crate::{Claim, JobStore, StoreError};

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

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn insert_then_get_round_trips(pool: PgPool) {
    let store = JobStore::new(pool);
    let new = new_job(|s| {
        s.payload = Some(json!({"to": "a@example.com", "n": 3}));
        s.priority = Some(7);
        s.queue = Some("mail".to_owned());
    });

    let inserted = store.insert(&new).await.unwrap();
    assert_eq!(inserted.id, new.id());
    assert_eq!(inserted.state, JobState::Queued);
    assert_eq!(inserted.attempt, 0);
    assert_eq!(inserted.queue, "mail");
    assert_eq!(inserted.priority, 7);
    assert_eq!(inserted.payload, json!({"to": "a@example.com", "n": 3}));
    assert!(inserted.lease_owner.is_none() && inserted.result.is_none());
    assert!(inserted.started_at.is_none() && inserted.finished_at.is_none());

    assert_eq!(store.get(new.id()).await.unwrap(), Some(inserted));
    assert_eq!(store.get(JobId::new()).await.unwrap(), None);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn delayed_job_is_not_claimable_before_run_at(pool: PgPool) {
    let store = JobStore::new(pool);
    let later = store
        .insert(&new_job(|s| s.delay_ms = Some(3_600_000)))
        .await
        .unwrap();
    assert!(later.run_at > later.created_at + chrono::Duration::minutes(59));
    assert!(
        store
            .claim_next("w1", &queues(), LEASE)
            .await
            .unwrap()
            .is_none()
    );

    // An absolute time in the past means "now".
    let past = chrono::Utc::now() - chrono::Duration::hours(1);
    let due = store
        .insert(&new_job(|s| s.run_at = Some(past)))
        .await
        .unwrap();
    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job.id, due.id);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn claims_follow_priority_then_age_within_requested_queues(pool: PgPool) {
    let store = JobStore::new(pool);
    let old_low = store.insert(&new_job(|_| {})).await.unwrap();
    let new_low = store.insert(&new_job(|_| {})).await.unwrap();
    let high = store
        .insert(&new_job(|s| s.priority = Some(10)))
        .await
        .unwrap();
    let other = store
        .insert(&new_job(|s| {
            s.queue = Some("other".to_owned());
            s.priority = Some(100);
        }))
        .await
        .unwrap();

    let mut order = Vec::new();
    while let Some(claimed) = store.claim_next("w1", &queues(), LEASE).await.unwrap() {
        order.push(claimed.job.id);
    }
    assert_eq!(order, vec![high.id, old_low.id, new_low.id]);

    // The job on the other queue was never touched.
    assert_eq!(
        store.get(other.id).await.unwrap().unwrap().state,
        JobState::Queued
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn claim_marks_running_and_issues_a_lease_and_fencing_token(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store.insert(&new_job(|_| {})).await.unwrap();

    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job.id, job.id);
    assert_eq!(claimed.job.state, JobState::Running);
    assert_eq!(claimed.job.attempt, 1);
    assert_eq!(claimed.job.lease_owner.as_deref(), Some("w1"));
    let expires = claimed.job.lease_expires_at.unwrap();
    let started = claimed.job.started_at.unwrap();
    assert_eq!((expires - started).num_seconds(), 60);

    assert_eq!(claimed.claim.job_id(), job.id);
    assert_eq!(claimed.claim.worker_id(), "w1");
    assert_eq!(claimed.claim.attempt(), 1);

    // A running job is not handed out again.
    assert!(
        store
            .claim_next("w2", &queues(), LEASE)
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn concurrent_claimers_never_receive_the_same_job(pool: PgPool) {
    const JOBS: usize = 60;
    const WORKERS: usize = 12;

    let store = JobStore::new(pool);
    for _ in 0..JOBS {
        store.insert(&new_job(|_| {})).await.unwrap();
    }

    let mut tasks = Vec::new();
    for w in 0..WORKERS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let worker_id = format!("w{w}");
            let mut mine = Vec::new();
            while let Some(claimed) = store
                .claim_next(&worker_id, &queues(), LEASE)
                .await
                .unwrap()
            {
                mine.push(claimed.job.id);
                tokio::task::yield_now().await;
            }
            mine
        }));
    }

    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    assert_eq!(all.len(), JOBS, "every job claimed, none twice");
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), JOBS);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn complete_commits_the_result_and_releases_the_lease(pool: PgPool) {
    let store = JobStore::new(pool);
    store.insert(&new_job(|_| {})).await.unwrap();
    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();

    let done = store
        .complete(&claimed.claim, &json!({"answer": 42}))
        .await
        .unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert_eq!(done.result, Some(json!({"answer": 42})));
    assert!(done.lease_owner.is_none() && done.lease_expires_at.is_none());
    assert!(done.finished_at.is_some());
    assert_eq!(store.get(done.id).await.unwrap(), Some(done));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn fail_dead_letters_the_job_with_a_bounded_error(pool: PgPool) {
    let store = JobStore::new(pool);
    store.insert(&new_job(|_| {})).await.unwrap();
    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();

    let long_error = "é".repeat(5000); // 10 000 bytes
    let dead = store
        .fail(&claimed.claim, &long_error, false)
        .await
        .unwrap();
    assert_eq!(dead.state, JobState::Dead);
    assert!(dead.result.is_none() && dead.lease_owner.is_none());
    let stored = dead.last_error.unwrap();
    assert_eq!(stored.len(), 4096);
    assert!(long_error.starts_with(&stored));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn stale_or_foreign_claims_cannot_settle_a_job(pool: PgPool) {
    let store = JobStore::new(pool);
    let job = store.insert(&new_job(|_| {})).await.unwrap();
    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();
    let running = store.get(job.id).await.unwrap().unwrap();

    let forged = [
        // Right attempt, wrong worker.
        Claim {
            job_id: job.id,
            worker_id: "intruder".to_owned(),
            attempt: 1,
        },
        // Right worker, an attempt it was never given.
        Claim {
            job_id: job.id,
            worker_id: "w1".to_owned(),
            attempt: 2,
        },
        Claim {
            job_id: job.id,
            worker_id: "w1".to_owned(),
            attempt: 0,
        },
    ];
    for claim in &forged {
        for outcome in [
            store.complete(claim, &json!("stolen")).await,
            store.fail(claim, "stolen", true).await,
        ] {
            match outcome {
                Err(StoreError::StaleClaim {
                    current_state,
                    current_attempt,
                    ..
                }) => {
                    assert_eq!(current_state, JobState::Running);
                    assert_eq!(current_attempt, 1);
                }
                other => panic!("expected StaleClaim, got {other:?}"),
            }
        }
    }
    // None of that changed the row, and the rightful holder can still finish.
    assert_eq!(store.get(job.id).await.unwrap().unwrap(), running);
    store
        .complete(&claimed.claim, &json!("mine"))
        .await
        .unwrap();
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn a_settled_job_cannot_be_settled_again(pool: PgPool) {
    let store = JobStore::new(pool);
    store.insert(&new_job(|_| {})).await.unwrap();
    let claimed = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();
    let done = store.complete(&claimed.claim, &json!(1)).await.unwrap();

    // A late failure report must not overwrite the committed success.
    let late = store.fail(&claimed.claim, "too late", true).await;
    assert!(matches!(
        late,
        Err(StoreError::StaleClaim {
            current_state: JobState::Succeeded,
            ..
        })
    ));
    assert!(matches!(
        store.complete(&claimed.claim, &json!(2)).await,
        Err(StoreError::StaleClaim { .. })
    ));
    assert_eq!(store.get(done.id).await.unwrap(), Some(done));
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn settling_a_missing_job_is_not_found(pool: PgPool) {
    let store = JobStore::new(pool);
    let ghost = Claim {
        job_id: JobId::new(),
        worker_id: "w1".to_owned(),
        attempt: 1,
    };
    assert!(matches!(
        store.complete(&ghost, &json!(null)).await,
        Err(StoreError::NotFound(id)) if id == ghost.job_id
    ));
}

/// Raw UPDATE that moves a row to `state`, keeping the lease/finished columns
/// consistent with the table's CHECK constraints so that only the transition
/// trigger can object.
async fn force_state(pool: &PgPool, id: JobId, state: JobState) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET state = $2,
            lease_owner      = CASE WHEN $2 = 'running' THEN 'test' END,
            lease_expires_at = CASE WHEN $2 = 'running' THEN now() END,
            finished_at      = CASE WHEN $2 IN ('succeeded', 'dead', 'cancelled') THEN now() END
        WHERE id = $1
        "#,
    )
    .bind(id.as_uuid())
    .bind(state.as_str())
    .execute(pool)
    .await
    .map(|_| ())
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn database_trigger_enforces_exactly_the_domain_state_machine(pool: PgPool) {
    use JobState::*;
    let store = JobStore::new(pool.clone());

    for from in JobState::ALL {
        // A legal path from `queued` to the starting state.
        let path: &[JobState] = match from {
            Queued => &[],
            Running => &[Running],
            Succeeded => &[Running, Succeeded],
            Dead => &[Running, Dead],
            Cancelled => &[Cancelled],
        };
        for to in JobState::ALL {
            if from == to {
                continue;
            }
            let job = store.insert(&new_job(|_| {})).await.unwrap();
            for &step in path {
                force_state(&pool, job.id, step).await.unwrap();
            }

            let outcome = force_state(&pool, job.id, to).await;
            assert_eq!(
                outcome.is_ok(),
                from.can_transition_to(to),
                "{from} -> {to}: database said {outcome:?}"
            );
            let expected = if from.can_transition_to(to) { to } else { from };
            assert_eq!(store.get(job.id).await.unwrap().unwrap().state, expected);
        }
    }
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn database_rejects_inconsistent_rows(pool: PgPool) {
    let store = JobStore::new(pool.clone());
    let job = store.insert(&new_job(|_| {})).await.unwrap();
    let id = job.id.as_uuid();

    // Born in a state other than queued.
    let born_running = sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, lease_owner, lease_expires_at)
         VALUES ($1, 'default', 'echo', '{}', 'running', 'w', now())",
    )
    .bind(JobId::new().as_uuid())
    .execute(&pool)
    .await;
    assert!(born_running.is_err());

    // Running without a lease.
    let leaseless = sqlx::query("UPDATE jobs SET state = 'running' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(leaseless.is_err());

    // A result on a job that has not succeeded.
    let early_result = sqlx::query("UPDATE jobs SET result = '1' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(early_result.is_err());

    // The fencing token moving backwards.
    store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap();
    let rewind = sqlx::query("UPDATE jobs SET attempt = 0 WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(rewind.is_err());

    assert_eq!(store.get(job.id).await.unwrap().unwrap().attempt, 1);
}
