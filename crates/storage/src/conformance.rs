//! One set of scenarios, run against every `JobStore`.
//!
//! The PostgreSQL store implements the scheduling rules in SQL; the in-memory
//! store implements them with the pure functions of the domain crate. These
//! tests are what keep the two from drifting apart: every scenario runs on
//! both, with the same retry policy and a fixed jitter, and must produce the
//! same states, counters, histories and delays.
//!
//! Time is the one thing the fixtures handle differently. For PostgreSQL,
//! "let 200 ms pass" means sleeping, because the store uses the database
//! clock. For the in-memory store it means advancing a `ManualClock`, which is
//! instant and exactly reproducible.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use taskforge_domain::{
    AttemptOutcome, FixedJitter, IdempotencyKey, JobId, JobState, JobSubmission, ManualClock,
    NewJob, RetryPolicy,
};

use crate::{
    CancelOutcome, Claim, JobStore, MemJobStore, PgJobStore, StoreError, Submitted,
};

const LEASE: Duration = Duration::from_millis(200);
/// Backoff ceilings of 80 ms, then 160 ms, capped at 320 ms.
const POLICY: RetryPolicy = RetryPolicy::new(Duration::from_millis(80), Duration::from_millis(320));
/// Fixed, so every delay is known exactly: half-way through the jitter range.
const JITTER: f64 = 0.5;

trait Fixture {
    type Store: JobStore;
    fn store(&self) -> &Self::Store;
    /// Let `d` pass on the store's clock.
    async fn pass(&self, d: Duration);
}

struct Pg(PgJobStore);

impl Fixture for Pg {
    type Store = PgJobStore;
    fn store(&self) -> &PgJobStore {
        &self.0
    }
    async fn pass(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

struct Mem {
    store: MemJobStore,
    clock: Arc<ManualClock>,
}

impl Fixture for Mem {
    type Store = MemJobStore;
    fn store(&self) -> &MemJobStore {
        &self.store
    }
    async fn pass(&self, d: Duration) {
        self.clock.advance(d);
    }
}

fn pg(pool: PgPool) -> Pg {
    Pg(PgJobStore::new(pool)
        .with_retry_policy(POLICY)
        .with_jitter(Arc::new(FixedJitter(JITTER))))
}

fn mem() -> Mem {
    let clock = Arc::new(ManualClock::default());
    let store = MemJobStore::new(clock.clone(), Arc::new(FixedJitter(JITTER))).with_retry_policy(POLICY);
    Mem { store, clock }
}

fn queues() -> Vec<String> {
    vec!["default".to_owned()]
}

fn job(edit: impl FnOnce(&mut JobSubmission)) -> NewJob {
    let mut s = JobSubmission {
        kind: "echo".to_owned(),
        ..Default::default()
    };
    edit(&mut s);
    NewJob::try_from(s).unwrap()
}

fn from_json(text: &str) -> NewJob {
    NewJob::try_from(serde_json::from_str::<JobSubmission>(text).unwrap()).unwrap()
}

async fn claim_one<S: JobStore>(store: &S, worker: &str) -> Claim {
    store
        .claim_next(worker, &queues(), LEASE)
        .await
        .unwrap()
        .expect("a job should be claimable")
        .claim
}

async fn outcomes<S: JobStore>(store: &S, id: JobId) -> Vec<Option<AttemptOutcome>> {
    store
        .attempts(id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.outcome)
        .collect()
}

/// PostgreSQL stores microseconds; allow for rounding.
fn assert_close(actual: chrono::Duration, expected: Duration, what: &str) {
    let diff = (actual.num_microseconds().unwrap() - expected.as_micros() as i64).abs();
    assert!(diff <= 2_000, "{what}: got {actual:?}, expected {expected:?}");
}

// ---------------------------------------------------------------- scenarios

async fn claim_order_and_due_time(f: &impl Fixture) {
    let s = f.store();
    let low = s.insert(&job(|_| {})).await.unwrap();
    let high = s.insert(&job(|j| j.priority = Some(10))).await.unwrap();
    let other = s
        .insert(&job(|j| {
            j.queue = Some("other".to_owned());
            j.priority = Some(100);
        }))
        .await
        .unwrap();
    let later = s.insert(&job(|j| j.delay_ms = Some(300))).await.unwrap();

    let first: Vec<JobId> = s
        .claim("w1", &queues(), LEASE, 10)
        .await
        .unwrap()
        .iter()
        .map(|c| c.job.id)
        .collect();
    assert_eq!(first, [high.id, low.id], "priority first; other queue and future job skipped");
    assert!(s.claim("w2", &queues(), LEASE, 10).await.unwrap().is_empty(), "nothing claimed twice");
    assert_eq!(s.get(other.id).await.unwrap().unwrap().state, JobState::Queued);

    f.pass(Duration::from_millis(350)).await;
    let due = s.claim_next("w2", &queues(), LEASE).await.unwrap().unwrap();
    assert_eq!(due.job.id, later.id);
    assert_eq!((due.job.attempt, due.claim.attempt()), (1, 1));
}

async fn failure_backoff_and_dead_letter(f: &impl Fixture) {
    let s = f.store();
    let id = s.insert(&job(|j| j.max_attempts = Some(3))).await.unwrap().id;

    for n in 1..=2 {
        let claim = claim_one(s, "w1").await;
        let after = s.fail(&claim, &format!("boom {n}"), true).await.unwrap();
        assert_eq!(after.state, JobState::Queued);
        assert_eq!(after.failures, n);
        let expected = POLICY.delay(n as u32, JITTER);
        assert_close(after.run_at - after.updated_at, expected, "retry delay");
        assert!(s.claim_next("w1", &queues(), LEASE).await.unwrap().is_none(), "not before the backoff");
        f.pass(expected + Duration::from_millis(30)).await;
    }
    let claim = claim_one(s, "w1").await;
    let dead = s.fail(&claim, "boom 3", true).await.unwrap();
    assert_eq!(dead.state, JobState::Dead);
    assert_eq!(dead.failures, 3);
    assert_eq!(dead.last_error.as_deref(), Some("boom 3"));
    assert!(dead.finished_at.is_some() && dead.lease_owner.is_none());
    assert_eq!(outcomes(s, id).await, vec![Some(AttemptOutcome::Failed); 3]);

    // Non-retryable: straight to dead with budget to spare.
    s.insert(&job(|j| j.max_attempts = Some(10))).await.unwrap();
    let claim = claim_one(s, "w1").await;
    let fatal = s.fail(&claim, "bad input", false).await.unwrap();
    assert_eq!((fatal.state, fatal.failures), (JobState::Dead, 1));
}

async fn lease_expiry_recovers_the_job_and_fences_the_old_worker(f: &impl Fixture) {
    let s = f.store();
    let id = s.insert(&job(|_| {})).await.unwrap().id;
    let zombie = claim_one(s, "w1").await;
    assert_eq!(s.reap_expired(100).await.unwrap().total(), 0, "lease still valid");

    f.pass(LEASE + Duration::from_millis(50)).await;
    let reaped = s.reap_expired(100).await.unwrap();
    assert_eq!((reaped.requeued, reaped.dead, reaped.cancelled), (1, 0, 0));
    let requeued = s.get(id).await.unwrap().unwrap();
    assert_eq!((requeued.state, requeued.failures), (JobState::Queued, 1));
    assert!(requeued.lease_owner.is_none());
    assert_close(
        requeued.run_at - requeued.updated_at,
        POLICY.delay(1, JITTER),
        "lease expiry backoff",
    );

    // The old worker wakes up: every write it attempts is refused.
    for refused in [
        s.complete(&zombie, &json!("late")).await,
        s.fail(&zombie, "late", true).await,
        s.release(&zombie).await,
    ] {
        assert!(matches!(refused, Err(StoreError::StaleClaim { .. })), "{refused:?}");
    }
    let beat = s.heartbeat("w1", &[zombie.clone()], LEASE).await.unwrap();
    assert_eq!(beat.lost, vec![id]);

    f.pass(POLICY.delay(1, JITTER) + Duration::from_millis(30)).await;
    let successor = claim_one(s, "w2").await;
    assert_eq!(successor.attempt(), 2);
    assert!(s.complete(&zombie, &json!("late")).await.is_err(), "still fenced");
    let done = s.complete(&successor, &json!("w2")).await.unwrap();
    assert_eq!(done.result, Some(json!("w2")));
    assert_eq!(
        outcomes(s, id).await,
        [Some(AttemptOutcome::LeaseExpired), Some(AttemptOutcome::Succeeded)]
    );
}

async fn heartbeats_keep_a_lease_alive(f: &impl Fixture) {
    let s = f.store();
    let id = s.insert(&job(|_| {})).await.unwrap().id;
    let claim = claim_one(s, "w1").await;
    for _ in 0..3 {
        f.pass(LEASE / 2).await;
        let beat = s.heartbeat("w1", &[claim.clone()], LEASE).await.unwrap();
        assert!(beat.lost.is_empty() && beat.cancel_requested.is_empty());
    }
    // 1.5 leases have passed since the claim, but it was renewed every half.
    assert_eq!(s.reap_expired(100).await.unwrap().total(), 0);
    assert_eq!(s.get(id).await.unwrap().unwrap().state, JobState::Running);
    // A heartbeat for someone else's claim renews nothing.
    assert_eq!(s.heartbeat("w2", &[claim], LEASE).await.unwrap().lost, vec![id]);
}

async fn cancellation_rules(f: &impl Fixture) {
    let s = f.store();

    // Queued: cancelled at once, never runs.
    let queued = s.insert(&job(|_| {})).await.unwrap();
    let CancelOutcome::Cancelled(c) = s.request_cancel(queued.id).await.unwrap() else {
        panic!("a queued job is cancelled immediately")
    };
    assert_eq!((c.state, c.attempt), (JobState::Cancelled, 0));
    assert!(s.claim_next("w1", &queues(), LEASE).await.unwrap().is_none());

    // Running: requested, seen by the heartbeat, acknowledged by the worker.
    let running = s.insert(&job(|_| {})).await.unwrap();
    let claim = claim_one(s, "w1").await;
    assert!(matches!(s.acknowledge_cancel(&claim).await, Err(StoreError::StaleClaim { .. })), "nothing to acknowledge yet");
    assert!(matches!(s.request_cancel(running.id).await.unwrap(), CancelOutcome::Requested(_)));
    let beat = s.heartbeat("w1", &[claim.clone()], LEASE).await.unwrap();
    assert_eq!(beat.cancel_requested, vec![running.id]);
    let acked = s.acknowledge_cancel(&claim).await.unwrap();
    assert_eq!((acked.state, acked.failures), (JobState::Cancelled, 0));
    assert_eq!(outcomes(s, running.id).await, [Some(AttemptOutcome::Cancelled)]);
    assert!(matches!(s.complete(&claim, &json!(1)).await, Err(StoreError::StaleClaim { .. })));

    // The result lands before the acknowledgement: success is final.
    let racing = s.insert(&job(|_| {})).await.unwrap();
    let claim = claim_one(s, "w1").await;
    s.request_cancel(racing.id).await.unwrap();
    let done = s.complete(&claim, &json!("made it")).await.unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert!(matches!(s.acknowledge_cancel(&claim).await, Err(StoreError::StaleClaim { .. })));
    assert!(matches!(s.request_cancel(racing.id).await.unwrap(), CancelOutcome::AlreadyFinished(_)));

    // A failure after a cancel request is not retried.
    let failing = s.insert(&job(|_| {})).await.unwrap();
    let claim = claim_one(s, "w1").await;
    s.request_cancel(failing.id).await.unwrap();
    assert_eq!(s.fail(&claim, "boom", true).await.unwrap().state, JobState::Cancelled);
    assert_eq!(outcomes(s, failing.id).await, [Some(AttemptOutcome::Cancelled)]);
}

async fn idempotent_submission(f: &impl Fixture) {
    let s = f.store();
    let key = || IdempotencyKey::parse("order-1").unwrap();

    let Submitted::Created(original) =
        s.submit(&job(|j| j.payload = Some(json!({"amount": 5}))).with_idempotency_key(key())).await.unwrap()
    else {
        panic!("first submission creates")
    };
    let again = s
        .submit(&job(|j| j.payload = Some(json!({"amount": 5}))).with_idempotency_key(key()))
        .await
        .unwrap();
    assert_eq!(again, Submitted::Duplicate(original.clone()));

    let other = s
        .submit(&job(|j| j.payload = Some(json!({"amount": 6}))).with_idempotency_key(key()))
        .await;
    assert!(matches!(other, Err(StoreError::IdempotencyConflict { .. })));

    // Regression test: a retry whose payload the database normalises
    // (1e2 is stored and returned as 100) is still the same request.
    let sci = || from_json(r#"{"kind":"echo","payload":{"n":1e2}}"#)
        .with_idempotency_key(IdempotencyKey::parse("sci").unwrap());
    let first = s.submit(&sci()).await.unwrap();
    let retry = s.submit(&sci()).await.unwrap();
    assert!(!first.is_duplicate());
    assert!(retry.is_duplicate(), "a genuine retry must not be called a conflict");
    assert_eq!(retry.job().id, first.job().id);
}

async fn release_and_redrive(f: &impl Fixture) {
    let s = f.store();
    let id = s.insert(&job(|j| j.max_attempts = Some(1))).await.unwrap().id;

    let claim = claim_one(s, "w1").await;
    let released = s.release(&claim).await.unwrap();
    assert_eq!((released.state, released.failures), (JobState::Queued, 0), "a release is not a failure");

    let claim = claim_one(s, "w2").await;
    assert_eq!(claim.attempt(), 2, "the fencing token still moved on");
    assert_eq!(s.fail(&claim, "boom", true).await.unwrap().state, JobState::Dead);

    let revived = s.redrive(id).await.unwrap();
    assert_eq!((revived.state, revived.failures), (JobState::Queued, 0));
    assert!(revived.finished_at.is_none());
    assert!(matches!(
        s.redrive(id).await,
        Err(StoreError::InvalidState { state: JobState::Queued, .. })
    ));
    assert_eq!(
        outcomes(s, id).await,
        [Some(AttemptOutcome::Released), Some(AttemptOutcome::Failed)]
    );
}

async fn purge_and_stats(f: &impl Fixture) {
    let s = f.store();
    let key = || IdempotencyKey::parse("purge-me").unwrap();
    let done = s.insert(&job(|_| {}).with_idempotency_key(key())).await.unwrap();
    let claim = claim_one(s, "w1").await;
    s.complete(&claim, &json!(1)).await.unwrap();
    let waiting = s.insert(&job(|_| {})).await.unwrap();
    let held = s.insert(&job(|j| j.queue = Some("slow".to_owned()))).await.unwrap();
    s.claim("w2", &["slow".to_owned()], LEASE, 1).await.unwrap();

    f.pass(LEASE + Duration::from_millis(100)).await;
    let stats = s.stats().await.unwrap();
    let count = |q: &str, st: JobState| {
        stats.counts.iter().find(|(qq, ss, _)| qq == q && *ss == st).map_or(0, |c| c.2)
    };
    assert_eq!(count("default", JobState::Succeeded), 1);
    assert_eq!(count("default", JobState::Queued), 1);
    assert_eq!(count("slow", JobState::Running), 1);
    assert_eq!(stats.expired_leases, 1, "the slow job's lease ran out and nobody reaped it");
    assert!(stats.oldest_due_secs >= 0.25, "{}", stats.oldest_due_secs);

    // Only finished jobs older than the cutoff go; their keys become free.
    assert_eq!(s.purge_finished(Duration::from_millis(200), 100).await.unwrap(), 1);
    assert!(s.get(done.id).await.unwrap().is_none());
    assert!(s.attempts(done.id).await.unwrap().is_empty());
    assert!(s.get(waiting.id).await.unwrap().is_some());
    assert!(s.get(held.id).await.unwrap().is_some());
    let reused = s.submit(&job(|_| {}).with_idempotency_key(key())).await.unwrap();
    assert!(!reused.is_duplicate());
}

macro_rules! conformance {
    ($($name:ident),* $(,)?) => {
        mod postgres {
            $(
                #[sqlx::test(migrator = "crate::MIGRATOR")]
                async fn $name(pool: sqlx::PgPool) {
                    super::$name(&super::pg(pool)).await;
                }
            )*
        }
        mod memory {
            $(
                #[tokio::test]
                async fn $name() {
                    super::$name(&super::mem()).await;
                }
            )*
        }
    };
}

conformance!(
    claim_order_and_due_time,
    failure_backoff_and_dead_letter,
    lease_expiry_recovers_the_job_and_fences_the_old_worker,
    heartbeats_keep_a_lease_alive,
    cancellation_rules,
    idempotent_submission,
    release_and_redrive,
    purge_and_stats,
);
