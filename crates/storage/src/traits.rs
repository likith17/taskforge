use std::future::Future;
use std::time::Duration;

use serde_json::Value;
use taskforge_domain::{Job, JobAttempt, JobId, NewJob};

use crate::{
    CancelOutcome, Claim, ClaimedJob, Heartbeat, ListFilter, QueueStats, Reaped, StoreError,
    Submitted,
};

/// Everything the scheduler needs from durable storage.
///
/// The API, worker and reaper are written against this trait, not against
/// PostgreSQL, so that a deterministic simulation can run them on an
/// in-memory store with a fake clock and seeded randomness.
///
/// # Contract every implementation must keep
///
/// * Each method is atomic: it happens completely or not at all.
/// * `claim` never hands the same job to two callers.
/// * `complete`, `fail`, `release` and `acknowledge_cancel` apply only while
///   the claim is still the current attempt of a running job; otherwise they
///   change nothing and return [`StoreError::StaleClaim`].
/// * Only transitions allowed by
///   [`JobState::can_transition_to`](taskforge_domain::JobState::can_transition_to)
///   ever happen.
/// * Failures follow [`decide_failure`](taskforge_domain::decide_failure).
///
/// The conformance tests in this crate check both implementations against
/// the same scenarios.
///
/// Methods return `impl Future + Send` rather than being `async fn`, so that
/// generic code can `tokio::spawn` the futures.
pub trait JobStore: Clone + Send + Sync + 'static {
    /// Cheap round trip, for readiness checks.
    fn ping(&self) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Persist a new job as `queued`. With an idempotency key, at most one
    /// job is ever created per key; repeats get [`Submitted::Duplicate`], or
    /// [`StoreError::IdempotencyConflict`] if the request differs.
    fn submit(&self, job: &NewJob)
    -> impl Future<Output = Result<Submitted, StoreError>> + Send;

    fn get(&self, id: JobId) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send;

    /// Newest first, with keyset pagination on the (time-ordered) id.
    fn list(&self, filter: &ListFilter)
    -> impl Future<Output = Result<Vec<Job>, StoreError>> + Send;

    /// Delivery history of a job, oldest attempt first.
    fn attempts(&self, id: JobId)
    -> impl Future<Output = Result<Vec<JobAttempt>, StoreError>> + Send;

    /// Lease up to `limit` due jobs from `queues` to `worker_id`, highest
    /// priority first, then oldest `run_at`.
    fn claim(
        &self,
        worker_id: &str,
        queues: &[String],
        lease: Duration,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<ClaimedJob>, StoreError>> + Send;

    /// Extend the lease of every claim that still holds.
    fn heartbeat(
        &self,
        worker_id: &str,
        claims: &[Claim],
        lease: Duration,
    ) -> impl Future<Output = Result<Heartbeat, StoreError>> + Send;

    /// `running -> succeeded`. Wins over a pending cancellation request: the
    /// first committed outcome decides.
    fn complete(
        &self,
        claim: &Claim,
        result: &Value,
    ) -> impl Future<Output = Result<Job, StoreError>> + Send;

    /// End the attempt in failure: retry, dead-letter or cancel, per
    /// [`decide_failure`](taskforge_domain::decide_failure).
    fn fail(
        &self,
        claim: &Claim,
        error: &str,
        retryable: bool,
    ) -> impl Future<Output = Result<Job, StoreError>> + Send;

    /// Hand the job back unfinished (graceful shutdown). Not a failure.
    fn release(&self, claim: &Claim) -> impl Future<Output = Result<Job, StoreError>> + Send;

    /// The worker stopped the handler because cancellation was requested.
    fn acknowledge_cancel(
        &self,
        claim: &Claim,
    ) -> impl Future<Output = Result<Job, StoreError>> + Send;

    /// Take back up to `limit` jobs whose lease has expired, treating each as
    /// a failed attempt. Safe to run from many processes at once.
    fn reap_expired(&self, limit: u32) -> impl Future<Output = Result<Reaped, StoreError>> + Send;

    /// Cancel a queued job now; flag a running one for its worker.
    fn request_cancel(
        &self,
        id: JobId,
    ) -> impl Future<Output = Result<CancelOutcome, StoreError>> + Send;

    /// `dead -> queued` with a fresh failure budget.
    fn redrive(&self, id: JobId) -> impl Future<Output = Result<Job, StoreError>> + Send;

    /// Delete up to `limit` jobs that finished more than `older_than` ago,
    /// with their history. Their idempotency keys become reusable.
    fn purge_finished(
        &self,
        older_than: Duration,
        limit: u32,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    fn stats(&self) -> impl Future<Output = Result<QueueStats, StoreError>> + Send;

    /// [`JobStore::submit`], for callers that do not care whether the job is new.
    fn insert(&self, job: &NewJob) -> impl Future<Output = Result<Job, StoreError>> + Send {
        async move { self.submit(job).await.map(Submitted::into_job) }
    }

    /// [`JobStore::claim`] for a single job.
    fn claim_next(
        &self,
        worker_id: &str,
        queues: &[String],
        lease: Duration,
    ) -> impl Future<Output = Result<Option<ClaimedJob>, StoreError>> + Send {
        async move { Ok(self.claim(worker_id, queues, lease, 1).await?.pop()) }
    }
}
