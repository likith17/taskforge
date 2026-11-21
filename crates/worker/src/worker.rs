use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde_json::Value;
use taskforge_domain::{JobId, JobState};
use taskforge_metrics::{JobOutcome, Metrics};
use taskforge_storage::{Claim, ClaimedJob, JobStore, StoreError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::Instrument;

use crate::{HandlerRegistry, JobContext, LocalTime, TokioTime};

/// The worker stops a handler once this fraction of the lease has passed
/// without a successful renewal. See [`Inner::execute`] for why it is less
/// than one.
const SELF_FENCE_AT: f64 = 0.9;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Must be unique per worker process incarnation: it is half of the
    /// fencing token.
    pub worker_id: String,
    pub queues: Vec<String>,
    /// How long to wait before looking again when the queues are empty.
    pub poll_interval: Duration,
    /// How long a claim stays valid without renewal. The worker renews every
    /// third of this, so a job survives two missed heartbeats.
    pub lease: Duration,
    /// Maximum number of jobs run at once. The worker never claims more than
    /// it has free slots for: this is its backpressure.
    pub concurrency: usize,
    /// On shutdown, how long running jobs get to finish before they are
    /// stopped and handed back to the queue.
    pub shutdown_grace: Duration,
}

/// What happened to one claimed job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Succeeded(JobId),
    /// The attempt failed and the job was re-queued with backoff.
    Retrying(JobId),
    /// The attempt failed and the job was dead-lettered.
    Dead(JobId),
    /// Cancellation was requested and the job ended `cancelled`.
    Cancelled(JobId),
    /// Handed back unfinished during shutdown.
    Released(JobId),
    /// The job was no longer ours; our work was discarded.
    LostClaim(JobId),
    /// The outcome could not be written. The lease will expire and the job
    /// will run again (at-least-once).
    Unreported(JobId),
}

impl Outcome {
    fn metric(self) -> JobOutcome {
        match self {
            Outcome::Succeeded(_) => JobOutcome::Succeeded,
            Outcome::Retrying(_) => JobOutcome::Retrying,
            Outcome::Dead(_) => JobOutcome::DeadLettered,
            Outcome::Cancelled(_) => JobOutcome::Cancelled,
            Outcome::Released(_) => JobOutcome::Released,
            Outcome::LostClaim(_) => JobOutcome::LostClaim,
            Outcome::Unreported(_) => JobOutcome::Unreported,
        }
    }
}

/// Why a running handler is being stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    Cancelled,
    LeaseLost,
    Shutdown,
}

enum Verdict {
    Done(Value),
    Failed { error: String, retryable: bool },
    Stopped(Stop),
}

/// A job this worker currently holds.
struct Active {
    claim: Claim,
    stop: watch::Sender<Option<Stop>>,
    /// Local time until which the lease is *known* to be valid, minus a
    /// safety margin. Moved forward by every successful renewal.
    valid_until: watch::Sender<Instant>,
}

impl Active {
    fn stop(&self, reason: Stop) {
        // First reason wins; later ones are ignored.
        self.stop.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(reason);
                true
            } else {
                false
            }
        });
    }
}

struct Inner<S> {
    store: S,
    /// Heartbeats get their own store (normally a separate, small pool), so
    /// that claims and settles saturating the main pool cannot delay lease
    /// renewal. A starved heartbeat means healthy jobs lose their leases and
    /// run twice.
    heartbeat_store: S,
    registry: HandlerRegistry,
    config: WorkerConfig,
    metrics: Arc<Metrics>,
    /// Every deadline, sleep and tick of this worker is measured on this
    /// clock. See [`LocalTime`].
    time: Arc<dyn LocalTime>,
    active: Mutex<BTreeMap<JobId, Active>>,
}

pub struct Worker<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Worker<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: JobStore> Worker<S> {
    pub fn new(store: S, registry: HandlerRegistry, config: WorkerConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                heartbeat_store: store.clone(),
                store,
                registry,
                config,
                metrics: Metrics::shared(),
                time: Arc::new(TokioTime),
                active: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    fn configure(&mut self) -> &mut Inner<S> {
        Arc::get_mut(&mut self.inner).expect("configure a worker before cloning or running it")
    }

    /// Send lease renewals through `store` instead of the main one.
    pub fn with_heartbeat_store(mut self, store: S) -> Self {
        self.configure().heartbeat_store = store;
        self
    }

    /// Record into a shared metrics registry (normally the one `/metrics`
    /// serves).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.configure().metrics = metrics;
        self
    }

    /// Measure this worker's deadlines, sleeps and heartbeat ticks on `time`
    /// instead of Tokio's clock. For simulations that give each worker a
    /// timer running at its own rate.
    pub fn with_local_time(mut self, time: Arc<dyn LocalTime>) -> Self {
        self.configure().time = time;
        self
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }

    pub fn id(&self) -> &str {
        &self.inner.config.worker_id
    }

    /// Number of jobs currently held.
    pub fn in_flight(&self) -> usize {
        self.inner.active().len()
    }

    /// Run until `shutdown` becomes `true` (or its sender is dropped), then
    /// drain: running jobs get `shutdown_grace` to finish, the rest are
    /// stopped and released back to the queue.
    ///
    /// Dropping (aborting) this future instead is an abrupt stop: handlers are
    /// aborted and nothing is reported, exactly as if the process had been
    /// killed. The jobs are recovered when their leases expire.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let inner = &self.inner;
        let config = &inner.config;
        tracing::info!(
            worker_id = %config.worker_id,
            queues = ?config.queues,
            concurrency = config.concurrency,
            kinds = ?inner.registry,
            "worker started"
        );

        let _heartbeats = AbortOnDrop(tokio::spawn(heartbeat_loop(inner.clone())));
        let slots = Arc::new(Semaphore::new(config.concurrency.max(1)));
        let mut jobs: JoinSet<Outcome> = JoinSet::new();
        let mut consecutive_errors: u32 = 0;

        while !*shutdown.borrow() {
            while jobs.try_join_next().is_some() {}

            // Backpressure: wait for a free slot before asking for work, and
            // ask for no more jobs than there are free slots.
            let first = tokio::select! {
                permit = slots.clone().acquire_owned() => permit.expect("semaphore is never closed"),
                _ = stop_requested(&mut shutdown) => break,
            };
            let mut permits = vec![first];
            while let Ok(permit) = slots.clone().try_acquire_owned() {
                permits.push(permit);
            }

            let sent_at = inner.time.now();
            let claimed = inner
                .store
                .claim(
                    &config.worker_id,
                    &config.queues,
                    config.lease,
                    permits.len() as u32,
                )
                .await;
            inner.metrics.claim_took(
                inner
                    .time
                    .now()
                    .saturating_duration_since(sent_at)
                    .as_secs_f64(),
            );
            let pause = match claimed {
                Ok(claimed) if !claimed.is_empty() => {
                    consecutive_errors = 0;
                    for (job, permit) in claimed.into_iter().zip(permits.drain(..)) {
                        jobs.spawn(inner.clone().run_job(job, sent_at, Some(permit)));
                    }
                    // There may be more work: look again as soon as a slot is free.
                    continue;
                }
                Ok(_) => {
                    consecutive_errors = 0;
                    config.poll_interval
                }
                // Typically the database being away. Keep the process alive
                // and back off, so it recovers by itself when the database
                // returns.
                Err(err) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    let pause = error_backoff(config.poll_interval, consecutive_errors);
                    tracing::warn!(error = %err, retry_in = ?pause, "could not claim jobs");
                    pause
                }
            };
            drop(permits);
            tokio::select! {
                _ = inner.time.sleep(pause) => {}
                _ = stop_requested(&mut shutdown) => break,
            }
        }

        self.drain(&mut jobs).await;
        tracing::info!(worker_id = %config.worker_id, "worker stopped");
    }

    async fn drain(&self, jobs: &mut JoinSet<Outcome>) {
        if jobs.is_empty() {
            return;
        }
        let grace = self.inner.config.shutdown_grace;
        tracing::info!(
            in_flight = jobs.len(),
            ?grace,
            "draining: waiting for running jobs"
        );
        let time = &self.inner.time;
        let finished_in_time =
            tokio::time::timeout_at(time.runtime_instant(time.deadline_in(grace)), async {
                while jobs.join_next().await.is_some() {}
            })
            .await
            .is_ok();
        if finished_in_time {
            return;
        }

        tracing::warn!(
            in_flight = jobs.len(),
            "grace period over: releasing unfinished jobs"
        );
        for active in self.inner.active().values() {
            active.stop(Stop::Shutdown);
        }
        // Stopping and releasing is quick; bound it anyway so a dead database
        // cannot hold the process hostage. Anything left is recovered by lease
        // expiry.
        let _ = tokio::time::timeout_at(
            time.runtime_instant(time.deadline_in(Duration::from_secs(10))),
            async { while jobs.join_next().await.is_some() {} },
        )
        .await;
    }

    /// Claim and execute at most one job, to completion, on the calling task.
    ///
    /// For tests and one-shot tools. No heartbeat runs, so the job must finish
    /// within one lease. Returns `Ok(None)` when nothing was runnable.
    pub async fn run_once(&self) -> Result<Option<Outcome>, StoreError> {
        let config = &self.inner.config;
        let sent_at = self.inner.time.now();
        let Some(job) = self
            .inner
            .store
            .claim_next(&config.worker_id, &config.queues, config.lease)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(self.inner.clone().run_job(job, sent_at, None).await))
    }
}

impl<S: JobStore> Inner<S> {
    fn active(&self) -> MutexGuard<'_, BTreeMap<JobId, Active>> {
        // A panic while holding the lock cannot leave the map inconsistent
        // (every critical section is a single insert/remove/read), so a
        // poisoned lock is safe to keep using.
        self.active.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How long after sending a claim or renewal we may keep working.
    fn fence_window(&self) -> Duration {
        self.config.lease.mul_f64(SELF_FENCE_AT)
    }

    async fn run_job(
        self: Arc<Self>,
        claimed: ClaimedJob,
        claim_sent_at: Instant,
        _slot: Option<OwnedSemaphorePermit>,
    ) -> Outcome {
        let ClaimedJob { job, claim } = claimed;
        let span =
            tracing::info_span!("job", job_id = %job.id, kind = %job.kind, attempt = job.attempt);
        async move {
            tracing::info!("job claimed");
            if let Some(started) = job.started_at {
                // Both instants come from the database clock.
                let waited = (started - job.run_at).num_milliseconds() as f64 / 1000.0;
                self.metrics.queue_waited(waited);
            }

            let (stop_tx, stop_rx) = watch::channel(None);
            let (valid_tx, valid_rx) = watch::channel(claim_sent_at + self.fence_window());
            self.active().insert(
                job.id,
                Active {
                    claim: claim.clone(),
                    stop: stop_tx,
                    valid_until: valid_tx,
                },
            );
            // Removes the entry however this task ends, including by abort.
            let _registered = Registered {
                active: &self.active,
                job_id: job.id,
            };

            let timeout = Duration::from_secs(u64::try_from(job.timeout_secs.max(1)).unwrap_or(1));
            let ctx = JobContext {
                job_id: job.id,
                kind: job.kind,
                queue: job.queue,
                attempt: job.attempt,
                worker_id: claim.worker_id().to_owned(),
            };
            let started = self.time.now();
            let verdict = self
                .execute(ctx, job.payload, timeout, stop_rx, valid_rx)
                .await;
            self.metrics.executed_for(
                self.time
                    .now()
                    .saturating_duration_since(started)
                    .as_secs_f64(),
            );
            // From here on stop signals are ignored: the handler is finished
            // and only the outcome remains to be written. Heartbeats keep the
            // lease alive meanwhile, and the write is fenced regardless.
            let outcome = self.settle(&claim, verdict).await;
            self.metrics.job_outcome(outcome.metric());
            outcome
        }
        .instrument(span)
        .await
    }

    /// Run the handler until it finishes, times out, or must be stopped.
    ///
    /// Self-fencing: the worker stops the handler when `valid_until` passes
    /// without a renewal. That instant is measured from when the claim or
    /// renewal was *sent*, which is no later than when the database wrote the
    /// lease, so the local deadline falls before the database's - and only
    /// after the database's deadline can the reaper hand the job to another
    /// worker. Stopping at 90% of the lease leaves room for clock-rate drift
    /// and for the abort to take effect. A dedicated timer per job (rather
    /// than a check on each heartbeat tick) means the worker reacts at the
    /// deadline itself, not up to a third of a lease later.
    async fn execute(
        &self,
        ctx: JobContext,
        payload: Value,
        timeout: Duration,
        mut stop: watch::Receiver<Option<Stop>>,
        mut valid_until: watch::Receiver<Instant>,
    ) -> Verdict {
        let Some(handler) = self.registry.get(&ctx.kind) else {
            return Verdict::Failed {
                error: format!("no handler registered for job kind {:?}", ctx.kind),
                retryable: false,
            };
        };
        // A separate task, so a panicking handler takes down neither this
        // worker nor the process. Aborted if we are dropped.
        let mut task = AbortOnDrop(tokio::spawn(handler.run(ctx, payload)));
        let timeout_at = self.time.now() + timeout;
        let mut renewals_open = true;

        loop {
            let deadline = *valid_until.borrow_and_update();
            let stopped = tokio::select! {
                // A finished handler wins every tie: its result is still
                // worth offering, and the settle is fenced anyway.
                biased;
                joined = &mut task.0 => return match joined {
                    Ok(Ok(result)) => Verdict::Done(result),
                    Ok(Err(error)) => Verdict::Failed {
                        error: error.message().to_owned(),
                        retryable: error.is_retryable(),
                    },
                    Err(join_error) if join_error.is_panic() => Verdict::Failed {
                        error: "handler panicked".to_owned(),
                        retryable: true,
                    },
                    Err(_) => Verdict::Failed {
                        error: "handler task was cancelled".to_owned(),
                        retryable: true,
                    },
                },
                reason = stop_signal(&mut stop) => Verdict::Stopped(reason),
                () = self.time.sleep_until(deadline) => {
                    tracing::warn!("lease could not be renewed in time; stopping the handler");
                    Verdict::Stopped(Stop::LeaseLost)
                }
                () = self.time.sleep_until(timeout_at) => Verdict::Failed {
                    error: format!("timed out after {}s", timeout.as_secs()),
                    retryable: true,
                },
                changed = valid_until.changed(), if renewals_open => {
                    // A renewal moved the deadline; wait on the new one.
                    renewals_open = changed.is_ok();
                    continue;
                }
            };
            // Abort takes effect at the handler's next await point. Wait for
            // it, so the handler is really gone before anything is reported.
            task.0.abort();
            let _ = (&mut task.0).await;
            return stopped;
        }
    }

    /// Write the outcome of an attempt.
    async fn settle(&self, claim: &Claim, verdict: Verdict) -> Outcome {
        let id = claim.job_id();
        // Keep trying through a database outage for as long as the lease could
        // plausibly still be ours; after that the reaper owns the job.
        let time = &*self.time;
        let deadline = time.now() + self.config.lease;
        let store = &self.store;

        let written = match &verdict {
            Verdict::Done(result) => {
                retry_unavailable(time, deadline, || store.complete(claim, result)).await
            }
            Verdict::Failed { error, retryable } => {
                retry_unavailable(time, deadline, || store.fail(claim, error, *retryable)).await
            }
            Verdict::Stopped(Stop::Cancelled) => {
                retry_unavailable(time, deadline, || store.acknowledge_cancel(claim)).await
            }
            Verdict::Stopped(Stop::Shutdown) => {
                retry_unavailable(time, deadline, || store.release(claim)).await
            }
            Verdict::Stopped(Stop::LeaseLost) => {
                // Not ours any more: there is nothing we are allowed to write.
                tracing::warn!("lease lost; handler stopped and its work discarded");
                return Outcome::LostClaim(id);
            }
        };

        match written {
            Ok(job) => match (&verdict, job.state) {
                (Verdict::Done(_), _) => {
                    tracing::info!("job succeeded");
                    Outcome::Succeeded(id)
                }
                (Verdict::Stopped(_), JobState::Queued) => {
                    tracing::info!("job released back to the queue");
                    Outcome::Released(id)
                }
                (Verdict::Failed { error, .. }, JobState::Queued) => {
                    tracing::warn!(error, failures = job.failures, retry_at = %job.run_at, "job failed; will retry");
                    Outcome::Retrying(id)
                }
                (Verdict::Failed { error, .. }, JobState::Dead) => {
                    tracing::error!(
                        error,
                        failures = job.failures,
                        "job failed and was dead-lettered"
                    );
                    Outcome::Dead(id)
                }
                _ => {
                    tracing::info!("job cancelled");
                    Outcome::Cancelled(id)
                }
            },
            Err(StoreError::StaleClaim {
                current_state,
                current_attempt,
                ..
            }) => {
                tracing::warn!(%current_state, current_attempt, "claim was lost; outcome discarded");
                Outcome::LostClaim(id)
            }
            Err(err) => {
                tracing::error!(error = %err, "could not record the outcome; the job will be recovered by lease expiry");
                Outcome::Unreported(id)
            }
        }
    }
}

/// Renew every held lease each `lease / 3`, and act on what comes back.
async fn heartbeat_loop<S: JobStore>(inner: Arc<Inner<S>>) {
    let lease = inner.config.lease;
    let mut ticker = inner
        .time
        .interval((lease / 3).max(Duration::from_millis(10)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick is immediate; a fresh claim needs no renewal

    loop {
        ticker.tick().await;
        let claims: Vec<Claim> = inner.active().values().map(|a| a.claim.clone()).collect();
        if claims.is_empty() {
            continue;
        }
        let sent_at = inner.time.now();
        match inner
            .heartbeat_store
            .heartbeat(&inner.config.worker_id, &claims, lease)
            .await
        {
            Ok(beat) => {
                let active = inner.active();
                for claim in &claims {
                    let id = claim.job_id();
                    // The job may have finished while the heartbeat was in flight.
                    let Some(entry) = active.get(&id).filter(|e| e.claim == *claim) else {
                        continue;
                    };
                    if beat.lost.contains(&id) {
                        entry.stop(Stop::LeaseLost);
                    } else {
                        entry
                            .valid_until
                            .send_replace(sent_at + inner.fence_window());
                        if beat.cancel_requested.contains(&id) {
                            entry.stop(Stop::Cancelled);
                        }
                    }
                }
            }
            // No renewal: each job's own timer stops it if this goes on.
            Err(err) => {
                inner.metrics.heartbeat_failed();
                tracing::warn!(error = %err, "heartbeat failed");
            }
        }
    }
}

/// Run `op`, repeating it while the database is unreachable, until `deadline`.
///
/// Safe because every settle statement is fenced: if an earlier try did reach
/// the database, the repeat finds the job no longer running under this claim
/// and gets `StaleClaim` rather than applying twice.
async fn retry_unavailable<T, F, Fut>(
    time: &dyn LocalTime,
    deadline: Instant,
    mut op: F,
) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let mut pause = Duration::from_millis(100);
    loop {
        match op().await {
            Err(err) if err.is_unavailable() && time.now() + pause < deadline => {
                tracing::warn!(error = %err, retry_in = ?pause, "database unavailable; retrying");
                time.sleep(pause).await;
                pause = (pause * 2).min(Duration::from_secs(2));
            }
            other => return other,
        }
    }
}

/// Resolves when shutdown has been requested or can no longer be requested.
async fn stop_requested(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

/// Resolves with the reason once a stop is signalled. Never resolves if the
/// sender goes away without signalling.
async fn stop_signal(stop: &mut watch::Receiver<Option<Stop>>) -> Stop {
    // Copy the value out so the channel's read guard is not held across an await.
    let signalled = stop
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|reason| *reason);
    match signalled {
        Some(reason) => reason,
        None => std::future::pending().await,
    }
}

/// `base * 2^(errors-1)`, capped at 10 seconds.
fn error_backoff(base: Duration, consecutive_errors: u32) -> Duration {
    const CAP: Duration = Duration::from_secs(10);
    let factor = 2u32.saturating_pow(consecutive_errors.saturating_sub(1).min(16));
    base.saturating_mul(factor)
        .min(CAP)
        .max(Duration::from_millis(50))
}

/// Aborts the task when dropped, so cancelling a parent cancels its children.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Removes a job from the active set when dropped.
struct Registered<'a> {
    active: &'a Mutex<BTreeMap<JobId, Active>>,
    job_id: JobId,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.job_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    fn unavailable() -> StoreError {
        StoreError::Database(sqlx::Error::PoolTimedOut)
    }

    #[test]
    fn error_backoff_doubles_and_is_capped() {
        let base = Duration::from_millis(250);
        assert_eq!(error_backoff(base, 1), Duration::from_millis(250));
        assert_eq!(error_backoff(base, 2), Duration::from_millis(500));
        assert_eq!(error_backoff(base, 3), Duration::from_secs(1));
        assert_eq!(error_backoff(base, 30), Duration::from_secs(10));
        assert_eq!(error_backoff(base, u32::MAX), Duration::from_secs(10));
        assert_eq!(error_backoff(Duration::ZERO, 1), Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn settle_is_retried_through_a_database_outage() {
        let calls = AtomicU32::new(0);
        let started = Instant::now();
        let result = retry_unavailable(&TokioTime, started + Duration::from_secs(30), || async {
            // Down for the first three tries, then back.
            if calls.fetch_add(1, Ordering::SeqCst) < 3 {
                Err(unavailable())
            } else {
                Ok("recorded")
            }
        })
        .await;
        assert_eq!(result.unwrap(), "recorded");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        // Backed off 100 + 200 + 400 ms between the tries.
        assert_eq!(started.elapsed(), Duration::from_millis(700));
    }

    #[tokio::test(start_paused = true)]
    async fn settle_retry_gives_up_at_the_deadline() {
        let calls = AtomicU32::new(0);
        let started = Instant::now();
        let deadline = started + Duration::from_secs(5);
        let result: Result<(), _> = retry_unavailable(&TokioTime, deadline, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(unavailable())
        })
        .await;
        assert!(result.unwrap_err().is_unavailable());
        assert!(calls.load(Ordering::SeqCst) > 3);
        assert!(
            started.elapsed() <= Duration::from_secs(5),
            "never sleeps past the deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn other_errors_are_not_retried() {
        let calls = AtomicU32::new(0);
        let result: Result<(), _> = retry_unavailable(
            &TokioTime,
            Instant::now() + Duration::from_secs(30),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(StoreError::Corrupt("not a connectivity problem".to_owned()))
            },
        )
        .await;
        assert!(matches!(result, Err(StoreError::Corrupt(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
