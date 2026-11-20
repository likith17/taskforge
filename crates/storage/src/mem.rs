//! An in-memory [`JobStore`] with injected time and randomness.
//!
//! It exists for two reasons:
//!
//! 1. To prove that the scheduling logic is separable from I/O. The worker,
//!    reaper and API run unchanged on top of it.
//! 2. As the foundation for deterministic simulation: with a
//!    [`ManualClock`](taskforge_domain::ManualClock) and a seeded
//!    [`Jitter`], every run is exactly reproducible.
//!
//! It is not durable and not meant for production. Every operation takes one
//! mutex, which is what makes each operation atomic here - the same guarantee
//! PostgreSQL gives the other implementation with single statements.
//!
//! Transitions use the pure functions of the domain crate
//! ([`decide_failure`], [`JobState::can_transition_to`]), and the conformance
//! tests run the same scenarios against this store and PostgreSQL.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use taskforge_domain::{
    AttemptOutcome, Clock, FailureContext, Jitter, Job, JobAttempt, JobId, JobState, NewJob,
    RetryPolicy, Schedule, SplitMix64, SystemClock, decide_failure,
};

use crate::pg::cancel_outcome;
use crate::types::{MAX_ERROR_BYTES, truncate};
use crate::{
    CancelOutcome, Claim, ClaimedJob, Heartbeat, JobStore, ListFilter, QueueStats, Reaped,
    StoreError, Submitted,
};

#[derive(Debug, Default)]
struct State {
    /// Keyed by id; ids are UUIDv7, so this map is also in creation order.
    jobs: BTreeMap<JobId, Job>,
    attempts: HashMap<JobId, Vec<JobAttempt>>,
    keys: HashMap<String, JobId>,
}

#[derive(Debug, Clone)]
pub struct MemJobStore {
    state: Arc<Mutex<State>>,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn Jitter>,
    retry: RetryPolicy,
}

impl Default for MemJobStore {
    fn default() -> Self {
        Self::new(Arc::new(SystemClock), Arc::new(SplitMix64::from_entropy()))
    }
}

impl MemJobStore {
    pub fn new(clock: Arc<dyn Clock>, jitter: Arc<dyn Jitter>) -> Self {
        Self {
            state: Arc::default(),
            clock,
            jitter,
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Every critical section leaves the state consistent before it can
        // panic (validation happens before mutation), so a poisoned lock is
        // still safe to use.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }
}

fn after(now: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    now + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)
}

/// Apply a state change, refusing anything the state machine forbids. A
/// refusal here is a bug in this file, so it is reported loudly.
fn transition(job: &mut Job, to: JobState, now: DateTime<Utc>) -> Result<(), StoreError> {
    if !job.state.can_transition_to(to) {
        return Err(StoreError::Corrupt(format!(
            "in-memory store attempted {} -> {to} on job {}",
            job.state, job.id
        )));
    }
    job.state = to;
    job.updated_at = now;
    Ok(())
}

/// Why an attempt is ending in failure.
struct Failure<'a> {
    error: &'a str,
    retryable: bool,
    outcome: AttemptOutcome,
}

impl State {
    fn job_mut(&mut self, id: JobId) -> Result<&mut Job, StoreError> {
        self.jobs.get_mut(&id).ok_or(StoreError::NotFound(id))
    }

    fn close_attempt(
        &mut self,
        job: &Job,
        outcome: AttemptOutcome,
        error: Option<&str>,
        now: DateTime<Utc>,
    ) {
        if let Some(record) = self
            .attempts
            .get_mut(&job.id)
            .and_then(|list| list.iter_mut().find(|a| a.attempt == job.attempt))
        {
            record.outcome = Some(outcome);
            record.error = error.map(str::to_owned);
            record.finished_at = Some(now);
        }
    }

    /// The job, if `claim` still holds it; otherwise the matching error.
    fn held(&mut self, claim: &Claim) -> Result<&mut Job, StoreError> {
        let job = self.job_mut(claim.job_id)?;
        if !claim.holds(job) {
            return Err(StoreError::StaleClaim {
                job_id: claim.job_id,
                worker_id: claim.worker_id.clone(),
                attempt: claim.attempt,
                current_state: job.state,
                current_attempt: job.attempt,
            });
        }
        Ok(job)
    }

    /// End the current attempt of a running job in failure.
    fn end_in_failure(
        &mut self,
        id: JobId,
        failure: Failure<'_>,
        policy: &RetryPolicy,
        jitter: f64,
        now: DateTime<Utc>,
    ) -> Result<Job, StoreError> {
        let Failure {
            error,
            retryable,
            outcome,
        } = failure;
        let error = truncate(error, MAX_ERROR_BYTES);
        let job = self.job_mut(id)?;
        let decision = decide_failure(
            FailureContext {
                failures: job.failures,
                max_attempts: job.max_attempts,
                cancel_requested: job.cancel_requested,
            },
            retryable,
            policy,
            jitter,
        );
        transition(job, decision.state, now)?;
        job.failures = decision.failures;
        job.last_error = Some(error.to_owned());
        job.lease_owner = None;
        job.lease_expires_at = None;
        match decision.retry_in {
            Some(delay) => job.run_at = after(now, delay),
            None => job.finished_at = Some(now),
        }
        let job = job.clone();
        let outcome = if job.state == JobState::Cancelled {
            AttemptOutcome::Cancelled
        } else {
            outcome
        };
        self.close_attempt(&job, outcome, Some(error), now);
        Ok(job)
    }
}

impl JobStore for MemJobStore {
    async fn ping(&self) -> Result<(), StoreError> {
        Ok(())
    }

    async fn submit(&self, new: &NewJob) -> Result<Submitted, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        if let Some(key) = new.idempotency_key() {
            if let Some(id) = state.keys.get(key.as_str()) {
                let existing = state.jobs[id].clone();
                return if new.same_request_as(&existing) {
                    Ok(Submitted::Duplicate(existing))
                } else {
                    Err(StoreError::IdempotencyConflict {
                        key: key.as_str().to_owned(),
                    })
                };
            }
        }
        let run_at = match new.schedule() {
            Schedule::Immediately => now,
            Schedule::At(at) => at,
            Schedule::After(delay) => after(now, delay),
        };
        let job = Job {
            id: new.id(),
            queue: new.queue().to_owned(),
            kind: new.kind().to_owned(),
            payload: new.payload().clone(),
            state: JobState::Queued,
            priority: new.priority(),
            run_at,
            attempt: 0,
            failures: 0,
            max_attempts: new.max_attempts(),
            timeout_secs: new.timeout_secs(),
            idempotency_key: new.idempotency_key().map(|k| k.as_str().to_owned()),
            request_hash: Some(new.request_hash().to_owned()),
            cancel_requested: false,
            lease_owner: None,
            lease_expires_at: None,
            result: None,
            last_error: None,
            created_at: now,
            updated_at: now,
            started_at: None,
            finished_at: None,
        };
        if let Some(key) = &job.idempotency_key {
            state.keys.insert(key.clone(), job.id);
        }
        state.jobs.insert(job.id, job.clone());
        Ok(Submitted::Created(job))
    }

    async fn get(&self, id: JobId) -> Result<Option<Job>, StoreError> {
        Ok(self.lock().jobs.get(&id).cloned())
    }

    async fn list(&self, filter: &ListFilter) -> Result<Vec<Job>, StoreError> {
        let state = self.lock();
        Ok(state
            .jobs
            .values()
            .rev()
            .filter(|j| filter.state.is_none_or(|s| j.state == s))
            .filter(|j| filter.queue.as_ref().is_none_or(|q| &j.queue == q))
            .filter(|j| filter.kind.as_ref().is_none_or(|k| &j.kind == k))
            .filter(|j| filter.before.is_none_or(|b| j.id < b))
            .take(filter.limit as usize)
            .cloned()
            .collect())
    }

    async fn attempts(&self, id: JobId) -> Result<Vec<JobAttempt>, StoreError> {
        Ok(self.lock().attempts.get(&id).cloned().unwrap_or_default())
    }

    async fn claim(
        &self,
        worker_id: &str,
        queues: &[String],
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let mut due: Vec<&Job> = state
            .jobs
            .values()
            .filter(|j| j.state == JobState::Queued && j.run_at <= now && queues.contains(&j.queue))
            .collect();
        due.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.run_at.cmp(&b.run_at))
                .then(a.id.cmp(&b.id))
        });
        let ids: Vec<JobId> = due.into_iter().take(limit as usize).map(|j| j.id).collect();

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            let job = state.job_mut(id)?;
            transition(job, JobState::Running, now)?;
            job.attempt += 1;
            job.lease_owner = Some(worker_id.to_owned());
            job.lease_expires_at = Some(after(now, lease));
            job.started_at = Some(now);
            let job = job.clone();
            state.attempts.entry(id).or_default().push(JobAttempt {
                attempt: job.attempt,
                worker_id: worker_id.to_owned(),
                started_at: now,
                finished_at: None,
                outcome: None,
                error: None,
            });
            let claim = Claim {
                job_id: id,
                worker_id: worker_id.to_owned(),
                attempt: job.attempt,
            };
            claimed.push(ClaimedJob { job, claim });
        }
        Ok(claimed)
    }

    async fn heartbeat(
        &self,
        worker_id: &str,
        claims: &[Claim],
        lease: Duration,
    ) -> Result<Heartbeat, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let mut beat = Heartbeat::default();
        for claim in claims {
            match state.jobs.get_mut(&claim.job_id) {
                Some(job) if claim.worker_id == worker_id && claim.holds(job) => {
                    job.lease_expires_at = Some(after(now, lease));
                    job.updated_at = now;
                    if job.cancel_requested {
                        beat.cancel_requested.push(job.id);
                    }
                }
                _ => beat.lost.push(claim.job_id),
            }
        }
        Ok(beat)
    }

    async fn complete(&self, claim: &Claim, result: &Value) -> Result<Job, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let job = state.held(claim)?;
        transition(job, JobState::Succeeded, now)?;
        job.result = Some(result.clone());
        job.last_error = None;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.finished_at = Some(now);
        let job = job.clone();
        state.close_attempt(&job, AttemptOutcome::Succeeded, None, now);
        Ok(job)
    }

    async fn fail(&self, claim: &Claim, error: &str, retryable: bool) -> Result<Job, StoreError> {
        let now = self.now();
        let jitter = self.jitter.sample();
        let mut state = self.lock();
        state.held(claim)?;
        state.end_in_failure(
            claim.job_id,
            Failure {
                error,
                retryable,
                outcome: AttemptOutcome::Failed,
            },
            &self.retry,
            jitter,
            now,
        )
    }

    async fn release(&self, claim: &Claim) -> Result<Job, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let job = state.held(claim)?;
        let outcome = if job.cancel_requested {
            transition(job, JobState::Cancelled, now)?;
            job.finished_at = Some(now);
            AttemptOutcome::Cancelled
        } else {
            transition(job, JobState::Queued, now)?;
            job.run_at = now;
            AttemptOutcome::Released
        };
        job.lease_owner = None;
        job.lease_expires_at = None;
        let job = job.clone();
        state.close_attempt(&job, outcome, None, now);
        Ok(job)
    }

    async fn acknowledge_cancel(&self, claim: &Claim) -> Result<Job, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let job = state.held(claim)?;
        if !job.cancel_requested {
            return Err(StoreError::StaleClaim {
                job_id: claim.job_id,
                worker_id: claim.worker_id.clone(),
                attempt: claim.attempt,
                current_state: job.state,
                current_attempt: job.attempt,
            });
        }
        transition(job, JobState::Cancelled, now)?;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.finished_at = Some(now);
        let job = job.clone();
        state.close_attempt(&job, AttemptOutcome::Cancelled, None, now);
        Ok(job)
    }

    async fn reap_expired(&self, limit: u32) -> Result<Reaped, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let mut expired: Vec<(DateTime<Utc>, JobId)> = state
            .jobs
            .values()
            .filter(|j| j.state == JobState::Running)
            .filter_map(|j| {
                j.lease_expires_at
                    .filter(|at| *at < now)
                    .map(|at| (at, j.id))
            })
            .collect();
        expired.sort();

        let mut reaped = Reaped::default();
        for (_, id) in expired.into_iter().take(limit as usize) {
            let job = state.end_in_failure(
                id,
                Failure {
                    error: "lease expired: the worker stopped renewing it",
                    retryable: true,
                    outcome: AttemptOutcome::LeaseExpired,
                },
                &self.retry,
                self.jitter.sample(),
                now,
            )?;
            reaped.count(job.state);
        }
        Ok(reaped)
    }

    async fn request_cancel(&self, id: JobId) -> Result<CancelOutcome, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let job = state.job_mut(id)?;
        match job.state {
            JobState::Queued => {
                transition(job, JobState::Cancelled, now)?;
                job.cancel_requested = true;
                job.finished_at = Some(now);
            }
            JobState::Running => {
                job.cancel_requested = true;
                job.updated_at = now;
            }
            _ => {}
        }
        Ok(cancel_outcome(job.clone()))
    }

    async fn redrive(&self, id: JobId) -> Result<Job, StoreError> {
        let now = self.now();
        let mut state = self.lock();
        let job = state.job_mut(id)?;
        if job.state != JobState::Dead {
            return Err(StoreError::InvalidState {
                job_id: id,
                state: job.state,
                action: "retry",
            });
        }
        transition(job, JobState::Queued, now)?;
        job.failures = 0;
        job.run_at = now;
        job.cancel_requested = false;
        job.finished_at = None;
        Ok(job.clone())
    }

    async fn purge_finished(&self, older_than: Duration, limit: u32) -> Result<u64, StoreError> {
        let cutoff =
            self.now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::MAX);
        let mut state = self.lock();
        let mut old: Vec<(DateTime<Utc>, JobId)> = state
            .jobs
            .values()
            .filter_map(|j| j.finished_at.filter(|at| *at < cutoff).map(|at| (at, j.id)))
            .collect();
        old.sort();
        let mut purged = 0;
        for (_, id) in old.into_iter().take(limit as usize) {
            if let Some(job) = state.jobs.remove(&id) {
                state.attempts.remove(&id);
                if let Some(key) = job.idempotency_key {
                    state.keys.remove(&key);
                }
                purged += 1;
            }
        }
        Ok(purged)
    }

    async fn stats(&self) -> Result<QueueStats, StoreError> {
        let now = self.now();
        let state = self.lock();
        let mut counts: BTreeMap<(String, &'static str), (JobState, i64)> = BTreeMap::new();
        let mut oldest_due: Option<DateTime<Utc>> = None;
        let mut expired_leases = 0;
        for job in state.jobs.values() {
            counts
                .entry((job.queue.clone(), job.state.as_str()))
                .or_insert((job.state, 0))
                .1 += 1;
            if job.state == JobState::Queued && job.run_at <= now {
                oldest_due = Some(oldest_due.map_or(job.run_at, |o| o.min(job.run_at)));
            }
            if job.state == JobState::Running && job.lease_expires_at.is_some_and(|at| at < now) {
                expired_leases += 1;
            }
        }
        Ok(QueueStats {
            counts: counts
                .into_iter()
                .map(|((queue, _), (state, n))| (queue, state, n))
                .collect(),
            oldest_due_secs: oldest_due
                .map_or(0.0, |at| (now - at).num_milliseconds() as f64 / 1000.0),
            expired_leases,
        })
    }
}
