use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use taskforge_domain::{
    AttemptOutcome, Job, JobAttempt, JobId, JobState, MAX_MAX_ATTEMPTS, NewJob, RetryPolicy,
    Schedule,
};
use uuid::Uuid;

use crate::StoreError;

/// Longest error message persisted with a failed job.
const MAX_ERROR_BYTES: usize = 4096;

/// The guard every settle statement carries. It matches only while the caller
/// still holds the current attempt of a running job.
const HOLDS_CLAIM: &str =
    "j.id = $1 AND j.state = 'running' AND j.lease_owner = $2 AND j.attempt = $3";

/// Proof that a worker holds a particular attempt of a job.
///
/// Only [`JobStore::claim`] creates one, and settling a job requires
/// presenting it. The pair (worker, attempt) is the fencing token: because
/// every claim increments `attempt`, a claim that has been superseded can
/// never match the row again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    // Crate-private: other crates cannot forge a claim, only receive one.
    pub(crate) job_id: JobId,
    pub(crate) worker_id: String,
    pub(crate) attempt: i32,
}

impl Claim {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn attempt(&self) -> i32 {
        self.attempt
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: Job,
    pub claim: Claim,
}

/// Result of a submission.
#[derive(Debug, Clone, PartialEq)]
pub enum Submitted {
    /// A new job was created.
    Created(Job),
    /// The idempotency key was seen before; this is the job it created.
    Duplicate(Job),
}

impl Submitted {
    pub fn job(&self) -> &Job {
        match self {
            Submitted::Created(job) | Submitted::Duplicate(job) => job,
        }
    }

    pub fn into_job(self) -> Job {
        match self {
            Submitted::Created(job) | Submitted::Duplicate(job) => job,
        }
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Submitted::Duplicate(_))
    }
}

/// What a heartbeat learned about the claims it tried to renew.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Heartbeat {
    /// Claims that no longer hold: stop working on these jobs.
    pub lost: Vec<JobId>,
    /// Still held (and renewed), but someone asked for cancellation.
    pub cancel_requested: Vec<JobId>,
}

/// Result of a cancellation request.
#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    /// The job is cancelled (it was queued, or had been cancelled before).
    Cancelled(Job),
    /// The job is running. The request is recorded; the worker will see it at
    /// its next heartbeat. The job may still succeed if it finishes first.
    Requested(Job),
    /// The job already succeeded or was dead-lettered; nothing to cancel.
    AlreadyFinished(Job),
}

/// What one reaper pass did with the expired leases it found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reaped {
    pub requeued: u64,
    pub dead: u64,
    pub cancelled: u64,
}

impl Reaped {
    pub fn total(&self) -> u64 {
        self.requeued + self.dead + self.cancelled
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    pub state: Option<JobState>,
    pub queue: Option<String>,
    pub kind: Option<String>,
    /// Keyset cursor: only jobs created before this one.
    pub before: Option<JobId>,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct JobStore {
    pool: PgPool,
    retry: RetryPolicy,
}

impl JobStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Cheap round trip, for readiness checks.
    pub async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    // ------------------------------------------------------------- submission

    /// Persist a new job as `queued`. Durable once this returns.
    ///
    /// With an idempotency key the insert happens at most once, however many
    /// times and however concurrently it is attempted: the unique index turns
    /// every insert after the first into a no-op, and the caller is handed the
    /// job the first one created.
    pub async fn submit(&self, job: &NewJob) -> Result<Submitted, StoreError> {
        // Relative delays are resolved against the database clock, the same
        // clock the claim query compares `run_at` with.
        let (run_at, delay): (Option<DateTime<Utc>>, Duration) = match job.schedule() {
            Schedule::Immediately => (None, Duration::ZERO),
            Schedule::At(at) => (Some(at), Duration::ZERO),
            Schedule::After(delay) => (None, delay),
        };
        let key = job.idempotency_key().map(|k| k.as_str());

        // The loop only repeats in one corner case: the row we collided with
        // was deleted before we could read it.
        for _ in 0..3 {
            let inserted = sqlx::query_as::<_, JobRow>(
                r#"
                INSERT INTO jobs (id, queue, kind, payload, priority, run_at,
                                  max_attempts, idempotency_key)
                VALUES ($1, $2, $3, $4, $5,
                        COALESCE($6::timestamptz, now()) + make_interval(secs => $7::float8),
                        $8, $9)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING *
                "#,
            )
            .bind(job.id().as_uuid())
            .bind(job.queue())
            .bind(job.kind())
            .bind(job.payload())
            .bind(job.priority())
            .bind(run_at)
            .bind(delay.as_secs_f64())
            .bind(job.max_attempts())
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(row) = inserted {
                return Ok(Submitted::Created(row.try_into()?));
            }

            // `DO NOTHING` waited for the competing insert to commit, so the
            // winner is visible to this fresh statement.
            let Some(key) = key else {
                return Err(StoreError::Corrupt(
                    "insert without an idempotency key returned no row".to_owned(),
                ));
            };
            let existing =
                sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE idempotency_key = $1")
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await?;
            if let Some(row) = existing {
                let existing = Job::try_from(row)?;
                return if job.same_request_as(&existing) {
                    Ok(Submitted::Duplicate(existing))
                } else {
                    Err(StoreError::IdempotencyConflict {
                        key: key.to_owned(),
                    })
                };
            }
        }
        Err(StoreError::Corrupt(
            "idempotent insert did not converge".to_owned(),
        ))
    }

    /// [`JobStore::submit`], for callers that do not care whether the job is new.
    pub async fn insert(&self, job: &NewJob) -> Result<Job, StoreError> {
        self.submit(job).await.map(Submitted::into_job)
    }

    // ---------------------------------------------------------------- queries

    pub async fn get(&self, id: JobId) -> Result<Option<Job>, StoreError> {
        sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .map(Job::try_from)
            .transpose()
    }

    /// Newest first, with keyset pagination on the (time-ordered) id.
    pub async fn list(&self, filter: &ListFilter) -> Result<Vec<Job>, StoreError> {
        let mut query = QueryBuilder::<Postgres>::new("SELECT * FROM jobs WHERE true");
        if let Some(state) = filter.state {
            query.push(" AND state = ").push_bind(state.as_str());
        }
        if let Some(queue) = &filter.queue {
            query.push(" AND queue = ").push_bind(queue);
        }
        if let Some(kind) = &filter.kind {
            query.push(" AND kind = ").push_bind(kind);
        }
        if let Some(before) = filter.before {
            query.push(" AND id < ").push_bind(before.as_uuid());
        }
        query
            .push(" ORDER BY id DESC LIMIT ")
            .push_bind(i64::from(filter.limit));
        query
            .build_query_as::<JobRow>()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(Job::try_from)
            .collect()
    }

    /// Delivery history of a job, oldest attempt first.
    pub async fn attempts(&self, id: JobId) -> Result<Vec<JobAttempt>, StoreError> {
        sqlx::query_as::<_, AttemptRow>(
            "SELECT attempt, worker_id, started_at, finished_at, outcome, error
             FROM job_attempts WHERE job_id = $1 ORDER BY attempt",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(JobAttempt::try_from)
        .collect()
    }

    // ------------------------------------------------------------------ claim

    /// Lease up to `limit` runnable jobs on any of `queues` to `worker_id`.
    ///
    /// One statement, hence atomic: it picks the best candidates, skipping
    /// rows a concurrent claimer has locked (`FOR UPDATE SKIP LOCKED`), marks
    /// them running, bumps the fencing token, starts the lease and opens an
    /// attempt record. Two workers calling this at the same instant always
    /// receive different jobs.
    pub async fn claim(
        &self,
        worker_id: &str,
        queues: &[String],
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, JobRow>(
            r#"
            WITH next AS (
                SELECT id
                FROM jobs
                WHERE state = 'queued' AND queue = ANY($1) AND run_at <= now()
                ORDER BY priority DESC, run_at, id
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            ),
            claimed AS (
                UPDATE jobs j
                SET state = 'running',
                    attempt = j.attempt + 1,
                    lease_owner = $2,
                    lease_expires_at = now() + make_interval(secs => $3::float8),
                    started_at = now()
                FROM next
                WHERE j.id = next.id
                RETURNING j.*
            ),
            recorded AS (
                INSERT INTO job_attempts (job_id, attempt, worker_id)
                SELECT id, attempt, $2 FROM claimed
            )
            SELECT * FROM claimed ORDER BY priority DESC, run_at, id
            "#,
        )
        .bind(queues)
        .bind(worker_id)
        .bind(lease.as_secs_f64())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let job = Job::try_from(row)?;
                let claim = Claim {
                    job_id: job.id,
                    worker_id: worker_id.to_owned(),
                    attempt: job.attempt,
                };
                Ok(ClaimedJob { job, claim })
            })
            .collect()
    }

    /// [`JobStore::claim`] for a single job.
    pub async fn claim_next(
        &self,
        worker_id: &str,
        queues: &[String],
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, StoreError> {
        Ok(self.claim(worker_id, queues, lease, 1).await?.pop())
    }

    // -------------------------------------------------------------- heartbeat

    /// Renew the lease on every claim that still holds, in one statement.
    ///
    /// Renewal is fenced like everything else: a claim that has been
    /// superseded is not renewed and is reported in `lost`.
    pub async fn heartbeat(
        &self,
        worker_id: &str,
        claims: &[Claim],
        lease: Duration,
    ) -> Result<Heartbeat, StoreError> {
        if claims.is_empty() {
            return Ok(Heartbeat::default());
        }
        let ids: Vec<Uuid> = claims.iter().map(|c| c.job_id.as_uuid()).collect();
        let attempts: Vec<i32> = claims.iter().map(|c| c.attempt).collect();
        let held: Vec<(Uuid, bool)> = sqlx::query_as(
            r#"
            UPDATE jobs j
            SET lease_expires_at = now() + make_interval(secs => $4::float8)
            FROM unnest($2::uuid[], $3::int4[]) AS c (id, attempt)
            WHERE j.id = c.id AND j.attempt = c.attempt
              AND j.lease_owner = $1 AND j.state = 'running'
            RETURNING j.id, j.cancel_requested
            "#,
        )
        .bind(worker_id)
        .bind(&ids)
        .bind(&attempts)
        .bind(lease.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;

        let lost = ids
            .iter()
            .filter(|id| !held.iter().any(|(held_id, _)| held_id == *id))
            .map(|id| JobId::from_uuid(*id))
            .collect();
        let cancel_requested = held
            .iter()
            .filter(|(_, cancel)| *cancel)
            .map(|(id, _)| JobId::from_uuid(*id))
            .collect();
        Ok(Heartbeat {
            lost,
            cancel_requested,
        })
    }

    // --------------------------------------------------------------- settling
    //
    // Each of these is one statement that changes the job and closes its
    // attempt record together, and applies only if `claim` is still the
    // current attempt. Otherwise nothing is written and `StaleClaim` is
    // returned.

    /// Commit a successful result: `running -> succeeded`.
    ///
    /// Succeeds even if cancellation was requested in the meantime: the first
    /// committed outcome wins, and this one got there first.
    pub async fn complete(&self, claim: &Claim, result: &Value) -> Result<Job, StoreError> {
        let sql = format!(
            r#"
            WITH settled AS (
                UPDATE jobs j
                SET state = 'succeeded', result = $4, last_error = NULL,
                    lease_owner = NULL, lease_expires_at = NULL, finished_at = now()
                WHERE {HOLDS_CLAIM}
                RETURNING j.*
            ),
            recorded AS (
                UPDATE job_attempts a
                SET outcome = 'succeeded', finished_at = now()
                FROM settled s
                WHERE a.job_id = s.id AND a.attempt = s.attempt
            )
            SELECT * FROM settled
            "#
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .bind(result)
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }

    /// Record a failed attempt.
    ///
    /// * retryable and attempts remain: `running -> queued`, delayed by backoff
    /// * otherwise: `running -> dead` (dead-lettered)
    /// * cancellation was requested: `running -> cancelled`
    pub async fn fail(
        &self,
        claim: &Claim,
        error: &str,
        retryable: bool,
    ) -> Result<Job, StoreError> {
        let sql = failure_sql(
            "",
            &format!("WHERE {HOLDS_CLAIM}"),
            ["$4", "$5", "$6", "$7"],
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .bind(truncate(error, MAX_ERROR_BYTES))
            .bind(retryable)
            .bind(self.retry.ceilings_secs(MAX_MAX_ATTEMPTS))
            .bind(AttemptOutcome::Failed.as_str())
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }

    /// Hand an unfinished job back during graceful shutdown:
    /// `running -> queued`, runnable at once, without counting a failure.
    /// (If cancellation was requested, it becomes `cancelled` instead.)
    pub async fn release(&self, claim: &Claim) -> Result<Job, StoreError> {
        let sql = format!(
            r#"
            WITH settled AS (
                UPDATE jobs j
                SET state = CASE WHEN j.cancel_requested THEN 'cancelled' ELSE 'queued' END,
                    run_at = CASE WHEN j.cancel_requested THEN j.run_at ELSE now() END,
                    finished_at = CASE WHEN j.cancel_requested THEN now() END,
                    lease_owner = NULL, lease_expires_at = NULL
                WHERE {HOLDS_CLAIM}
                RETURNING j.*
            ),
            recorded AS (
                UPDATE job_attempts a
                SET outcome = CASE WHEN s.state = 'cancelled' THEN 'cancelled' ELSE 'released' END,
                    finished_at = now()
                FROM settled s
                WHERE a.job_id = s.id AND a.attempt = s.attempt
            )
            SELECT * FROM settled
            "#
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }

    /// The worker stopped the handler because cancellation was requested:
    /// `running -> cancelled`. Applies only if it really was requested.
    pub async fn acknowledge_cancel(&self, claim: &Claim) -> Result<Job, StoreError> {
        let sql = format!(
            r#"
            WITH settled AS (
                UPDATE jobs j
                SET state = 'cancelled', lease_owner = NULL, lease_expires_at = NULL,
                    finished_at = now()
                WHERE {HOLDS_CLAIM} AND j.cancel_requested
                RETURNING j.*
            ),
            recorded AS (
                UPDATE job_attempts a
                SET outcome = 'cancelled', finished_at = now()
                FROM settled s
                WHERE a.job_id = s.id AND a.attempt = s.attempt
            )
            SELECT * FROM settled
            "#
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }

    /// Interpret the outcome of a fenced UPDATE. No row means the guard did not
    /// match: find out why, so the caller gets a precise error.
    async fn settled(&self, claim: &Claim, row: Option<JobRow>) -> Result<Job, StoreError> {
        if let Some(row) = row {
            return row.try_into();
        }
        let current = self
            .get(claim.job_id)
            .await?
            .ok_or(StoreError::NotFound(claim.job_id))?;
        Err(StoreError::StaleClaim {
            job_id: claim.job_id,
            worker_id: claim.worker_id.clone(),
            attempt: claim.attempt,
            current_state: current.state,
            current_attempt: current.attempt,
        })
    }

    // --------------------------------------------------------------- recovery

    /// Take back up to `limit` jobs whose lease has expired, treating each as
    /// a failed attempt (retry with backoff, or dead-letter when exhausted).
    ///
    /// One statement. `SKIP LOCKED` makes each expired lease the business of
    /// exactly one reaper, so any number of scheduler processes may run this
    /// concurrently. The worker that lost the lease is fenced off by the
    /// state change itself: its claim no longer matches a running row.
    pub async fn reap_expired(&self, limit: u32) -> Result<Reaped, StoreError> {
        let sql = failure_sql(
            r#"
            expired AS (
                SELECT id FROM jobs
                WHERE state = 'running' AND lease_expires_at < now()
                ORDER BY lease_expires_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            "#,
            "FROM expired WHERE j.id = expired.id",
            ["$2", "$3", "$4", "$5"],
        );
        let rows = sqlx::query_as::<_, JobRow>(&sql)
            .bind(i64::from(limit))
            .bind("lease expired: the worker stopped renewing it")
            .bind(true)
            .bind(self.retry.ceilings_secs(MAX_MAX_ATTEMPTS))
            .bind(AttemptOutcome::LeaseExpired.as_str())
            .fetch_all(&self.pool)
            .await?;

        let mut reaped = Reaped::default();
        for row in rows {
            let job = Job::try_from(row)?;
            tracing::warn!(
                job_id = %job.id, attempt = job.attempt, failures = job.failures,
                now = %job.state, "lease expired; job taken back from its worker"
            );
            match job.state {
                JobState::Queued => reaped.requeued += 1,
                JobState::Dead => reaped.dead += 1,
                _ => reaped.cancelled += 1,
            }
        }
        Ok(reaped)
    }

    // ----------------------------------------------------------- user actions

    /// Ask for a job to be cancelled.
    ///
    /// A queued job is cancelled on the spot. A running job cannot be stopped
    /// from here, so the request is recorded for its worker; whichever commits
    /// first - the worker's result or its acknowledgement of the cancellation
    /// - decides the final state, and the other is rejected.
    pub async fn request_cancel(&self, id: JobId) -> Result<CancelOutcome, StoreError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET cancel_requested = true,
                state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
                finished_at = CASE WHEN state = 'queued' THEN now() ELSE finished_at END
            WHERE id = $1 AND state IN ('queued', 'running')
            RETURNING *
            "#,
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        // No row updated: the job is missing or already in a terminal state.
        let job = match row {
            Some(row) => Job::try_from(row)?,
            None => self.get(id).await?.ok_or(StoreError::NotFound(id))?,
        };
        Ok(match job.state {
            JobState::Cancelled => CancelOutcome::Cancelled(job),
            JobState::Queued | JobState::Running => CancelOutcome::Requested(job),
            JobState::Succeeded | JobState::Dead => CancelOutcome::AlreadyFinished(job),
        })
    }

    /// Operator re-drive: put a dead-lettered job back in the queue with a
    /// fresh failure budget (`dead -> queued`).
    pub async fn redrive(&self, id: JobId) -> Result<Job, StoreError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET state = 'queued', failures = 0, run_at = now(),
                cancel_requested = false, finished_at = NULL
            WHERE id = $1 AND state = 'dead'
            RETURNING *
            "#,
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return row.try_into();
        }
        let current = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
        Err(StoreError::InvalidState {
            job_id: id,
            state: current.state,
            action: "retry",
        })
    }
}

/// The statement that ends an attempt in failure. Shared by `fail` (one job,
/// fenced by its claim) and the reaper (a batch of expired leases), so both
/// apply exactly the same retry rules.
///
/// `params` are the placeholders for, in order: error text, retryable flag,
/// backoff ceilings in seconds indexed by failure count, and the attempt
/// outcome label.
///
/// The delay is `ceiling * (0.5 + 0.5 * random())`: the equal-jitter formula
/// of `RetryPolicy::delay`, evaluated per row so that jobs rescheduled by one
/// statement do not all come back at the same instant.
fn failure_sql(leading_cte: &str, target: &str, params: [&str; 4]) -> String {
    let [error, retryable, ceilings, outcome] = params;
    let will_retry = format!(
        "(NOT j.cancel_requested AND {retryable}::bool AND j.failures + 1 < j.max_attempts)"
    );
    format!(
        r#"
        WITH {leading_cte} settled AS (
            UPDATE jobs j
            SET failures = j.failures + 1,
                state = CASE WHEN j.cancel_requested THEN 'cancelled'
                             WHEN {will_retry} THEN 'queued'
                             ELSE 'dead' END,
                run_at = CASE WHEN {will_retry}
                              THEN now() + make_interval(secs =>
                                   ({ceilings}::float8[])[j.failures + 1] * (0.5 + 0.5 * random()))
                              ELSE j.run_at END,
                finished_at = CASE WHEN {will_retry} THEN NULL ELSE now() END,
                last_error = {error}::text, lease_owner = NULL, lease_expires_at = NULL
            {target}
            RETURNING j.*
        ),
        recorded AS (
            UPDATE job_attempts a
            SET outcome = CASE WHEN s.state = 'cancelled' THEN 'cancelled' ELSE {outcome}::text END,
                error = {error}::text, finished_at = now()
            FROM settled s
            WHERE a.job_id = s.id AND a.attempt = s.attempt
        )
        SELECT * FROM settled
        "#
    )
}

/// Cut `s` to at most `max` bytes without splitting a UTF-8 character.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    queue: String,
    kind: String,
    payload: Value,
    state: String,
    priority: i16,
    run_at: DateTime<Utc>,
    attempt: i32,
    failures: i32,
    max_attempts: i32,
    idempotency_key: Option<String>,
    cancel_requested: bool,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    result: Option<Value>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl TryFrom<JobRow> for Job {
    type Error = StoreError;

    fn try_from(row: JobRow) -> Result<Self, Self::Error> {
        let state: JobState = row
            .state
            .parse()
            .map_err(|e| StoreError::Corrupt(format!("job {}: {e}", row.id)))?;
        Ok(Job {
            id: JobId::from_uuid(row.id),
            queue: row.queue,
            kind: row.kind,
            payload: row.payload,
            state,
            priority: row.priority,
            run_at: row.run_at,
            attempt: row.attempt,
            failures: row.failures,
            max_attempts: row.max_attempts,
            idempotency_key: row.idempotency_key,
            cancel_requested: row.cancel_requested,
            lease_owner: row.lease_owner,
            lease_expires_at: row.lease_expires_at,
            result: row.result,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct AttemptRow {
    attempt: i32,
    worker_id: String,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    outcome: Option<String>,
    error: Option<String>,
}

impl TryFrom<AttemptRow> for JobAttempt {
    type Error = StoreError;

    fn try_from(row: AttemptRow) -> Result<Self, Self::Error> {
        let outcome = row
            .outcome
            .map(|raw| raw.parse::<AttemptOutcome>())
            .transpose()
            .map_err(|e| StoreError::Corrupt(format!("attempt {}: {e}", row.attempt)))?;
        Ok(JobAttempt {
            attempt: row.attempt,
            worker_id: row.worker_id,
            started_at: row.started_at,
            finished_at: row.finished_at,
            outcome,
            error: row.error,
        })
    }
}

#[cfg(test)]
mod unit_tests {
    use super::truncate;

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // 'é' is two bytes; cutting inside it backs off to the boundary.
        assert_eq!(truncate("né", 2), "n");
        assert_eq!(truncate("né", 3), "né");
    }
}
