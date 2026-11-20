//! The PostgreSQL implementation of [`JobStore`].
//!
//! Every state transition is one SQL statement, so it is atomic without an
//! explicit transaction and cannot be half-applied by a crash. Every deadline
//! is compared with the database's `now()`, so API, worker and scheduler
//! machines never need synchronised clocks.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use taskforge_domain::{
    AttemptOutcome, Jitter, Job, JobAttempt, JobId, JobState, MAX_MAX_ATTEMPTS, NewJob,
    RetryPolicy, Schedule, SplitMix64,
};
use uuid::Uuid;

use crate::types::{MAX_ERROR_BYTES, truncate};
use crate::{
    CancelOutcome, Claim, ClaimedJob, Heartbeat, JobStore, ListFilter, QueueStats, Reaped,
    StoreError, Submitted,
};

/// The guard every settle statement carries. It matches only while the caller
/// still holds the current attempt of a running job: this one line is the
/// fencing check.
const HOLDS_CLAIM: &str =
    "j.id = $1 AND j.state = 'running' AND j.lease_owner = $2 AND j.attempt = $3";

#[derive(Debug, Clone)]
pub struct PgJobStore {
    pool: PgPool,
    retry: RetryPolicy,
    jitter: Arc<dyn Jitter>,
}

impl PgJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: RetryPolicy::default(),
            jitter: Arc::new(SplitMix64::from_entropy()),
        }
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Replace the jitter source, e.g. with a seeded or fixed one in tests.
    pub fn with_jitter(mut self, jitter: Arc<dyn Jitter>) -> Self {
        self.jitter = jitter;
        self
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
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

    /// Run a settle statement whose first three parameters are the claim.
    async fn settle(&self, claim: &Claim, sql: &str) -> Result<Job, StoreError> {
        let row = sqlx::query_as::<_, JobRow>(sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }
}

impl JobStore for PgJobStore {
    async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    // ------------------------------------------------------------- submission

    /// With an idempotency key the insert happens at most once, however many
    /// times and however concurrently it is attempted: the unique index turns
    /// every insert after the first into a no-op, and the caller is handed the
    /// job the first one created.
    async fn submit(&self, job: &NewJob) -> Result<Submitted, StoreError> {
        // Relative delays are resolved against the database clock, the same
        // clock the claim query compares `run_at` with.
        let (run_at, delay): (Option<DateTime<Utc>>, Duration) = match job.schedule() {
            Schedule::Immediately => (None, Duration::ZERO),
            Schedule::At(at) => (Some(at), Duration::ZERO),
            Schedule::After(delay) => (None, delay),
        };
        let key = job.idempotency_key().map(|k| k.as_str());

        // The loop only repeats in one corner case: the row we collided with
        // was purged before we could read it.
        for _ in 0..3 {
            let inserted = sqlx::query_as::<_, JobRow>(
                r#"
                INSERT INTO jobs (id, queue, kind, payload, priority, run_at, max_attempts,
                                  timeout_secs, idempotency_key, request_hash)
                VALUES ($1, $2, $3, $4, $5,
                        COALESCE($6::timestamptz, now()) + make_interval(secs => $7::float8),
                        $8, $9, $10, $11)
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
            .bind(job.timeout_secs())
            .bind(key)
            .bind(job.request_hash())
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

    // ---------------------------------------------------------------- queries

    async fn get(&self, id: JobId) -> Result<Option<Job>, StoreError> {
        sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .map(Job::try_from)
            .transpose()
    }

    async fn list(&self, filter: &ListFilter) -> Result<Vec<Job>, StoreError> {
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

    async fn attempts(&self, id: JobId) -> Result<Vec<JobAttempt>, StoreError> {
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

    /// One statement, hence atomic: pick the best candidates, skipping rows a
    /// concurrent claimer has locked (`FOR UPDATE SKIP LOCKED`), mark them
    /// running, bump the fencing token, start the lease and open an attempt
    /// record.
    ///
    /// Why `SKIP LOCKED`: a plain `FOR UPDATE` is also correct, but every
    /// concurrent claimer would queue behind the first one's lock on the same
    /// "next" row. A read-then-update without locking is not correct at all:
    /// two workers read the same row and both run it.
    async fn claim(
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

    // -------------------------------------------------------------- heartbeat

    /// Renew every claim that still holds, in one statement. Renewal is fenced
    /// like everything else: a superseded claim is not renewed and is
    /// reported in `lost`.
    async fn heartbeat(
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
    // Each settle is one statement that changes the job and closes its attempt
    // record together, and applies only if `claim` is still the current
    // attempt. Otherwise nothing is written and `StaleClaim` is returned.

    async fn complete(&self, claim: &Claim, result: &Value) -> Result<Job, StoreError> {
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

    async fn fail(&self, claim: &Claim, error: &str, retryable: bool) -> Result<Job, StoreError> {
        let sql = failure_sql(
            "",
            &format!("WHERE {HOLDS_CLAIM}"),
            ["$4", "$5", "$6", "$7", "$8::float8"],
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(claim.job_id.as_uuid())
            .bind(&claim.worker_id)
            .bind(claim.attempt)
            .bind(truncate(error, MAX_ERROR_BYTES))
            .bind(retryable)
            .bind(self.retry.ceilings_secs(MAX_MAX_ATTEMPTS))
            .bind(AttemptOutcome::Failed.as_str())
            .bind(self.jitter.sample())
            .fetch_optional(&self.pool)
            .await?;
        self.settled(claim, row).await
    }

    async fn release(&self, claim: &Claim) -> Result<Job, StoreError> {
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
        self.settle(claim, &sql).await
    }

    async fn acknowledge_cancel(&self, claim: &Claim) -> Result<Job, StoreError> {
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
        self.settle(claim, &sql).await
    }

    // --------------------------------------------------------------- recovery

    /// One statement. `SKIP LOCKED` makes each expired lease the business of
    /// exactly one reaper, so any number of processes may run this
    /// concurrently. The worker that lost the lease is fenced off by the state
    /// change itself: its claim no longer matches a running row.
    async fn reap_expired(&self, limit: u32) -> Result<Reaped, StoreError> {
        // `FOR UPDATE` cannot be combined with a window function, so rows are
        // locked first and numbered in a second step. The number picks each
        // row's own jitter sample out of the array.
        let sql = failure_sql(
            r#"
            locked AS (
                SELECT id, lease_expires_at FROM jobs
                WHERE state = 'running' AND lease_expires_at < now()
                ORDER BY lease_expires_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            expired AS (
                SELECT id, row_number() OVER (ORDER BY lease_expires_at, id) AS n FROM locked
            ),
            "#,
            "FROM expired WHERE j.id = expired.id",
            ["$2", "$3", "$4", "$5", "($6::float8[])[expired.n]"],
        );
        let jitters: Vec<f64> = (0..limit).map(|_| self.jitter.sample()).collect();
        let rows = sqlx::query_as::<_, JobRow>(&sql)
            .bind(i64::from(limit))
            .bind("lease expired: the worker stopped renewing it")
            .bind(true)
            .bind(self.retry.ceilings_secs(MAX_MAX_ATTEMPTS))
            .bind(AttemptOutcome::LeaseExpired.as_str())
            .bind(jitters)
            .fetch_all(&self.pool)
            .await?;

        let mut reaped = Reaped::default();
        for row in rows {
            let job = Job::try_from(row)?;
            tracing::warn!(
                job_id = %job.id, attempt = job.attempt, failures = job.failures,
                now = %job.state, "lease expired; job taken back from its worker"
            );
            reaped.count(job.state);
        }
        Ok(reaped)
    }

    // ----------------------------------------------------------- user actions

    async fn request_cancel(&self, id: JobId) -> Result<CancelOutcome, StoreError> {
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
        Ok(cancel_outcome(job))
    }

    async fn redrive(&self, id: JobId) -> Result<Job, StoreError> {
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

    async fn purge_finished(&self, older_than: Duration, limit: u32) -> Result<u64, StoreError> {
        let deleted = sqlx::query(
            r#"
            DELETE FROM jobs
            WHERE id IN (
                SELECT id FROM jobs
                WHERE finished_at < now() - make_interval(secs => $1::float8)
                ORDER BY finished_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            "#,
        )
        .bind(older_than.as_secs_f64())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(deleted.rows_affected())
    }

    async fn stats(&self) -> Result<QueueStats, StoreError> {
        let counts: Vec<(String, String, i64)> =
            sqlx::query_as("SELECT queue, state, count(*) FROM jobs GROUP BY queue, state")
                .fetch_all(&self.pool)
                .await?;
        let (oldest_due_secs, expired_leases): (f64, i64) = sqlx::query_as(
            r#"
            SELECT
                COALESCE((SELECT extract(epoch FROM now() - min(run_at))::float8
                          FROM jobs WHERE state = 'queued' AND run_at <= now()), 0),
                (SELECT count(*) FROM jobs
                 WHERE state = 'running' AND lease_expires_at < now())
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let counts = counts
            .into_iter()
            .map(|(queue, state, n)| {
                let state = state
                    .parse()
                    .map_err(|e| StoreError::Corrupt(format!("stats: {e}")))?;
                Ok((queue, state, n))
            })
            .collect::<Result<_, StoreError>>()?;
        Ok(QueueStats {
            counts,
            oldest_due_secs,
            expired_leases,
        })
    }
}

pub(crate) fn cancel_outcome(job: Job) -> CancelOutcome {
    match job.state {
        JobState::Cancelled => CancelOutcome::Cancelled(job),
        JobState::Queued | JobState::Running => CancelOutcome::Requested(job),
        JobState::Succeeded | JobState::Dead => CancelOutcome::AlreadyFinished(job),
    }
}

/// The statement that ends an attempt in failure. Shared by `fail` (one job,
/// fenced by its claim) and the reaper (a batch of expired leases), so both
/// apply exactly the same rules.
///
/// It is `decide_failure` from the domain crate written in SQL: the CASE
/// order (cancelled, then retry, then dead) and the delay
/// `ceiling(failures) * (0.5 + 0.5 * jitter)` match it exactly. Doing the
/// arithmetic here rather than in Rust keeps each transition a single
/// statement - no read-modify-write - and the conformance tests check that
/// this store and the in-memory store agree.
///
/// `params` are the SQL expressions for, in order: error text, retryable
/// flag, backoff ceilings in seconds indexed by failure count, attempt
/// outcome label, and the jitter sample in `[0, 1)`.
fn failure_sql(leading_cte: &str, target: &str, params: [&str; 5]) -> String {
    let [error, retryable, ceilings, outcome, jitter] = params;
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
                                   ({ceilings}::float8[])[j.failures + 1] * (0.5 + 0.5 * {jitter}))
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
    timeout_secs: i32,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
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
            timeout_secs: row.timeout_secs,
            idempotency_key: row.idempotency_key,
            request_hash: row.request_hash,
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
