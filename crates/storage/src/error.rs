use taskforge_domain::{JobId, JobState};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job {0} not found")]
    NotFound(JobId),

    /// The caller tried to settle an attempt it no longer holds: the job has
    /// moved on (finished, or been handed to another attempt). The caller's
    /// work must be discarded; nothing was written.
    #[error(
        "stale claim on job {job_id}: worker {worker_id:?} holds attempt {attempt}, \
         but the job is {current_state} at attempt {current_attempt}"
    )]
    StaleClaim {
        job_id: JobId,
        worker_id: String,
        attempt: i32,
        current_state: JobState,
        current_attempt: i32,
    },

    /// An idempotency key was reused for a different request.
    #[error("idempotency key {key:?} was already used for a different request")]
    IdempotencyConflict { key: String },

    /// The requested action does not apply to the job in its current state.
    #[error("cannot {action} job {job_id}: it is {state}")]
    InvalidState {
        job_id: JobId,
        state: JobState,
        action: &'static str,
    },

    /// A row that violates what the schema is supposed to guarantee.
    #[error("corrupt job row: {0}")]
    Corrupt(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl StoreError {
    /// True when the failure is about reaching the database rather than about
    /// the request: the same operation may succeed if repeated later.
    pub fn is_unavailable(&self) -> bool {
        match self {
            StoreError::Database(
                sqlx::Error::Io(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed,
            ) => true,
            // SQLSTATE class 08 = connection exception, 57P = operator
            // intervention (server shutting down, connection terminated).
            StoreError::Database(sqlx::Error::Database(db)) => db
                .code()
                .is_some_and(|code| code.starts_with("08") || code.starts_with("57P")),
            _ => false,
        }
    }
}
