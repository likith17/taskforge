use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use taskforge_domain::JobId;

/// What a handler is told about the job it is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobContext {
    pub job_id: JobId,
    pub kind: String,
    pub queue: String,
    /// 1 for the first delivery. A handler can see `attempt > 1` and must be
    /// prepared for it: delivery is at-least-once.
    pub attempt: i32,
    /// The worker running this attempt.
    pub worker_id: String,
}

/// A handler's failure. The message is persisted with the job and returned by
/// the API, so it must not contain secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    message: String,
    retryable: bool,
}

impl HandlerError {
    /// A failure that may go away if the job is tried again (a timeout, a
    /// `503`, a lock conflict). The job is retried with backoff until its
    /// attempts are used up.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    /// A failure that trying again cannot fix (invalid input, a missing
    /// record). The job is dead-lettered at once.
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HandlerError {}

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send>>;

/// Code that knows how to run one kind of job.
///
/// Implemented for any `Fn(JobContext, Value) -> impl Future`, so a handler is
/// usually just an `async fn`.
pub trait JobHandler: Send + Sync + 'static {
    fn run(&self, ctx: JobContext, payload: Value) -> HandlerFuture;
}

impl<F, Fut> JobHandler for F
where
    F: Fn(JobContext, Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, HandlerError>> + Send + 'static,
{
    fn run(&self, ctx: JobContext, payload: Value) -> HandlerFuture {
        Box::pin(self(ctx, payload))
    }
}

/// The closed set of job kinds a worker is willing to run.
#[derive(Default, Clone)]
pub struct HandlerRegistry {
    handlers: BTreeMap<String, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handler` for `kind`, replacing any previous registration.
    pub fn register(mut self, kind: &str, handler: impl JobHandler) -> Self {
        self.handlers.insert(kind.to_owned(), Arc::new(handler));
        self
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(kind).cloned()
    }

    pub fn kinds(&self) -> BTreeSet<String> {
        self.handlers.keys().cloned().collect()
    }
}

impl fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.handlers.keys()).finish()
    }
}
