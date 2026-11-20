//! Pure domain model for TaskForge.
//!
//! Nothing in this crate performs I/O. It defines what a job *is*, which state
//! changes are legal, and what a valid submission looks like, so that every
//! other crate (storage, API, worker) shares one definition of the rules.

mod attempt;
mod clock;
mod decide;
mod jitter;
mod job;
mod retry;
mod state;

pub use attempt::{AttemptOutcome, JobAttempt, UnknownAttemptOutcome};
pub use clock::{Clock, ManualClock, SystemClock};
pub use decide::{FailureContext, FailureDecision, decide_failure};
pub use jitter::{FixedJitter, Jitter, SplitMix64};
pub use job::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_QUEUE, IdempotencyKey, Job, JobId, JobSubmission, MAX_DELAY,
    MAX_IDEMPOTENCY_KEY_LEN, MAX_MAX_ATTEMPTS, MAX_NAME_LEN, MAX_PAYLOAD_BYTES, MAX_PRIORITY,
    MIN_PRIORITY, NewJob, Schedule, ValidationError,
};
pub use retry::RetryPolicy;
pub use state::{JobState, UnknownJobState};
