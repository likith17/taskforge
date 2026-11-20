//! Pure scheduling decisions.
//!
//! The rules for what happens to a job are written here once, as plain
//! functions of their inputs: no clock, no database, no randomness of their
//! own. The in-memory store calls them directly. The PostgreSQL store applies
//! the same rules inside SQL (so that each transition stays one atomic
//! statement), and the shared conformance tests in the storage crate check
//! that both stores produce identical results.

use std::time::Duration;

use crate::{JobState, RetryPolicy};

/// What a failed attempt needs to know about its job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureContext {
    /// Failed attempts before this one.
    pub failures: i32,
    pub max_attempts: i32,
    pub cancel_requested: bool,
}

/// What becomes of a job when an attempt ends badly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FailureDecision {
    /// `Queued` (retry), `Dead` (dead-letter) or `Cancelled`.
    pub state: JobState,
    /// Failure count after this attempt. Always one more than before: even a
    /// cancelled or dead-lettered attempt was a failed attempt.
    pub failures: i32,
    /// For a retry, how long until the job may be claimed again.
    pub retry_in: Option<Duration>,
}

/// Decide the fate of a job whose current attempt failed (handler error,
/// panic, timeout, or an expired lease).
///
/// In order of precedence:
/// 1. cancellation was requested -> `cancelled` (never retry what someone
///    asked to stop);
/// 2. the error is retryable and attempts remain -> `queued`, after a
///    jittered exponential backoff;
/// 3. otherwise -> `dead`.
///
/// `jitter` is a sample in `[0, 1]` from a [`Jitter`](crate::Jitter) source.
pub fn decide_failure(
    ctx: FailureContext,
    retryable: bool,
    policy: &RetryPolicy,
    jitter: f64,
) -> FailureDecision {
    let failures = ctx.failures.saturating_add(1);
    if ctx.cancel_requested {
        return FailureDecision {
            state: JobState::Cancelled,
            failures,
            retry_in: None,
        };
    }
    if retryable && failures < ctx.max_attempts {
        // `failures` is at least 1 here, so the cast cannot wrap.
        let delay = policy.delay(failures as u32, jitter);
        return FailureDecision {
            state: JobState::Queued,
            failures,
            retry_in: Some(delay),
        };
    }
    FailureDecision {
        state: JobState::Dead,
        failures,
        retry_in: None,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const POLICY: RetryPolicy = RetryPolicy::new(Duration::from_secs(2), Duration::from_secs(60));

    fn ctx(failures: i32, max_attempts: i32) -> FailureContext {
        FailureContext {
            failures,
            max_attempts,
            cancel_requested: false,
        }
    }

    #[test]
    fn retries_until_the_budget_is_spent_then_dead_letters() {
        // max_attempts = 3: the 1st and 2nd failures retry, the 3rd is final.
        let first = decide_failure(ctx(0, 3), true, &POLICY, 0.0);
        assert_eq!(first.state, JobState::Queued);
        assert_eq!(first.failures, 1);
        assert_eq!(first.retry_in, Some(Duration::from_secs(1)));

        let second = decide_failure(ctx(1, 3), true, &POLICY, 1.0);
        assert_eq!(second.state, JobState::Queued);
        assert_eq!(second.retry_in, Some(Duration::from_secs(4)));

        let third = decide_failure(ctx(2, 3), true, &POLICY, 0.5);
        assert_eq!(third.state, JobState::Dead);
        assert_eq!(third.failures, 3);
        assert_eq!(third.retry_in, None);
    }

    #[test]
    fn non_retryable_failure_dead_letters_at_once() {
        let d = decide_failure(ctx(0, 10), false, &POLICY, 0.5);
        assert_eq!((d.state, d.failures, d.retry_in), (JobState::Dead, 1, None));
    }

    #[test]
    fn a_single_attempt_job_is_never_retried() {
        let d = decide_failure(ctx(0, 1), true, &POLICY, 0.5);
        assert_eq!(d.state, JobState::Dead);
    }

    #[test]
    fn cancellation_wins_over_retry() {
        let mut c = ctx(0, 10);
        c.cancel_requested = true;
        let d = decide_failure(c, true, &POLICY, 0.5);
        assert_eq!(
            (d.state, d.failures, d.retry_in),
            (JobState::Cancelled, 1, None)
        );
    }

    proptest! {
        #[test]
        fn the_decision_is_always_a_legal_transition_from_running(
            failures in 0i32..30,
            max_attempts in 1i32..=25,
            cancel in any::<bool>(),
            retryable in any::<bool>(),
            jitter in 0.0f64..=1.0,
        ) {
            let c = FailureContext { failures, max_attempts, cancel_requested: cancel };
            let d = decide_failure(c, retryable, &POLICY, jitter);
            prop_assert!(JobState::Running.can_transition_to(d.state));
            prop_assert_eq!(d.failures, failures + 1);
            // A delay exists exactly when the job goes back to the queue.
            prop_assert_eq!(d.retry_in.is_some(), d.state == JobState::Queued);
            // Never retried beyond the budget.
            if d.state == JobState::Queued {
                prop_assert!(d.failures < max_attempts);
                prop_assert!(retryable && !cancel);
            }
        }
    }
}
