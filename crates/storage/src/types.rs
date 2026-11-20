//! Values passed across the [`JobStore`](crate::JobStore) boundary.

use taskforge_domain::{Job, JobId, JobState};

/// Longest error message persisted with a failed job.
pub(crate) const MAX_ERROR_BYTES: usize = 4096;

/// Proof that a worker holds a particular attempt of a job.
///
/// Only [`JobStore::claim`](crate::JobStore::claim) creates one, and settling
/// a job requires presenting it. The pair (worker, attempt) is the fencing
/// token: because every claim increments `attempt`, a claim that has been
/// superseded can never match the job again.
///
/// The fields are private to this crate: code elsewhere can receive a claim
/// but never forge one. That is also why every `JobStore` implementation
/// lives in this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
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

    /// Whether this claim is the current holder of `job`.
    pub(crate) fn holds(&self, job: &Job) -> bool {
        job.id == self.job_id
            && job.state == JobState::Running
            && job.lease_owner.as_deref() == Some(self.worker_id.as_str())
            && job.attempt == self.attempt
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

    pub(crate) fn count(&mut self, state: JobState) {
        match state {
            JobState::Queued => self.requeued += 1,
            JobState::Dead => self.dead += 1,
            _ => self.cancelled += 1,
        }
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

/// A point-in-time summary of the queue, for metrics and dashboards.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueStats {
    /// Number of jobs per (queue, state).
    pub counts: Vec<(String, JobState, i64)>,
    /// Age of the oldest job that is due but not yet claimed. Zero when there
    /// is none. A growing value means workers are not keeping up.
    pub oldest_due_secs: f64,
    /// Running jobs whose lease has already expired but which no reaper has
    /// taken back yet. Persistently non-zero means no reaper is running.
    pub expired_leases: i64,
}

/// Cut `s` to at most `max` bytes without splitting a UTF-8 character.
pub(crate) fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
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
