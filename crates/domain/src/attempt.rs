use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// How one delivery of a job to a worker ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    /// The handler returned a result and it was committed.
    Succeeded,
    /// The handler returned an error, panicked, or had no registration.
    Failed,
    /// The worker stopped renewing its lease (crash, hang, partition) and the
    /// scheduler took the job back.
    LeaseExpired,
    /// The worker stopped the handler because cancellation was requested.
    Cancelled,
    /// The worker handed the job back unfinished during a graceful shutdown.
    /// Does not count as a failure.
    Released,
}

impl AttemptOutcome {
    pub const ALL: [AttemptOutcome; 5] = [
        AttemptOutcome::Succeeded,
        AttemptOutcome::Failed,
        AttemptOutcome::LeaseExpired,
        AttemptOutcome::Cancelled,
        AttemptOutcome::Released,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            AttemptOutcome::Succeeded => "succeeded",
            AttemptOutcome::Failed => "failed",
            AttemptOutcome::LeaseExpired => "lease_expired",
            AttemptOutcome::Cancelled => "cancelled",
            AttemptOutcome::Released => "released",
        }
    }
}

impl fmt::Display for AttemptOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown attempt outcome {0:?}")]
pub struct UnknownAttemptOutcome(pub String);

impl FromStr for AttemptOutcome {
    type Err = UnknownAttemptOutcome;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AttemptOutcome::ALL
            .into_iter()
            .find(|outcome| outcome.as_str() == s)
            .ok_or_else(|| UnknownAttemptOutcome(s.to_owned()))
    }
}

/// One delivery of a job to a worker. The history of a job is the list of
/// these; more than one means the job was executed more than once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobAttempt {
    pub attempt: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    /// `None` while the attempt is still in flight.
    pub outcome: Option<AttemptOutcome>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_form_round_trips_and_matches_serde() {
        for outcome in AttemptOutcome::ALL {
            assert_eq!(outcome.as_str().parse::<AttemptOutcome>(), Ok(outcome));
            assert_eq!(
                serde_json::to_value(outcome).unwrap(),
                serde_json::Value::String(outcome.as_str().to_owned())
            );
        }
        assert!("timed_out".parse::<AttemptOutcome>().is_err());
    }
}
