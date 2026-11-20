use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The lifecycle state of a job.
///
/// ```text
///            claim                 handler returned Ok
///  queued ──────────▶ running ───────────────────────────▶ succeeded
///    │  ▲               │
///    │  │ retry /       ├── retries exhausted / fatal ──▶ dead ──┐
///    │  │ lease expired │                                        │ operator
///    │  └───────────────┤                                        │ re-drive
///    │                  └── cancellation acknowledged ─▶ cancelled
///    │                                                     ▲     │
///    └──────────── cancelled before it ran ────────────────┘     │
///    ▲                                                           │
///    └───────────────────────────────────────────────────────────┘
/// ```
///
/// `succeeded` and `cancelled` are final. `dead` is the dead-letter state: the
/// system has given up, but an operator may put the job back in the queue.
///
/// The same table is enforced a second time by a trigger in the database
/// (`jobs_enforce_transition`), and an integration test checks that the two
/// agree, so a bug in application code cannot persist an illegal transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Waiting to be claimed (possibly not before `run_at`).
    Queued,
    /// Leased to exactly one worker.
    Running,
    /// The handler completed and its result was committed.
    Succeeded,
    /// Dead-lettered: failed and will not be retried automatically.
    Dead,
    /// Cancelled by request.
    Cancelled,
}

impl JobState {
    pub const ALL: [JobState; 5] = [
        JobState::Queued,
        JobState::Running,
        JobState::Succeeded,
        JobState::Dead,
        JobState::Cancelled,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Dead => "dead",
            JobState::Cancelled => "cancelled",
        }
    }

    /// No worker will pick this job up again without outside intervention.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Dead | JobState::Cancelled
        )
    }

    /// Whether moving from `self` to `next` is a legal state change.
    ///
    /// Staying in the same state (for example renewing a lease while
    /// `running`) is not a transition and returns `false`.
    pub const fn can_transition_to(self, next: JobState) -> bool {
        use JobState::*;
        matches!(
            (self, next),
            (Queued, Running)          // claimed by a worker
                | (Queued, Cancelled)  // cancelled before it ran
                | (Running, Succeeded) // handler returned Ok
                | (Running, Queued)    // retry scheduled, lease expired, or released
                | (Running, Dead)      // retries exhausted or non-retryable failure
                | (Running, Cancelled) // cancellation acknowledged by the worker
                | (Dead, Queued) // operator re-drive
        )
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown job state {0:?}")]
pub struct UnknownJobState(pub String);

impl FromStr for JobState {
    type Err = UnknownJobState;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobState::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| UnknownJobState(s.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::JobState::{self, *};

    #[test]
    fn transition_table_is_exactly_the_documented_one() {
        let allowed = [
            (Queued, Running),
            (Queued, Cancelled),
            (Running, Succeeded),
            (Running, Queued),
            (Running, Dead),
            (Running, Cancelled),
            (Dead, Queued),
        ];
        for from in JobState::ALL {
            for to in JobState::ALL {
                assert_eq!(
                    from.can_transition_to(to),
                    allowed.contains(&(from, to)),
                    "{from} -> {to}"
                );
            }
        }
    }

    #[test]
    fn succeeded_and_cancelled_are_final() {
        for state in [Succeeded, Cancelled] {
            assert!(state.is_terminal());
            assert!(JobState::ALL.iter().all(|&to| !state.can_transition_to(to)));
        }
    }

    #[test]
    fn no_state_transitions_to_itself() {
        assert!(JobState::ALL.iter().all(|&s| !s.can_transition_to(s)));
    }

    #[test]
    fn only_running_can_reach_a_result() {
        for from in JobState::ALL {
            assert_eq!(from.can_transition_to(Succeeded), from == Running);
        }
    }

    #[test]
    fn string_form_round_trips_and_matches_serde() {
        for state in JobState::ALL {
            assert_eq!(state.as_str().parse::<JobState>(), Ok(state));
            assert_eq!(
                serde_json::to_value(state).unwrap(),
                serde_json::Value::String(state.as_str().to_owned())
            );
        }
        assert!("paused".parse::<JobState>().is_err());
    }
}
