use std::fmt;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};

/// A source of "now".
///
/// The PostgreSQL store does not use this: it compares every deadline with
/// the database's own `now()`, so that the clocks of API, worker and scheduler
/// machines never have to agree. The trait exists for stores that have no
/// database clock - the in-memory store, and a deterministic simulation that
/// needs to move time forward on command.
pub trait Clock: Send + Sync + fmt::Debug + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// The machine's wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock that only moves when told to.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    pub fn advance(&self, by: Duration) {
        let by = chrono::Duration::from_std(by).unwrap_or(chrono::Duration::MAX);
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = now.checked_add_signed(by).unwrap_or(*now);
    }
}

impl Default for ManualClock {
    /// Starts at a fixed instant, so runs are reproducible.
    fn default() -> Self {
        Self::new(DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_advanced() {
        let clock = ManualClock::default();
        let start = clock.now();
        assert_eq!(clock.now(), start);
        clock.advance(Duration::from_millis(1500));
        assert_eq!(clock.now() - start, chrono::Duration::milliseconds(1500));
    }
}
