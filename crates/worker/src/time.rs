//! The worker's own sense of time.
//!
//! Lease expiry is judged on one clock only - the store's - so a worker never
//! compares its local time with a database timestamp, and a clock *offset*
//! between machines cannot matter. What can matter is clock *rate*: a worker
//! fences itself after 90% of a lease measured on its own timer, which is
//! only safe while that timer runs no more than about 10% slow relative to
//! the store's clock.
//!
//! [`LocalTime`] lets a deterministic simulation give each worker a timer that
//! runs at its own rate, so that assumption can be tested. Production uses
//! [`TokioTime`], which is exactly the Tokio timer the worker always used.

use std::fmt;
use std::time::Duration;

use tokio::time::{Instant, Interval, Sleep};

/// How far away "never" is, for durations too long to add to an instant.
/// The same value Tokio uses for `sleep` and `timeout` in that case.
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

/// A worker's local clock, expressed in Tokio instants.
///
/// Implementations supply two functions: what the clock reads now, and when
/// (on Tokio's timer) it will read a given value. Everything the worker
/// does with time - deadlines, sleeps, timeouts, the heartbeat ticker - is
/// built from these two, so a skewed clock affects all of them consistently.
pub trait LocalTime: Send + Sync + fmt::Debug + 'static {
    /// What this clock reads now.
    fn now(&self) -> Instant;

    /// The instant on Tokio's timer at which this clock will read `local`.
    fn runtime_instant(&self, local: Instant) -> Instant;

    /// A timer that fires when this clock reads `deadline`.
    fn sleep_until(&self, deadline: Instant) -> Sleep {
        tokio::time::sleep_until(self.runtime_instant(deadline))
    }

    /// A timer that fires once `duration` has passed on this clock.
    fn sleep(&self, duration: Duration) -> Sleep {
        self.sleep_until(self.deadline_in(duration))
    }

    /// The local instant `duration` from now, saturating far in the future.
    fn deadline_in(&self, duration: Duration) -> Instant {
        let now = self.now();
        now.checked_add(duration)
            .unwrap_or_else(|| now + FAR_FUTURE)
    }

    /// A ticker that fires every `period` of this clock, starting now.
    ///
    /// Panics if `period` is zero, like [`tokio::time::interval`].
    fn interval(&self, period: Duration) -> Interval {
        let start = self.now();
        let first = self.runtime_instant(start);
        let runtime_period = self
            .runtime_instant(start + period)
            .saturating_duration_since(first);
        // A clock running very fast could map a short period to zero, which
        // `interval_at` rejects; one nanosecond is the shortest it accepts.
        let runtime_period = if period.is_zero() {
            period
        } else {
            runtime_period.max(Duration::from_nanos(1))
        };
        tokio::time::interval_at(first, runtime_period)
    }
}

/// Tokio's own clock. What every worker uses outside a simulation.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioTime;

impl LocalTime for TokioTime {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn runtime_instant(&self, local: Instant) -> Instant {
        local
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        tokio::time::sleep(duration)
    }

    fn interval(&self, period: Duration) -> Interval {
        tokio::time::interval(period)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs at `rate` local seconds per Tokio second, from `origin`.
    #[derive(Debug)]
    struct Scaled {
        origin: Instant,
        rate: f64,
    }

    impl LocalTime for Scaled {
        fn now(&self) -> Instant {
            self.origin
                + Instant::now()
                    .duration_since(self.origin)
                    .mul_f64(self.rate)
        }
        fn runtime_instant(&self, local: Instant) -> Instant {
            self.origin
                + local
                    .saturating_duration_since(self.origin)
                    .div_f64(self.rate)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tokio_time_is_the_tokio_timer() {
        let start = Instant::now();
        assert_eq!(TokioTime.now(), start);
        assert_eq!(TokioTime.runtime_instant(start), start);
        TokioTime.sleep(Duration::from_millis(250)).await;
        assert_eq!(Instant::now() - start, Duration::from_millis(250));
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_clock_makes_every_wait_longer() {
        let origin = Instant::now();
        let slow = Scaled { origin, rate: 0.5 };

        slow.sleep(Duration::from_secs(1)).await;
        assert_eq!(Instant::now() - origin, Duration::from_secs(2));
        assert_eq!(slow.now() - origin, Duration::from_secs(1));

        let mut ticker = slow.interval(Duration::from_secs(3));
        ticker.tick().await; // immediate
        let before = Instant::now();
        ticker.tick().await;
        assert_eq!(Instant::now() - before, Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn huge_durations_saturate_instead_of_panicking() {
        let far = TokioTime.deadline_in(Duration::MAX);
        assert!(far > Instant::now() + Duration::from_secs(86_400 * 365));
    }
}
