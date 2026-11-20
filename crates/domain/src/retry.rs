use std::time::Duration;

/// Exponential backoff with equal jitter.
///
/// After the `n`-th failure of a job the retry is delayed by a value drawn
/// uniformly from `[ceiling(n) / 2, ceiling(n)]`, where
/// `ceiling(n) = min(cap, base * 2^(n-1))`.
///
/// The lower half guarantees a minimum pause, so a failing dependency is not
/// hit again immediately; the random upper half spreads retries out, so jobs
/// that failed together do not all come back together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub base: Duration,
    pub cap: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(300),
        }
    }
}

impl RetryPolicy {
    pub const fn new(base: Duration, cap: Duration) -> Self {
        Self { base, cap }
    }

    /// Upper bound of the delay after the `failures`-th failure (1-based).
    /// Never exceeds `cap`, and never overflows however large `failures` is.
    pub fn ceiling(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(64);
        let nanos = self
            .base
            .as_nanos()
            .checked_shl(doublings)
            .filter(|shifted| shifted >> doublings == self.base.as_nanos())
            .unwrap_or(u128::MAX)
            .min(self.cap.as_nanos());
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// The delay for a given jitter sample in `[0, 1]` (clamped).
    ///
    /// This is the reference definition. The storage layer applies the same
    /// formula inside SQL, with `random()` as the jitter source, so that each
    /// row gets its own sample even when many jobs are rescheduled by one
    /// statement.
    pub fn delay(&self, failures: u32, jitter: f64) -> Duration {
        let jitter = if jitter.is_finite() {
            jitter.clamp(0.0, 1.0)
        } else {
            1.0
        };
        let half = self.ceiling(failures) / 2;
        half + half.mul_f64(jitter)
    }

    /// `ceiling(1)..=ceiling(count)` in seconds, for the storage layer.
    pub fn ceilings_secs(&self, count: u32) -> Vec<f64> {
        (1..=count).map(|n| self.ceiling(n).as_secs_f64()).collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn doubles_from_base_up_to_cap() {
        let policy = RetryPolicy::new(Duration::from_secs(2), Duration::from_secs(30));
        let secs: Vec<u64> = (0..=7).map(|n| policy.ceiling(n).as_secs()).collect();
        // failures = 0 is treated like the first failure.
        assert_eq!(secs, [2, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(policy.ceilings_secs(3), [2.0, 4.0, 8.0]);
    }

    #[test]
    fn delay_spans_the_upper_half_of_the_ceiling() {
        let policy = RetryPolicy::new(Duration::from_secs(4), Duration::from_secs(60));
        assert_eq!(policy.delay(1, 0.0), Duration::from_secs(2));
        assert_eq!(policy.delay(1, 0.5), Duration::from_secs(3));
        assert_eq!(policy.delay(1, 1.0), Duration::from_secs(4));
        // Garbage jitter is clamped, not propagated.
        assert_eq!(policy.delay(1, -3.0), Duration::from_secs(2));
        assert_eq!(policy.delay(1, f64::NAN), Duration::from_secs(4));
    }

    fn policies() -> impl Strategy<Value = RetryPolicy> {
        (0u64..=3_600_000, 0u64..=86_400_000).prop_map(|(base_ms, cap_ms)| {
            RetryPolicy::new(
                Duration::from_millis(base_ms),
                Duration::from_millis(cap_ms),
            )
        })
    }

    proptest! {
        #[test]
        fn ceiling_never_exceeds_the_cap(policy in policies(), failures in any::<u32>()) {
            prop_assert!(policy.ceiling(failures) <= policy.cap);
        }

        #[test]
        fn ceiling_never_shrinks_as_failures_grow(policy in policies(), failures in 0u32..10_000) {
            prop_assert!(policy.ceiling(failures) <= policy.ceiling(failures + 1));
        }

        #[test]
        fn first_ceiling_is_the_base_unless_capped(policy in policies()) {
            prop_assert_eq!(policy.ceiling(1), policy.base.min(policy.cap));
        }

        #[test]
        fn delay_is_within_the_upper_half(
            policy in policies(),
            failures in any::<u32>(),
            jitter in 0.0f64..=1.0,
        ) {
            let ceiling = policy.ceiling(failures);
            let delay = policy.delay(failures, jitter);
            prop_assert!(delay >= ceiling / 2);
            prop_assert!(delay <= ceiling);
        }

        #[test]
        fn more_jitter_never_means_less_delay(
            policy in policies(),
            failures in 0u32..64,
            a in 0.0f64..=1.0,
            b in 0.0f64..=1.0,
        ) {
            let (low, high) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(policy.delay(failures, low) <= policy.delay(failures, high));
        }
    }
}
