use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// A source of jitter: values uniformly distributed in `[0, 1)`.
///
/// Retry delays are randomised so that jobs which failed together do not all
/// come back together. Drawing that randomness through a trait (rather than
/// calling SQL `random()` or a global RNG) means a test or a simulation can
/// fix it, and a failure can be replayed exactly from its seed.
pub trait Jitter: Send + Sync + fmt::Debug + 'static {
    fn sample(&self) -> f64;
}

/// SplitMix64: a tiny, fast, well-studied generator (Steele, Lea & Flood,
/// 2014). Plenty for spreading retries out; not for anything cryptographic.
///
/// Implemented here instead of depending on `rand`, which would pull in
/// several crates to produce one float. Its state is a counter, so sampling
/// is a single atomic add: no lock, safe to share between tasks.
#[derive(Debug)]
pub struct SplitMix64 {
    state: AtomicU64,
}

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    pub const fn seeded(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Seeded from the clock and process id: different in every process,
    /// which is what production wants.
    pub fn from_entropy() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self::seeded(nanos ^ (u64::from(std::process::id()) << 32))
    }

    pub fn next_u64(&self) -> u64 {
        let mut z = self
            .state
            .fetch_add(GOLDEN_GAMMA, Ordering::Relaxed)
            .wrapping_add(GOLDEN_GAMMA);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl Jitter for SplitMix64 {
    fn sample(&self) -> f64 {
        // The top 53 bits fill an f64 mantissa exactly: uniform in [0, 1).
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Always the same value. For tests that need to know the exact delay.
#[derive(Debug, Clone, Copy)]
pub struct FixedJitter(pub f64);

impl Jitter for FixedJitter {
    fn sample(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let a = SplitMix64::seeded(42);
        let b = SplitMix64::seeded(42);
        let xs: Vec<u64> = (0..5).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..5).map(|_| b.next_u64()).collect();
        assert_eq!(xs, ys);
        assert_ne!(xs[0], xs[1]);
    }

    #[test]
    fn matches_the_published_reference_output() {
        // First outputs of SplitMix64 seeded with 0, as produced by the
        // reference C implementation.
        let rng = SplitMix64::seeded(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn samples_are_in_range_and_roughly_uniform() {
        let rng = SplitMix64::seeded(7);
        let samples: Vec<f64> = (0..10_000).map(|_| rng.sample()).collect();
        assert!(samples.iter().all(|s| (0.0..1.0).contains(s)));
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        assert!((mean - 0.5).abs() < 0.02, "mean {mean}");
        let low = samples.iter().filter(|s| **s < 0.5).count();
        assert!((4_700..5_300).contains(&low), "{low} below 0.5");
    }
}
