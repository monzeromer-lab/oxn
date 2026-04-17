//! Retry backoff strategies.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Strategy used to compute the delay before the next attempt.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use oxn::Backoff;
///
/// // Retry 5s after every failure.
/// let fixed = Backoff::Fixed { delay: Duration::from_secs(5) };
///
/// // 200ms, 400ms, 800ms, 1.6s, …, capped at 30s.
/// let exp = Backoff::Exponential {
///     initial: Duration::from_millis(200),
///     max: Some(Duration::from_secs(30)),
/// };
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Backoff {
    /// Constant `delay` between attempts.
    Fixed {
        /// Delay between retries.
        delay: Duration,
    },
    /// `initial * 2^(attempt - 1)`, optionally capped by `max`.
    Exponential {
        /// Delay before the first retry (attempt 1).
        initial: Duration,
        /// Optional upper bound on computed delays.
        max: Option<Duration>,
    },
}

impl Backoff {
    /// Helper: fixed 1-second backoff.
    pub const ONE_SECOND: Self = Self::Fixed {
        delay: Duration::from_secs(1),
    };

    /// Compute the delay for the given attempt number (1-based).
    pub fn compute(&self, attempt: u32) -> Duration {
        match *self {
            Self::Fixed { delay } => delay,
            Self::Exponential { initial, max } => {
                let shift = attempt.saturating_sub(1).min(30);
                let millis = initial
                    .as_millis()
                    .saturating_mul(1u128 << shift)
                    .min(u64::MAX as u128) as u64;
                let d = Duration::from_millis(millis);
                match max {
                    Some(cap) if d > cap => cap,
                    _ => d,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_grows_and_caps() {
        let b = Backoff::Exponential {
            initial: Duration::from_millis(100),
            max: Some(Duration::from_secs(10)),
        };
        assert_eq!(b.compute(1), Duration::from_millis(100));
        assert_eq!(b.compute(2), Duration::from_millis(200));
        assert_eq!(b.compute(4), Duration::from_millis(800));
        // Eventually hits the cap.
        assert_eq!(b.compute(20), Duration::from_secs(10));
    }

    #[test]
    fn fixed_is_constant() {
        let b = Backoff::Fixed {
            delay: Duration::from_secs(3),
        };
        assert_eq!(b.compute(1), b.compute(7));
    }
}
