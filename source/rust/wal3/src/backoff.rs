//! A perfect backoff algorithm.
//!
//! This algorithm is based upon the following insight:  The integral of system headroom across the
//! recovery window must be at least as large as the integral of the system downtime during an
//! outage.
//!
//! It looks like this:
//! ```text
//! │
//! │                            HHHHHHHHHHHHHHHHHHHHH
//! │                            HHHHHHHHHHHHHHHHHHHHH
//! ├────────────┐              ┌─────────────────────
//! │            │DDDDDDDDDDDDDD│          
//! │            │DDDDDDDDDDDDDD│          
//! │            │DDDDDDDDDDDDDD│          
//! │            └──────────────┘          
//! └────────────────────────────────────────────────
//! ```
//!
//! The area of downtime, D, must be less than or equal to the area of headroom, H, for the system
//! to be able to absorb the downtime.  If t_D is the duration of downtime, t_R is the duration
//! of recovery, T_N the nominal throughput of the system and T_H the throughput kept in reserve as
//! headroom, we can say t_D * T_N = t_R * T_H, or t_R = t_D * T_N / T_H.
//!
//! This module provides an `ExponentialBackoff` struct that implements an exponential backoff
//! algorithm based on this insight.
//!
//! Here is an example that shows how to use this struct:
//!
//! ```ignore
//! let exp_backoff = ExponentialBackoff::new(1_000.0, 100.0);
//! loop {
//!     let result = match try_some_operation().await {
//!         Ok(result) => break result,
//!         Err(e) => {
//!             if e.is_recoverable() {
//!                 tokio::time::sleep(exp_backoff.next()).await;
//!             } else {
//!                 return Err(e);
//!             }
//!         }
//!     };
//!     // process the result
//! }
//! ```

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

//////////////////////////////////////// ExponentialBackoff ////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    throughput_ops_sec: f64,
    reserve_capacity: f64,
    start: Instant,
}

impl ExponentialBackoff {
    pub fn new(throughput_ops_sec: impl Into<f64>, reserve_capacity: impl Into<f64>) -> Self {
        let throughput_ops_sec = throughput_ops_sec.into();
        let reserve_capacity = reserve_capacity.into();
        Self {
            throughput_ops_sec,
            reserve_capacity,
            start: Instant::now(),
        }
    }

    pub fn next(&self) -> Duration {
        // Figure out the recovery window
        let elapsed = self.start.elapsed();
        // The units on throughput_ops_sec and reserve_capacity cancel out, so we simply scale
        // elapsed.as_micros() by the ratio of the two.
        let recovery_window = Duration::from_micros(
            (elapsed.as_micros() as f64 * self.throughput_ops_sec / self.reserve_capacity) as u64,
        );
        // Use the hash table's random state to hash the current time to get a random number.
        let s = RandomState::new();
        let random = s.hash_one(Instant::now());
        // Scale the random number to be between 0 and 1.
        let ratio = (random & 0x1fffffffffffffu64) as f64 / (1u64 << f64::MANTISSA_DIGITS) as f64;
        // Scale the recovery window by the random number.
        Duration::from_micros((recovery_window.as_micros() as f64 * ratio) as u64)
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_exponential_backoff() {
        let exp_backoff = ExponentialBackoff::new(1_000.0, 100.0);
        assert!(exp_backoff.next() < Duration::from_secs(1));
        assert!(exp_backoff.next() < Duration::from_secs(1));
        assert!(exp_backoff.next() < Duration::from_secs(1));
        std::thread::sleep(Duration::from_secs(10));
        let mut durations = (0..100).map(|_| exp_backoff.next()).collect::<Vec<_>>();
        durations.sort();
        assert!(
            durations.iter().sum::<Duration>() / durations.len() as u32 > Duration::from_secs(10)
        );
    }
}
