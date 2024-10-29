use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

//////////////////////////////////////// ExponentialBackoff ////////////////////////////////////////

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
        let elapsed = self.start.elapsed();
        let recovery_window = Duration::from_micros(
            (elapsed.as_micros() as f64 * self.throughput_ops_sec / self.reserve_capacity) as u64,
        );
        let s = RandomState::new();
        let random = s.hash_one(Instant::now());
        let ratio = (random & 0x1fffffffffffffu64) as f64 / (1u64 << f64::MANTISSA_DIGITS) as f64;
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
