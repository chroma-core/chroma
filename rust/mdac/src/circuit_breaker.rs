//! A circuit breaker is like a semaphore except instead of queueing up requests, it rejects them.
//! It is adaptive and circuit-switched so that when the system encounters saturation---as
//! determined by a standing queue---it will reject requests.
//!
//! Initialize the CircuitBreaker with a parallelism (the number of threads allowed past the
//! circuit breaker).
//!
//! To understand the intuition for why this works, consider Little's Law L = \lambda W.  The
//! system has zero control over throughput; it will be offered by the client and the only thing we
//! can do is serve or reject a request.  There is a saturation point beyond which we can do
//! neither, so we will assume that \lambda is less than this saturation point and is constant over
//! small time windows.
//!
//! Given a constant \lambda, the only way to control L is through direct influence over
//! scheduling.  Schedule more work to increase L.  Shed load to decrease L.
//!
//! The point of the circuit-breaker, then, is to adaptively decide to enqueue a request for
//! serving or reject it altogether based upon current system conditions.

use std::sync::atomic::{AtomicI64, Ordering};

/////////////////////////////////////// CircuitBreakerConfig ///////////////////////////////////////

/// A circuit breaker configuration.  The default configuration admits 0 requests, which symbolizes
/// it being disabled.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct CircuitBreakerConfig {
    /// The number of requests that can be admitted at any given time.  Set to <= 0 to disable the
    /// circuit breaker.
    pub requests: u32,
}

impl CircuitBreakerConfig {
    pub fn enabled(&self) -> bool {
        self.requests > 0
    }
}

////////////////////////////////////////// CircuitBreaker //////////////////////////////////////////

pub struct CircuitBreaker {
    count: AtomicI64,
}

impl CircuitBreaker {
    /// Construct a new circuit breaker from the configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let count = AtomicI64::new(config.requests as i64);
        Self { count }
    }

    /// Admit one request into the circuit breaker.  If the request is admitted, a ticket is
    /// returned.  If the request is not admitted, no ticket is returned.
    pub fn admit_one(&self) -> bool {
        if self.count.fetch_sub(1, Ordering::Relaxed) > 0 {
            true
        } else {
            self.count.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Release a previously returned ticket.
    pub fn release_one(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn empty() {
        let config = CircuitBreakerConfig { requests: 1 };
        let _semaphore = CircuitBreaker::new(config);
    }

    #[test]
    fn serial() {
        let config = CircuitBreakerConfig { requests: 1 };
        let semaphore = CircuitBreaker::new(config);
        let now = Instant::now();
        while now.elapsed() < Duration::from_secs(10) {
            if semaphore.admit_one() {
                semaphore.release_one();
            }
        }
    }

    #[tokio::test]
    async fn steady_state_accept() {
        // The assumption is that the semaphore under a constant load that, per Little's Law, will
        // yield 5 active requests at all times where each request takes 100ms.  The semaphore is
        // configured thresholds such that it should never load shed.
        //
        // L = 5    W = 100ms       \lambda = 50
        let config = CircuitBreakerConfig { requests: 10 };
        let (success, failure) = steady_state_test(config, 5, Duration::from_millis(100)).await;
        assert!(success > 0);
        assert_approx_eq(success, 50 * 10, 10.0);
        assert_approx_eq(failure, 0, 1.0);
    }

    #[tokio::test]
    async fn breaking_point() {
        // The assumption is that the semaphore under a constant load that, per Little's Law, will
        // yield 5 active requests at all times where each request takes 100ms.  The semaphore is
        // configured thresholds such that it should never load shed.
        //
        // L = 10   W = 100ms       \lambda = 100
        let config = CircuitBreakerConfig { requests: 10 };
        let (success, failure) = steady_state_test(config, 10, Duration::from_millis(100)).await;
        assert!(success > 0);
        assert_approx_eq(success, 100 * 10, 250.0);
        assert_approx_eq(failure, 0, 1.0);
    }

    #[tokio::test]
    async fn steady_state_reject() {
        // The assumption is that the semaphore under a constant load that, per Little's Law, will
        // yield 100 active requests at all times where each request takes 100ms.  The semaphore is
        // configured thresholds such that only 10 active requests can be in there, which means a
        // standing wave of 90 requests to time out.
        //
        // Overall:
        // L = 100   W = 100ms       \lambda = 1000
        // Goodput:
        // L = 10    W = 100ms       \lambda = 100
        // Timeouts:
        // L = 90    W ~= 1ms        \lambda ~= 90000
        let config = CircuitBreakerConfig { requests: 10 };
        let (success, failure) = steady_state_test(config, 100, Duration::from_millis(100)).await;
        assert!(success > 0);
        assert_approx_eq(success, 100 * 10, 100.0);
        assert!(failure >= 180_000);
    }

    async fn steady_state_test(
        config: CircuitBreakerConfig,
        num_tasks: usize,
        task_wait: Duration,
    ) -> (u64, u64) {
        let success = Arc::new(AtomicU64::new(0));
        let failure = Arc::new(AtomicU64::new(0));
        let semaphore = Arc::new(CircuitBreaker::new(config));
        let now = Instant::now();
        let mut tasks = vec![];
        for _ in 0..num_tasks {
            let success = Arc::clone(&success);
            let failure = Arc::clone(&failure);
            let semaphore = Arc::clone(&semaphore);
            tasks.push(tokio::spawn(async move {
                while now.elapsed() < Duration::from_secs(10) {
                    let ticket = semaphore.admit_one();
                    if ticket {
                        tokio::time::sleep(task_wait).await;
                        semaphore.release_one();
                        success.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failure.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }));
        }
        for task in tasks {
            let _ = task.await;
        }
        (
            success.load(Ordering::Relaxed),
            failure.load(Ordering::Relaxed),
        )
    }

    fn assert_approx_eq(a: u64, b: u64, epsilon: f64) {
        assert!(
            (a as f64 - b as f64).abs() < epsilon,
            "{} != {} ± {}",
            a,
            b,
            epsilon
        );
    }
}
