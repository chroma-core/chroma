//! Rate-limiting primitives.
//!
//! `TokenBucket` is the per-client steady-state pacing limiter: takes one
//! token per request, refills at `rps` tokens/second.
//!
//! `RateLimitGate` is the *shared cooldown*: when any in-flight request
//! sees a 429, it bumps a global "do not start a new request before X"
//! deadline. All other workers respect that deadline before doing anything,
//! so a single overload causes one cooldown rather than N concurrent
//! retries.
//!
//! `RateLimitedError` carries the server's `Retry-After` (or our default
//! backoff) so callers can decide whether to retry or give up.

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time::sleep;

use crate::{DEFAULT_POLL_RPS, DEFAULT_RPS};

const DEFAULT_BACKOFF_INITIAL_S: f64 = 5.0;
pub const MAX_BACKOFF_S: f64 = 60.0;

#[derive(Debug, Error)]
#[error("rate-limited (HTTP 429); retry_after={retry_after:.1}s body={body}")]
pub struct RateLimitedError {
    pub retry_after: f64,
    pub body: String,
}

impl RateLimitedError {
    pub fn from_response_parts(
        retry_after_header: Option<&str>,
        body: String,
        default_backoff: f64,
    ) -> Self {
        let retry_after = retry_after_header
            .and_then(|h| h.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(default_backoff);
        Self { retry_after, body }
    }
}

#[derive(Debug)]
struct BucketInner {
    capacity: f64,
    tokens: f64,
    rps: f64,
    last_refill: Instant,
}

#[derive(Debug, Clone)]
pub struct TokenBucket {
    inner: Arc<Mutex<BucketInner>>,
}

impl TokenBucket {
    pub fn new(rps: f64) -> Self {
        let rps = rps.max(0.0);
        let capacity = (rps.max(1.0)).max(1.0);
        Self {
            inner: Arc::new(Mutex::new(BucketInner {
                capacity,
                tokens: capacity,
                rps,
                last_refill: Instant::now(),
            })),
        }
    }

    #[allow(dead_code)]
    pub fn for_default_rps() -> Self {
        Self::new(DEFAULT_RPS)
    }

    #[allow(dead_code)]
    pub fn for_default_poll_rps() -> Self {
        Self::new(DEFAULT_POLL_RPS)
    }

    /// Block (asynchronously) until at least one token is available, then
    /// take it. Returns immediately if `rps == 0` (rate limit disabled).
    pub async fn take(&self) {
        loop {
            let wait = {
                let mut g = self.inner.lock();
                if g.rps <= 0.0 {
                    return;
                }
                let now = Instant::now();
                let dt = now.duration_since(g.last_refill).as_secs_f64();
                g.tokens = (g.tokens + dt * g.rps).min(g.capacity);
                g.last_refill = now;
                if g.tokens >= 1.0 {
                    g.tokens -= 1.0;
                    return;
                }
                let need = 1.0 - g.tokens;
                Duration::from_secs_f64(need / g.rps)
            };
            sleep(wait).await;
        }
    }
}

#[derive(Debug, Default)]
struct GateInner {
    /// Earliest wall time at which the next request may proceed.
    until: Option<Instant>,
    /// How many times we've tripped the gate (for stats).
    trips: u64,
    /// Cumulative wall-time spent waiting on the gate (for stats).
    total_wait: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct RateLimitGate {
    inner: Arc<Mutex<GateInner>>,
}

impl RateLimitGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trip(&self, retry_after: f64) {
        let mut g = self.inner.lock();
        let until = Instant::now() + Duration::from_secs_f64(retry_after.max(0.5));
        g.until = match g.until {
            Some(prev) if prev > until => Some(prev),
            _ => Some(until),
        };
        g.trips += 1;
    }

    pub async fn wait_if_open(&self) {
        loop {
            let now = Instant::now();
            let (until, _) = {
                let g = self.inner.lock();
                (g.until, g.trips)
            };
            match until {
                Some(t) if t > now => {
                    let dur = t - now;
                    {
                        let mut g = self.inner.lock();
                        g.total_wait += dur;
                    }
                    sleep(dur).await;
                }
                _ => return,
            }
        }
    }

    #[allow(dead_code)]
    pub fn trips(&self) -> u64 {
        self.inner.lock().trips
    }
    #[allow(dead_code)]
    pub fn total_wait_secs(&self) -> f64 {
        self.inner.lock().total_wait.as_secs_f64()
    }
}

pub fn default_initial_backoff_s() -> f64 {
    DEFAULT_BACKOFF_INITIAL_S
}
