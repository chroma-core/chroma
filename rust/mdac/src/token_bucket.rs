//! A drainable GCRA (generic cell rate algorithm) rate limiter with token refunds.
//!
//! Instead of periodically refilling a counter, the bucket tracks a theoretical arrival time.
//! Consuming tokens advances that time; returning tokens moves it back, no earlier than now.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A thread-safe rate limiter that starts with a full burst allowance.
///
/// Share a bucket using [`std::sync::Arc`]. Operations use a compare-and-swap loop and never
/// wait for tokens to refill. Refills are computed lazily using a monotonic clock, retaining
/// nanosecond precision. Arrival times must fit within `u64::MAX` nanoseconds of construction
/// (about 584 years); drains that would exceed that horizon are rejected, never rounded down.
///
/// ```
/// use std::time::Duration;
/// use mdac::TokenBucket;
///
/// // Refill one token every 100 ms, allowing bursts of up to ten tokens.
/// let bucket = TokenBucket::new(10, Duration::from_millis(100));
/// assert!(bucket.drain(4));
/// bucket.put_back(2); // Refund unused work.
/// ```
#[derive(Debug)]
pub struct TokenBucket {
    epoch: Instant,
    capacity: u32,
    interval: u64,
    burst: u64,
    // Nanoseconds since epoch; checked arithmetic prevents wraparound from granting tokens.
    arrival: AtomicU64,
}

impl TokenBucket {
    /// Create a full bucket with `capacity` tokens and a refill interval of `interval` per token.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` or `interval` is zero, or if `capacity * interval` exceeds
    /// `u64::MAX` nanoseconds (about 584 years).
    pub fn new(capacity: u32, interval: Duration) -> Self {
        assert!(capacity > 0, "token bucket capacity must be positive");
        assert!(
            !interval.is_zero(),
            "token bucket interval must be positive"
        );
        let burst = u64::try_from(interval.as_nanos() * u128::from(capacity))
            .expect("token bucket burst duration must fit in u64 nanoseconds");
        Self {
            epoch: Instant::now(),
            capacity,
            interval: interval.as_nanos() as u64,
            burst,
            arrival: AtomicU64::new(0),
        }
    }

    /// Consume `tokens`, returning whether the entire amount was available.
    ///
    /// A rejected drain leaves the bucket unchanged. Draining zero always succeeds; draining
    /// more than the capacity always fails. To empty a full bucket, drain its capacity.
    pub fn drain(&self, tokens: u32) -> bool {
        self.put_back_and_drain(0, tokens)
    }

    /// Return `tokens` to the bucket, capped at its capacity.
    ///
    /// Excess tokens are discarded, including tokens already replenished by elapsed time.
    /// Returning zero does nothing. Callers are responsible for refunding only unused work;
    /// this operation does not track which drains have already been refunded.
    pub fn put_back(&self, tokens: u32) {
        self.put_back_and_drain(tokens, 0);
    }

    /// Atomically return `excess` tokens, then attempt to consume `need` tokens.
    ///
    /// The refund is capped at the burst capacity **before** draining, so excess credit cannot
    /// bypass the peak limit. Returns whether the entire drain succeeded. The refund is kept
    /// even when the drain fails. As with [`Self::put_back`], callers must avoid double refunds.
    ///
    /// An uncontended update uses one compare-and-swap; contention retries the whole operation.
    pub fn put_back_and_drain(&self, excess: u32, need: u32) -> bool {
        self.update(
            || u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX),
            excess,
            need,
        )
    }

    fn update(&self, now: impl Fn() -> u64, excess: u32, need: u32) -> bool {
        // GCRA represents spent tokens as time debt: max(arrival - now, 0). A full bucket has
        // no debt; an empty bucket has `burst = capacity * interval` debt. Each token costs
        // `interval` nanoseconds, so elapsed time repays debt without a refill task.
        //
        // Refunds cannot restore more than capacity. This clamp also makes multiplication safe
        // under the constructor's burst bound. An oversized drain may overflow, so keep its
        // cost optional: it must fail without preventing the refund.
        let refund = self.interval * u64::from(excess.min(self.capacity));
        let cost = self.interval.checked_mul(u64::from(need));
        let mut arrival = self.arrival.load(Ordering::Relaxed);
        loop {
            // Refresh time after every failed CAS so retries account for refill during contention.
            // Taking a clock callback also lets tests exercise exact boundaries without sleeping.
            let now = now();
            // Cap the refund at a full bucket BEFORE adding the drain. Simply netting excess
            // against need would let discarded credit fund work beyond the burst allowance.
            // Saturation prevents subtraction underflow; max(now) discards excess credit.
            // With no refund, preserve the old timestamp so a rejected drain changes nothing.
            let refunded = if excess == 0 {
                arrival
            } else {
                arrival.saturating_sub(refund).max(now)
            };
            // Idle time cannot accumulate credit beyond a full bucket, hence max(now). Reject
            // unrepresentable arrival times rather than wrap around and accidentally grant tokens.
            let drained = cost.and_then(|cost| refunded.max(now).checked_add(cost));
            // The candidate is at least now, so subtraction is safe and avoids overflowing
            // `now + burst`. A zero drain always succeeds, even at the timestamp horizon.
            let admitted = need == 0
                || (need <= self.capacity
                    && drained.is_some_and(|arrival| arrival - now <= self.burst));
            // A failed drain still commits its refund; the two steps become visible together.
            let next = if admitted && need > 0 {
                drained.expect("admitted drain fits in u64")
            } else {
                refunded
            };
            // No write is needed when the operation leaves the observed balance unchanged.
            if next == arrival {
                return admitted;
            }

            // One successful CAS commits the whole operation. On failure, recompute from the
            // winner's state instead of applying our refund twice or overwriting its drain.
            // Weak CAS may fail spuriously, which this loop also handles. Relaxed ordering is
            // sufficient: the atomic protects only this balance and publishes no other memory.
            match self.arrival.compare_exchange_weak(
                arrival,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return admitted,
                Err(actual) => arrival = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn randomized_operations_match_token_balance(
            capacity in prop_oneof![8 => 1u32..=64, 1 => Just(u32::MAX)],
            interval in prop_oneof![8 => 1u64..=1_000, 1 => any::<u64>()],
            start in prop_oneof![8 => Just(0u64), 1 => any::<u64>(), 1 => Just(u64::MAX)],
            operations in proptest::collection::vec(
                (
                    prop_oneof![8 => Just(0u64), 16 => 1u64..=1_000, 1 => any::<u64>()],
                    prop_oneof![2 => Just(0u32), 8 => 1u32..=128, 1 => Just(u32::MAX)],
                    prop_oneof![2 => Just(0u32), 8 => 1u32..=128, 1 => Just(u32::MAX)],
                ),
                1..128,
            ),
        ) {
            // Keep configurations representable, including bursts near the u64 limit.
            let interval = interval.clamp(1, u64::MAX / u64::from(capacity));
            let bucket = TokenBucket::new(capacity, Duration::from_nanos(interval));
            let interval = u128::from(interval);
            let full = u128::from(capacity) * interval;
            let mut balance = full;
            let mut now = start;

            for (step, (elapsed, excess, need)) in operations.into_iter().enumerate() {
                let previous = now;
                now = now.saturating_add(elapsed);

                // Independent reference model: store available credit, including fractional
                // tokens, rather than an arrival timestamp. u128 keeps its arithmetic exact.
                balance = (balance + u128::from(now - previous)).min(full);
                balance = (balance + u128::from(excess) * interval).min(full);
                let cost = u128::from(need) * interval;
                // In addition to available credit, the implementation requires enough timestamp
                // range to represent the resulting debt. Exhausting that range must fail closed.
                let admitted = need == 0
                    || (cost <= balance
                        && full - balance + cost <= u128::from(u64::MAX - now));
                if admitted {
                    balance -= cost;
                }

                prop_assert_eq!(
                    bucket.update(|| now, excess, need),
                    admitted,
                    "step {}, now {}, excess {}, need {}", step, now, excess, need
                );
                // Check the remaining fractional balance too: matching decisions alone could
                // miss a lost refund or refill that only affects a later request.
                let debt = u128::from(bucket.arrival.load(Ordering::Relaxed).saturating_sub(now));
                prop_assert!(debt <= full, "burst limit exceeded at step {}", step);
                prop_assert_eq!(full - debt, balance, "balance differs at step {}", step);
            }
        }
    }

    fn state() -> TokenBucket {
        TokenBucket::new(3, Duration::from_nanos(10))
    }

    #[test]
    fn burst_and_refill_boundaries() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        assert!(!bucket.update(|| 0, 0, 1));
        assert!(!bucket.update(|| 9, 0, 1));
        assert!(bucket.update(|| 10, 0, 1));
        assert!(!bucket.update(|| 19, 0, 1));
        assert!(bucket.update(|| 20, 0, 1));
    }

    #[test]
    fn rejected_drains_do_not_consume_tokens() {
        let bucket = state();
        assert!(!bucket.update(|| 0, 0, 4));
        assert!(bucket.update(|| 0, 0, 2));
        assert!(!bucket.update(|| 0, 0, 2));
        assert!(bucket.update(|| 0, 0, 1));
        assert!(bucket.update(|| 0, 0, 0));
    }

    #[test]
    fn idle_time_cannot_accumulate_more_than_capacity() {
        let bucket = state();
        assert!(bucket.update(|| 1_000, 0, 3));
        assert!(!bucket.update(|| 1_000, 0, 1));
    }

    #[test]
    fn refunds_restore_tokens_and_preserve_partial_refill() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        bucket.update(|| 5, 1, 0);
        assert!(bucket.update(|| 5, 0, 1));
        assert!(!bucket.update(|| 9, 0, 1));
        assert!(bucket.update(|| 10, 0, 1));
    }

    #[test]
    fn excess_refunds_are_capped_even_after_idle_time() {
        let bucket = state();
        for now in [0, 5, 1_000] {
            bucket.update(|| now, u32::MAX, 0);
            assert!(bucket.update(|| now, 0, 3));
            assert!(!bucket.update(|| now, 0, 1));
        }
    }

    #[test]
    fn zero_refund_does_not_change_state() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        bucket.update(|| 5, 0, 0);
        assert!(!bucket.update(|| 5, 0, 1));
        assert!(bucket.update(|| 10, 0, 1));
    }

    #[test]
    fn combined_refund_is_capped_before_drain() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        assert!(bucket.update(|| 5, u32::MAX, 2));
        assert!(bucket.update(|| 5, 0, 1));
        assert!(!bucket.update(|| 5, 0, 1));
        assert!(!bucket.update(|| 14, 0, 1));
        assert!(bucket.update(|| 15, 0, 1));
    }

    #[test]
    fn failed_combined_drain_keeps_refund() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        assert!(!bucket.update(|| 0, 1, 2));
        assert!(bucket.update(|| 0, 0, 1));
        assert!(!bucket.update(|| 0, u32::MAX, 4));
        assert!(bucket.update(|| 0, 0, 3));
    }

    #[test]
    fn combined_refund_preserves_partial_refill() {
        let bucket = state();
        assert!(bucket.update(|| 0, 0, 3));
        assert!(bucket.update(|| 5, 2, 1));
        assert!(bucket.update(|| 9, 0, 1));
        assert!(!bucket.update(|| 9, 0, 1));
        assert!(bucket.update(|| 10, 0, 1));
    }

    #[test]
    fn combined_operations_match_sequential_operations() {
        for spent in 0..=3 {
            for now in [0, 5, 10, 100] {
                for excess in 0..=5 {
                    for need in 0..=5 {
                        let combined = state();
                        let sequential = state();
                        assert!(combined.update(|| 0, 0, spent));
                        assert!(sequential.update(|| 0, 0, spent));
                        sequential.update(|| now, excess, 0);
                        assert_eq!(
                            combined.update(|| now, excess, need),
                            sequential.update(|| now, 0, need)
                        );
                        assert_eq!(
                            combined.arrival.load(Ordering::Relaxed),
                            sequential.arrival.load(Ordering::Relaxed)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn concurrent_combined_operations_preserve_balance() {
        let bucket = TokenBucket::new(100, Duration::from_secs(3_600));
        assert!(bucket.drain(100));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        assert!(bucket.put_back_and_drain(2, 1));
                        assert!(bucket.drain(1));
                    }
                });
            }
        });
        assert!(!bucket.drain(1));
    }

    #[test]
    fn largest_configuration_does_not_overflow() {
        let bucket = TokenBucket::new(u32::MAX, Duration::from_nanos(u64::MAX / u32::MAX as u64));
        assert!(bucket.update(|| 0, 0, u32::MAX));
        assert!(!bucket.update(|| 0, 0, 1));
        assert!(bucket.update(|| 0, u32::MAX, u32::MAX));
    }

    #[test]
    fn overflow_rejects_drain_but_keeps_refund() {
        let bucket = TokenBucket::new(1, Duration::from_nanos(u64::MAX));
        assert!(bucket.update(|| 0, 0, 1));
        assert!(!bucket.update(|| 1, 1, 1));
        assert_eq!(bucket.arrival.load(Ordering::Relaxed), 1);
        assert!(!bucket.update(|| 1, 0, u32::MAX));
        assert_eq!(bucket.arrival.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn clock_horizon_does_not_grant_tokens() {
        let bucket = state();
        assert!(bucket.update(|| u64::MAX - 30, 0, 3));
        assert!(!bucket.update(|| u64::MAX - 20, 0, 1));
        assert!(bucket.update(|| u64::MAX - 20, 1, 1));
        assert!(!bucket.update(|| u64::MAX, u32::MAX, 1));
        assert!(bucket.update(|| u64::MAX, 0, 0));
        assert!(!bucket.update(|| u64::MAX, 0, 1));
    }

    #[test]
    fn nanosecond_precision_is_preserved() {
        let bucket = TokenBucket::new(1, Duration::from_nanos(1));
        assert!(bucket.update(|| 0, 0, 1));
        assert!(!bucket.update(|| 0, 0, 1));
        assert!(bucket.update(|| 1, 0, 1));
        assert!(!bucket.update(|| 1, 0, 1));
    }

    #[test]
    #[should_panic(expected = "burst duration must fit in u64 nanoseconds")]
    fn unrepresentable_interval_is_invalid() {
        TokenBucket::new(1, Duration::MAX);
    }

    #[test]
    #[should_panic(expected = "burst duration must fit in u64 nanoseconds")]
    fn unrepresentable_burst_is_invalid() {
        TokenBucket::new(2, Duration::from_nanos(u64::MAX));
    }

    #[test]
    fn concurrent_drains_share_one_allowance() {
        let bucket = TokenBucket::new(100, Duration::from_secs(3_600));
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| (0..100).filter(|_| bucket.drain(1)).count()))
                .collect();
            let admitted: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(admitted, 100);
        });
        bucket.put_back(100);
        assert!(bucket.drain(100));
    }

    #[test]
    #[should_panic(expected = "capacity must be positive")]
    fn zero_capacity_is_invalid() {
        TokenBucket::new(0, Duration::from_secs(1));
    }

    #[test]
    #[should_panic(expected = "interval must be positive")]
    fn zero_interval_is_invalid() {
        TokenBucket::new(1, Duration::ZERO);
    }
}
