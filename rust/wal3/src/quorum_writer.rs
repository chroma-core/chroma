//! Imagine all the implications of writing your own quorum-based algorithm.
//!
//! The following problem would eventually dominate:  How do you handle partial failure of the
//! quorum writers?  A down node that affirmatively fails is relatively easy, and naive mitigation
//! of this will leave the system under-replicated.
//!
//! There's a need for a performant coordination mechanism that does the following:
//! - Given as input a set of `futures` that return a Result.
//! - Run all `futures` in parallel.
//! - Collect the first `min_futures_to_wait_for` futures' results.
//! - Start an N-second timer.
//! - Try to collect the remaining `futures.len() - min_futures_to_wait_for` futures.
//! - Stop on time-out and cancel remaining futures.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

/// Runs futures in parallel and waits for a quorum to complete.
///
/// This function executes all provided futures concurrently and:
/// 1. Waits for at least `min_futures_to_wait_for` futures to complete successfully.
/// 2. After reaching the minimum, starts a timer and attempts to collect remaining results.
/// 3. Cancels any futures that haven't completed when the timeout expires.
///
/// Returns a vector of `Option<Result<S, E>>` where:
/// - `Some(result)` indicates a future that completed (successfully or with error).
/// - `None` indicates a future that was cancelled due to timeout.
///
/// The results are returned in the same order as the input futures.
pub async fn write_quorum<S, E, F: Future<Output = Result<S, E>> + Send + 'static>(
    futures: Vec<F>,
    min_futures_to_wait_for: usize,
    timeout: Duration,
) -> Vec<Option<Result<S, E>>>
where
    S: Send + 'static,
    E: Send + 'static,
{
    let num_futures = futures.len();

    if num_futures == 0 {
        return Vec::new();
    }

    let mut results: Vec<Option<Result<S, E>>> = (0..num_futures).map(|_| None).collect();

    type IndexedFuture<S, E> = Pin<Box<dyn Future<Output = (usize, Result<S, E>)> + Send>>;
    let mut pending: FuturesUnordered<IndexedFuture<S, E>> = FuturesUnordered::new();

    for (idx, fut) in futures.into_iter().enumerate() {
        pending.push(Box::pin(async move { (idx, fut.await) }));
    }

    let mut ok_count = 0;

    // Phase 1: Wait for the minimum number of Ok futures to complete.
    while ok_count < min_futures_to_wait_for {
        if let Some((idx, result)) = pending.next().await {
            if result.is_ok() {
                ok_count += 1;
            }
            results[idx] = Some(result);
        } else {
            // All futures have completed before reaching the minimum.
            break;
        }
    }

    // Phase 2: Try to collect remaining futures within the timeout.
    if !pending.is_empty() {
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;

                maybe_result = pending.next() => {
                    match maybe_result {
                        Some((idx, result)) => {
                            results[idx] = Some(result);
                        }
                        None => {
                            // All futures completed.
                            break;
                        }
                    }
                }
                _ = &mut deadline => {
                    // Timeout reached; remaining futures will be cancelled.
                    break;
                }
            }
        }
    }

    // Dropping pending cancels any remaining futures.
    results
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::BoxFuture;

    use super::write_quorum;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn empty_futures_returns_empty_vec() {
        let futures: Vec<BoxFuture<'static, Result<(), ()>>> = vec![];
        let results = write_quorum(futures, 0, TEST_TIMEOUT).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn all_futures_complete_immediately() {
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> = vec![
            Box::pin(async { Ok(1) }),
            Box::pin(async { Ok(2) }),
            Box::pin(async { Ok(3) }),
        ];
        let results = write_quorum(futures, 2, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().as_ref().ok(), Some(&1));
        assert_eq!(results[1].as_ref().unwrap().as_ref().ok(), Some(&2));
        assert_eq!(results[2].as_ref().unwrap().as_ref().ok(), Some(&3));
    }

    #[tokio::test]
    async fn mixed_success_and_error() {
        let futures: Vec<BoxFuture<'static, Result<i32, &'static str>>> = vec![
            Box::pin(async { Ok(1) }),
            Box::pin(async { Err("error") }),
            Box::pin(async { Ok(3) }),
        ];
        let results = write_quorum(futures, 3, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 3);
        assert!(results[0].as_ref().unwrap().is_ok());
        assert!(results[1].as_ref().unwrap().is_err());
        assert!(results[2].as_ref().unwrap().is_ok());
    }

    #[tokio::test]
    async fn slow_future_cancelled_after_timeout() {
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(1)
            }),
            Box::pin(async {
                // This future takes much longer than the timeout.
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(2)
            }),
        ];
        let results = write_quorum(futures, 1, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 2);
        // First future should complete.
        assert!(results[0].is_some());
        // Second future should be cancelled (None) because it exceeds the 5s timeout.
        assert!(results[1].is_none());
    }

    #[tokio::test]
    async fn min_zero_starts_timeout_immediately() {
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(1)
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(2)
            }),
        ];
        let results = write_quorum(futures, 0, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 2);
        // Both should complete within the 5s timeout.
        assert!(results[0].is_some());
        assert!(results[1].is_some());
    }

    #[tokio::test]
    async fn preserves_order_of_results() {
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(100)
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(200)
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(300)
            }),
        ];
        let results = write_quorum(futures, 3, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 3);
        // Results should be in the original order, not completion order.
        assert_eq!(results[0].as_ref().unwrap().as_ref().ok(), Some(&100));
        assert_eq!(results[1].as_ref().unwrap().as_ref().ok(), Some(&200));
        assert_eq!(results[2].as_ref().unwrap().as_ref().ok(), Some(&300));
    }

    #[tokio::test]
    async fn only_ok_responses_count_toward_min_futures() {
        // This test verifies that only Ok responses count toward min_futures_to_wait_for.
        // We have 3 futures: 2 fast errors and 1 slow success that takes longer than the
        // provided timeout (5 seconds).
        //
        // If errors counted toward the minimum (min=2), we'd hit the quorum after the 2
        // errors complete, then the 5-second timeout would start, and the slow success
        // would be cancelled.
        //
        // Since only Ok responses should count, the 2 errors shouldn't satisfy min=2,
        // and we must continue waiting in Phase 1 until the slow success completes.
        let futures: Vec<BoxFuture<'static, Result<i32, &'static str>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Err("error1")
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Err("error2")
            }),
            Box::pin(async {
                // This takes longer than the timeout (5s).
                // If errors count toward min_futures_to_wait_for, this will be cancelled.
                tokio::time::sleep(Duration::from_secs(7)).await;
                Ok(42)
            }),
        ];
        let results = write_quorum(futures, 2, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 3);
        println!("results[0] = {:?}", results[0]);
        println!("results[1] = {:?}", results[1]);
        println!("results[2] = {:?}", results[2]);
        // The first two futures return errors.
        assert!(results[0].as_ref().unwrap().is_err());
        assert!(results[1].as_ref().unwrap().is_err());
        // The third future must complete with Ok(42) because only Ok responses count.
        // If this is None, it means the future was cancelled, indicating that errors
        // were incorrectly counted toward min_futures_to_wait_for.
        let third_result = results[2].as_ref().expect(
            "third future should complete; if None, errors are incorrectly counting toward quorum",
        );
        assert_eq!(
            third_result.as_ref().ok(),
            Some(&42),
            "only Ok responses should count toward min_futures_to_wait_for"
        );
    }

    #[tokio::test]
    async fn five_futures_min_three_with_three_errors_does_not_block() {
        // This test verifies behavior when we have 5 futures with min_futures_to_wait_for=3,
        // and 3 of them error while 2 succeed.
        //
        // Since only Ok responses count toward the quorum, the 3 errors should not satisfy
        // min=3. However, once all 5 futures complete, we should get results for all of them
        // without blocking indefinitely.
        let futures: Vec<BoxFuture<'static, Result<i32, &'static str>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Err("error1")
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Err("error2")
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Err("error3")
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(1)
            }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(2)
            }),
        ];
        let results = write_quorum(futures, 3, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 5);
        println!("results[0] = {:?}", results[0]);
        println!("results[1] = {:?}", results[1]);
        println!("results[2] = {:?}", results[2]);
        println!("results[3] = {:?}", results[3]);
        println!("results[4] = {:?}", results[4]);
        // All futures should have completed (no None values).
        assert!(results[0].is_some(), "future 0 should complete");
        assert!(results[1].is_some(), "future 1 should complete");
        assert!(results[2].is_some(), "future 2 should complete");
        assert!(results[3].is_some(), "future 3 should complete");
        assert!(results[4].is_some(), "future 4 should complete");
        // First three should be errors.
        assert!(results[0].as_ref().unwrap().is_err());
        assert!(results[1].as_ref().unwrap().is_err());
        assert!(results[2].as_ref().unwrap().is_err());
        // Last two should be Ok.
        assert_eq!(results[3].as_ref().unwrap().as_ref().ok(), Some(&1));
        assert_eq!(results[4].as_ref().unwrap().as_ref().ok(), Some(&2));
    }

    #[tokio::test]
    async fn single_future_completes() {
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> = vec![Box::pin(async { Ok(42) })];
        let results = write_quorum(futures, 1, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().as_ref().ok(), Some(&42));
    }

    #[tokio::test]
    async fn min_greater_than_futures_len_returns_all_available() {
        // When min_futures_to_wait_for exceeds the number of futures, the function should
        // still return all available results without hanging.
        let futures: Vec<BoxFuture<'static, Result<i32, ()>>> =
            vec![Box::pin(async { Ok(1) }), Box::pin(async { Ok(2) })];
        let results = write_quorum(futures, 5, TEST_TIMEOUT).await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().as_ref().ok(), Some(&1));
        assert_eq!(results[1].as_ref().unwrap().as_ref().ok(), Some(&2));
    }
}
