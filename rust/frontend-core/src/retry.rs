//! Retry policy for transient sysdb failures.
//!
//! Setup operations (creating databases, collections, and attached functions)
//! talk to sysdb over gRPC. A momentary backend hiccup — the service briefly
//! `UNAVAILABLE`, an aborted Spanner transaction, a missed deadline — should
//! not fail the whole operation. [`retry_transient`] rides those out with
//! bounded exponential backoff while failing fast on deterministic
//! client/logic errors (invalid argument, not found, already exists, …).
//!
//! Span instrumentation stays the caller's concern: callers wrap the operation
//! in a `#[tracing::instrument]` span and the retry loop runs inside it (so the
//! span covers every attempt and gives each retry warning its context). The
//! loop itself only emits a `warn!` per retry via `.notify`, matching the
//! workspace convention of driving `backon` directly with a small
//! transient-error predicate plus a notify hook (cf. `is_retryable_error` and
//! the `.notify` retry logs in `frontend/src/executor/distributed.rs` and
//! `frontend/src/impls/service_based_frontend.rs`).
//!
//! Note the sysdb *client* does not retry; only the sysdb *server* retries its
//! Spanner transactions. This loop adds the transport-level coverage (a dropped
//! RPC, sysdb restarting) the server-side retries can't see. Callers must
//! ensure the wrapped operation is idempotent, since it may run more than once.

use std::future::Future;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use chroma_error::{ChromaError, ErrorCodes};

/// Retries after the initial attempt before giving up.
const MAX_RETRIES: usize = 3;
/// Delay before the first retry; doubles each attempt up to [`MAX_DELAY`].
const MIN_DELAY: Duration = Duration::from_millis(200);
/// Ceiling on the backoff delay between attempts.
const MAX_DELAY: Duration = Duration::from_secs(5);

/// Whether a gRPC error code reflects a transient backend condition worth
/// retrying rather than a deterministic failure. Use as the `backon`
/// `.when(|e| is_transient(e.code()))` predicate.
pub fn is_transient(code: ErrorCodes) -> bool {
    matches!(
        code,
        ErrorCodes::Unavailable | ErrorCodes::Aborted | ErrorCodes::DeadlineExceeded
    )
}

/// Bounded exponential backoff (with jitter) for transient sysdb retries.
fn transient_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_max_times(MAX_RETRIES)
        .with_min_delay(MIN_DELAY)
        .with_max_delay(MAX_DELAY)
        .with_jitter()
}

/// Run an idempotent async sysdb `operation`, retrying only
/// [transient](is_transient) failures with [`transient_backoff`] and emitting a
/// `warn!` per retry (within the caller's span). Errors are returned unchanged
/// once retries are exhausted (or immediately for non-transient errors), so
/// callers keep their typed error and existing handling (e.g. mapping
/// `AlreadyExists` to success).
pub async fn retry_transient<T, E, F, Fut>(operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: ChromaError,
{
    operation
        .retry(transient_backoff())
        .when(|err: &E| is_transient(err.code()))
        .notify(|err: &E, delay: Duration| {
            tracing::warn!(
                error = %err,
                retry_after_ms = delay.as_millis() as u64,
                "retrying transient sysdb failure",
            );
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_codes_are_retried() {
        for code in [
            ErrorCodes::Unavailable,
            ErrorCodes::Aborted,
            ErrorCodes::DeadlineExceeded,
        ] {
            assert!(is_transient(code), "{code:?} should be retried");
        }
    }

    #[test]
    fn deterministic_codes_are_not_retried() {
        for code in [
            ErrorCodes::InvalidArgument,
            ErrorCodes::NotFound,
            ErrorCodes::AlreadyExists,
            ErrorCodes::PermissionDenied,
            ErrorCodes::Internal,
            ErrorCodes::Unauthenticated,
        ] {
            assert!(!is_transient(code), "{code:?} should not be retried");
        }
    }
}
