use chroma_error::{ChromaError, ErrorCodes};

/// Errors local to the mullet reverse-proxy. `AuthError`, scorecard
/// rate-limit, and other shared errors flow through `ServerError` via
/// existing `ChromaError` impls; this enum covers the two failure modes
/// unique to the proxy itself: a body the proxy can't merge `user` into,
/// and an upstream/mullet network failure.
///
/// Note: dashboard-api returns 502 for upstream failures, but the Rust
/// `ChromaError` ladder has no 502 — `Unavailable` maps to 503. The
/// divergence is intentional and called out in the PR.
#[derive(Debug, thiserror::Error)]
pub(super) enum MulletProxyError {
    #[error("invalid JSON body: {0}")]
    InvalidBody(String),
    #[error("mullet upstream unavailable")]
    Upstream,
}

impl ChromaError for MulletProxyError {
    fn code(&self) -> ErrorCodes {
        match self {
            MulletProxyError::InvalidBody(_) => ErrorCodes::InvalidArgument,
            MulletProxyError::Upstream => ErrorCodes::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_map_to_expected_chroma_errors() {
        assert_eq!(
            MulletProxyError::InvalidBody("x".to_string()).code(),
            ErrorCodes::InvalidArgument
        );
        assert_eq!(MulletProxyError::Upstream.code(), ErrorCodes::Unavailable);
    }
}
