use chroma_error::{ChromaError, ErrorCodes};

/// Errors local to the `/api/ask` reverse-proxy. `AuthError`, scorecard
/// rate-limit, and other shared errors flow through `ServerError` via
/// existing `ChromaError` impls; this enum covers the three failure modes
/// unique to the proxy: a body the proxy can't merge `user` into, an
/// upstream/Modal network failure, and a missing config field (endpoint
/// URL or Modal credential env var).
#[derive(Debug, thiserror::Error)]
pub(super) enum AskProxyError {
    #[error("invalid JSON body: {0}")]
    InvalidBody(String),
    #[error("Modal upstream unavailable")]
    Upstream,
    #[error("{0} is not configured")]
    MissingConfig(&'static str),
}

impl ChromaError for AskProxyError {
    fn code(&self) -> ErrorCodes {
        match self {
            AskProxyError::InvalidBody(_) => ErrorCodes::InvalidArgument,
            AskProxyError::Upstream => ErrorCodes::Unavailable,
            AskProxyError::MissingConfig(_) => ErrorCodes::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_map_to_expected_chroma_errors() {
        assert_eq!(
            AskProxyError::InvalidBody("x".to_string()).code(),
            ErrorCodes::InvalidArgument
        );
        assert_eq!(AskProxyError::Upstream.code(), ErrorCodes::Unavailable);
        assert_eq!(
            AskProxyError::MissingConfig("foo").code(),
            ErrorCodes::Internal
        );
    }
}
