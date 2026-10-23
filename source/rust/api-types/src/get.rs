use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

pub const STALE_READ_ERROR_NAME: &str = "StaleReadError";

/// Identifies the log upper bound that defines an OCC read snapshot.
///
/// A token is captured from a read that has observed a concrete log upper
/// bound. Later reads can use the token to execute against the same logical
/// snapshot, provided the referenced log range is still materializable.
///
/// The offset is always non-zero. Offset zero is reserved for reads that are
/// not pinned to a caller-visible OCC snapshot.
///
/// # Examples
///
/// ```rust
/// use chroma_api_types::{OccReadToken, StaleReadError};
///
/// let token = OccReadToken::try_new(42).unwrap();
/// assert_eq!(token.log_upper_bound_offset(), 42);
///
/// assert_eq!(
///     OccReadToken::try_new(0),
///     Err(StaleReadError::InvalidReadToken {
///         log_upper_bound_offset: 0,
///     }),
/// );
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OccReadToken {
    log_upper_bound_offset: u64,
}

impl OccReadToken {
    /// Create a read token for a non-zero log upper bound.
    ///
    /// # Errors
    ///
    /// Returns [`StaleReadError::InvalidReadToken`] when
    /// `log_upper_bound_offset` is zero.
    pub fn try_new(log_upper_bound_offset: u64) -> Result<Self, StaleReadError> {
        if log_upper_bound_offset == 0 {
            return Err(StaleReadError::InvalidReadToken {
                log_upper_bound_offset,
            });
        }
        Ok(Self {
            log_upper_bound_offset,
        })
    }

    /// Return the log upper bound that defines this token's snapshot.
    pub fn log_upper_bound_offset(self) -> u64 {
        self.log_upper_bound_offset
    }
}

/// Selects how a `get` read participates in OCC snapshot handling.
///
/// Normal reads do not expose an OCC token. Token-capturing reads execute at
/// the currently scouted log upper bound and return an [`OccReadToken`] to the
/// internal transaction plumbing. Token-pinned reads execute at the snapshot
/// named by a previously captured token.
///
/// A captured or supplied token can become stale if compaction or log purging
/// removes data needed to materialize the snapshot; those cases are reported
/// as [`StaleReadError`] by the frontend.
///
/// # Examples
///
/// ```rust
/// use chroma_api_types::{OccReadMode, OccReadToken};
///
/// assert_eq!(OccReadMode::default(), OccReadMode::None);
///
/// let token = OccReadToken::try_new(42).unwrap();
/// let mode = OccReadMode::AtToken(token);
/// assert!(matches!(mode, OccReadMode::AtToken(read_token) if read_token == token));
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OccReadMode {
    /// Execute a normal read without producing or consuming an OCC read token.
    #[default]
    None,
    /// Execute at the current scouted log upper bound and capture a token for it.
    Capture,
    /// Execute at the snapshot identified by the supplied read token.
    AtToken(OccReadToken),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StaleReadError {
    #[error("transactional reads require log scouting/read-token generation to be enabled")]
    ReadTokenGenerationDisabled,
    #[error(
        "transactional read token generation returned invalid log upper bound offset {log_upper_bound_offset}"
    )]
    InvalidReadToken { log_upper_bound_offset: u64 },
    #[error(
        "read token at log upper bound offset {log_upper_bound_offset} is too old to materialize because the collection is compacted through log position {collection_log_position}"
    )]
    VersionTooOld {
        log_upper_bound_offset: u64,
        collection_log_position: i64,
    },
    #[error(
        "read token at log upper bound offset {log_upper_bound_offset} can no longer be materialized: {reason}"
    )]
    VersionPurged {
        log_upper_bound_offset: u64,
        reason: String,
    },
}

impl StaleReadError {
    pub fn version_too_old(log_upper_bound_offset: u64, collection_log_position: i64) -> Self {
        Self::VersionTooOld {
            log_upper_bound_offset,
            collection_log_position,
        }
    }

    pub fn version_purged(log_upper_bound_offset: u64, reason: impl Into<String>) -> Self {
        Self::VersionPurged {
            log_upper_bound_offset,
            reason: reason.into(),
        }
    }
}

impl ChromaError for StaleReadError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::FailedPrecondition
    }
}
