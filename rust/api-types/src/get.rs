use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

pub const STALE_READ_ERROR_NAME: &str = "StaleReadError";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OccReadToken {
    log_upper_bound_offset: u64,
}

impl OccReadToken {
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

    pub fn log_upper_bound_offset(self) -> u64 {
        self.log_upper_bound_offset
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OccReadMode {
    #[default]
    None,
    Capture,
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
