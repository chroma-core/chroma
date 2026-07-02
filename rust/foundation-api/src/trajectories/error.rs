use std::error::Error;
use std::fmt;
use std::num::TryFromIntError;
use std::str::Utf8Error;

use chroma::client::ChromaHttpClientError;
use chroma_error::{ChromaError, ErrorCodes};
use uuid::Uuid;

use super::model::WriteState;

/// Reports a failure to encode, validate, persist, or rehydrate trajectory data.
#[derive(Debug)]
pub enum TrajectoryError {
    /// Wraps JSON serialization or deserialization failure.
    Json(serde_json::Error),
    /// Wraps a Chroma client failure.
    Chroma(ChromaHttpClientError),
    /// Wraps invalid UTF-8 in data that must be stored as a Chroma document.
    Utf8(Utf8Error),
    /// Wraps an integer conversion that would lose information.
    IntConversion(TryFromIntError),
    /// Indicates that a required generated Chroma key is absent.
    MissingKey(String),
    /// Indicates that no trajectory exists for the requested UUID.
    NotFound {
        /// UUID of the missing trajectory.
        tid: Uuid,
    },
    /// Indicates that a create-only trajectory write saw existing records.
    AlreadyExists {
        /// UUID of the existing trajectory.
        tid: Uuid,
    },
    /// Indicates that a path UUID and body UUID identify different trajectories.
    IdMismatch {
        /// UUID supplied by the route path.
        path: Uuid,
        /// UUID supplied in the request body.
        body: Uuid,
    },
    /// Indicates that an append request did not contain any complete entries.
    EmptyAppend {
        /// UUID of the trajectory being appended.
        tid: Uuid,
    },
    /// Indicates that a generated or loaded Chroma key violates the schema.
    InvalidKey(String),
    /// Indicates that a stored JSON value violates the trajectory schema.
    InvalidValue(String),
    /// Indicates that a key or document exceeds its configured byte budget.
    SizeLimit(String),
    /// Indicates that a loaded chunkset digest does not match its content.
    HashMismatch {
        /// Base key of the chunkset whose digest failed validation.
        base_key: String,
        /// Digest recorded in the chunkset metadata.
        expected: String,
        /// Digest computed from the loaded chunks.
        actual: String,
    },
    /// Indicates that an open trajectory was loaded through a finalized-only path.
    FinalizedRequired {
        /// UUID of the trajectory that is still open.
        tid: Uuid,
    },
    /// Indicates that a write requiring an open trajectory saw another state.
    NotOpen {
        /// UUID of the trajectory that was not open.
        tid: Uuid,
        /// Persisted write state of the trajectory.
        write_state: WriteState,
    },
    /// Indicates that an optimistic entry-count check failed.
    EntryCountMismatch {
        /// UUID of the trajectory whose entry count was checked.
        tid: Uuid,
        /// Entry count required by the caller or persisted header.
        expected: usize,
        /// Entry count observed in the input or persisted header.
        actual: usize,
    },
}

impl fmt::Display for TrajectoryError {
    /// Render the error with its durable context.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrajectoryError::Json(err) => write!(f, "json error: {err}"),
            TrajectoryError::Chroma(err) => write!(f, "chroma error: {err}"),
            TrajectoryError::Utf8(err) => write!(f, "utf8 error: {err}"),
            TrajectoryError::IntConversion(err) => write!(f, "integer conversion error: {err}"),
            TrajectoryError::MissingKey(key) => write!(f, "missing key: {key}"),
            TrajectoryError::NotFound { tid } => write!(f, "trajectory {tid} not found"),
            TrajectoryError::AlreadyExists { tid } => {
                write!(f, "trajectory {tid} already exists")
            }
            TrajectoryError::IdMismatch { path, body } => write!(
                f,
                "trajectory id mismatch: path id {path} does not match body id {body}"
            ),
            TrajectoryError::EmptyAppend { tid } => {
                write!(f, "trajectory {tid} append requires at least one entry")
            }
            TrajectoryError::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            TrajectoryError::InvalidValue(msg) => write!(f, "invalid value: {msg}"),
            TrajectoryError::SizeLimit(msg) => write!(f, "size limit exceeded: {msg}"),
            TrajectoryError::HashMismatch {
                base_key,
                expected,
                actual,
            } => write!(
                f,
                "chunkset hash mismatch for {base_key}: expected {expected}, got {actual}"
            ),
            TrajectoryError::FinalizedRequired { tid } => {
                write!(
                    f,
                    "trajectory {tid} is open but finalized data was required"
                )
            }
            TrajectoryError::NotOpen { tid, write_state } => {
                write!(f, "trajectory {tid} is not open: {write_state:?}")
            }
            TrajectoryError::EntryCountMismatch {
                tid,
                expected,
                actual,
            } => write!(
                f,
                "trajectory {tid} entry count mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl Error for TrajectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TrajectoryError::Json(err) => Some(err),
            TrajectoryError::Chroma(err) => Some(err),
            TrajectoryError::Utf8(err) => Some(err),
            TrajectoryError::IntConversion(err) => Some(err),
            TrajectoryError::MissingKey(_)
            | TrajectoryError::NotFound { .. }
            | TrajectoryError::AlreadyExists { .. }
            | TrajectoryError::IdMismatch { .. }
            | TrajectoryError::EmptyAppend { .. }
            | TrajectoryError::InvalidKey(_)
            | TrajectoryError::InvalidValue(_)
            | TrajectoryError::SizeLimit(_)
            | TrajectoryError::HashMismatch { .. }
            | TrajectoryError::FinalizedRequired { .. }
            | TrajectoryError::NotOpen { .. }
            | TrajectoryError::EntryCountMismatch { .. } => None,
        }
    }
}

impl ChromaError for TrajectoryError {
    fn code(&self) -> ErrorCodes {
        match self {
            TrajectoryError::AlreadyExists { .. } => ErrorCodes::AlreadyExists,
            TrajectoryError::NotFound { .. } => ErrorCodes::NotFound,
            TrajectoryError::NotOpen { .. }
            | TrajectoryError::EntryCountMismatch { .. }
            | TrajectoryError::FinalizedRequired { .. } => ErrorCodes::FailedPrecondition,
            TrajectoryError::Chroma(_) => ErrorCodes::Internal,
            TrajectoryError::Json(_)
            | TrajectoryError::Utf8(_)
            | TrajectoryError::IntConversion(_)
            | TrajectoryError::MissingKey(_)
            | TrajectoryError::IdMismatch { .. }
            | TrajectoryError::EmptyAppend { .. }
            | TrajectoryError::InvalidKey(_)
            | TrajectoryError::InvalidValue(_)
            | TrajectoryError::SizeLimit(_)
            | TrajectoryError::HashMismatch { .. } => ErrorCodes::InvalidArgument,
        }
    }
}

impl TrajectoryError {
    /// Whether this error wraps a proxied FE 404 from a collection-id route.
    pub(crate) fn is_chroma_not_found(&self) -> bool {
        matches!(
            self,
            TrajectoryError::Chroma(ChromaHttpClientError::ApiError(_, status))
                if *status == reqwest::StatusCode::NOT_FOUND
        )
    }
}

impl From<serde_json::Error> for TrajectoryError {
    /// Preserve JSON errors as trajectory errors.
    fn from(err: serde_json::Error) -> Self {
        TrajectoryError::Json(err)
    }
}

impl From<ChromaHttpClientError> for TrajectoryError {
    /// Preserve Chroma client errors as trajectory errors.
    fn from(err: ChromaHttpClientError) -> Self {
        TrajectoryError::Chroma(err)
    }
}

impl From<Utf8Error> for TrajectoryError {
    /// Preserve UTF-8 errors as trajectory errors.
    fn from(err: Utf8Error) -> Self {
        TrajectoryError::Utf8(err)
    }
}

impl From<TryFromIntError> for TrajectoryError {
    /// Preserve integer conversion errors as trajectory errors.
    fn from(err: TryFromIntError) -> Self {
        TrajectoryError::IntConversion(err)
    }
}
