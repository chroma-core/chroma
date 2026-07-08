use chroma_api_types::{StaleReadError as ApiStaleReadError, CONDITIONAL_WRITE_CONFLICT_MESSAGE};
use chroma_error::{source_chain_contains, ChromaError, ErrorCodes};
use chroma_log::PushLogsError;
use chroma_types::ConditionalCommitError;
use pyo3::PyErr;
use thiserror::Error;

pyo3::import_exception!(chromadb.errors, ChromaAuthError);
pyo3::import_exception!(chromadb.errors, InvalidArgumentError);
pyo3::import_exception!(chromadb.errors, AuthorizationError);
pyo3::import_exception!(chromadb.errors, NotFoundError);
pyo3::import_exception!(chromadb.errors, UniqueConstraintError);
pyo3::import_exception!(chromadb.errors, InternalError);
pyo3::import_exception!(chromadb.errors, RateLimitError);
pyo3::import_exception!(chromadb.errors, BackoffError);
pyo3::import_exception!(chromadb.errors, ConditionalWriteConflictError);
pyo3::import_exception!(chromadb.errors, TransactionsNotSupportedError);
pyo3::import_exception!(chromadb.errors, StaleReadError);

#[derive(Error, Debug)]
#[error(transparent)]
pub(crate) struct ChromaPyError(Box<dyn ChromaError>);

impl From<ChromaPyError> for PyErr {
    fn from(value: ChromaPyError) -> Self {
        let message = value.to_string();
        let chroma_error = value.0.as_ref();
        if (chroma_error.code() == ErrorCodes::Aborted
            && message.contains(CONDITIONAL_WRITE_CONFLICT_MESSAGE))
            || source_chain_contains(chroma_error, |err| {
                matches!(
                    err.downcast_ref::<PushLogsError>(),
                    Some(PushLogsError::ConditionalWriteConflict)
                )
            })
        {
            return ConditionalWriteConflictError::new_err(message);
        }
        if source_chain_contains(chroma_error, |err| err.is::<ApiStaleReadError>()) {
            return StaleReadError::new_err(message);
        }
        if source_chain_contains(chroma_error, |err| {
            matches!(
                err.downcast_ref::<ConditionalCommitError>(),
                Some(ConditionalCommitError::TransactionsNotSupported { .. })
            )
        }) {
            return TransactionsNotSupportedError::new_err(message);
        }
        if source_chain_contains(chroma_error, |err| {
            matches!(
                err.downcast_ref::<ConditionalCommitError>(),
                Some(ConditionalCommitError::Backoff)
            )
        }) {
            return BackoffError::new_err(message);
        }
        match chroma_error.code() {
            ErrorCodes::InvalidArgument => InvalidArgumentError::new_err(message),
            ErrorCodes::Unauthenticated => ChromaAuthError::new_err(message),
            ErrorCodes::PermissionDenied => AuthorizationError::new_err(message),
            ErrorCodes::NotFound => NotFoundError::new_err(message),
            ErrorCodes::Internal => InternalError::new_err(message),
            _ => InternalError::new_err(message),
        }
    }
}

impl<E: ChromaError + 'static> From<E> for ChromaPyError {
    fn from(value: E) -> Self {
        Self(Box::new(value))
    }
}

pub(crate) type ChromaPyResult<T> = Result<T, ChromaPyError>;

#[derive(Error, Debug)]
#[error(transparent)]
pub(crate) struct WrappedPyErr(pub PyErr);

impl From<PyErr> for WrappedPyErr {
    fn from(value: PyErr) -> Self {
        Self(value)
    }
}

impl ChromaError for WrappedPyErr {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
pub(crate) struct WrappedUuidError(#[from] pub uuid::Error);

impl ChromaError for WrappedUuidError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

#[derive(Error, Debug)]
#[error("{0}")]
pub(crate) struct InvalidDatabaseNameError(pub String);

impl ChromaError for InvalidDatabaseNameError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}
