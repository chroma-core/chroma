use chroma_error::{ChromaError, ErrorCodes};
use pyo3::PyErr;
use thiserror::Error;

pyo3::import_exception!(chromadb.errors, ChromaAuthError);
pyo3::import_exception!(chromadb.errors, InvalidArgumentError);
pyo3::import_exception!(chromadb.errors, AuthorizationError);
pyo3::import_exception!(chromadb.errors, NotFoundError);
pyo3::import_exception!(chromadb.errors, UniqueConstraintError);
pyo3::import_exception!(chromadb.errors, InternalError);
pyo3::import_exception!(chromadb.errors, RateLimitError);

#[derive(Error, Debug)]
#[error(transparent)]
pub(crate) struct ChromaPyError(Box<dyn ChromaError>);

impl From<ChromaPyError> for PyErr {
    fn from(value: ChromaPyError) -> Self {
        match value.0.code() {
            ErrorCodes::InvalidArgument => InvalidArgumentError::new_err(value.to_string()),
            ErrorCodes::Unauthenticated => ChromaAuthError::new_err(value.to_string()),
            ErrorCodes::PermissionDenied => AuthorizationError::new_err(value.to_string()),
            ErrorCodes::NotFound => NotFoundError::new_err(value.to_string()),
            ErrorCodes::Internal => InternalError::new_err(value.to_string()),
            _ => InternalError::new_err(value.to_string()),
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
