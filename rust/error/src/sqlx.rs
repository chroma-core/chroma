use crate::{ChromaError, ErrorCodes};
use thiserror::Error;

/// Implements `ChromaError` for `sqlx::Error`.
#[derive(Debug, Error)]
#[error("Database error: {0}")]
pub struct WrappedSqlxError(pub sqlx::Error);

impl ChromaError for WrappedSqlxError {
    fn code(&self) -> crate::ErrorCodes {
        match self.0 {
            sqlx::Error::RowNotFound => ErrorCodes::NotFound,
            sqlx::Error::PoolTimedOut => ErrorCodes::ResourceExhausted,
            sqlx::Error::PoolClosed => ErrorCodes::Unavailable,
            _ => ErrorCodes::Internal,
        }
    }
}

impl From<sqlx::Error> for WrappedSqlxError {
    fn from(value: sqlx::Error) -> Self {
        Self(value)
    }
}

impl From<sqlx::Error> for Box<dyn ChromaError> {
    fn from(value: sqlx::Error) -> Self {
        Box::new(WrappedSqlxError(value))
    }
}

impl From<WrappedSqlxError> for Box<dyn ChromaError> {
    fn from(value: WrappedSqlxError) -> Self {
        Box::new(value)
    }
}
