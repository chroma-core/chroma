use crate::{ChromaError, ErrorCodes};
use thiserror::Error;

impl From<ErrorCodes> for tonic::Code {
    fn from(err: ErrorCodes) -> tonic::Code {
        match err {
            ErrorCodes::Success => tonic::Code::Ok,
            ErrorCodes::Cancelled => tonic::Code::Cancelled,
            ErrorCodes::Unknown => tonic::Code::Unknown,
            ErrorCodes::InvalidArgument => tonic::Code::InvalidArgument,
            ErrorCodes::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            ErrorCodes::NotFound => tonic::Code::NotFound,
            ErrorCodes::AlreadyExists => tonic::Code::AlreadyExists,
            ErrorCodes::PermissionDenied => tonic::Code::PermissionDenied,
            ErrorCodes::ResourceExhausted => tonic::Code::ResourceExhausted,
            ErrorCodes::FailedPrecondition => tonic::Code::FailedPrecondition,
            ErrorCodes::Aborted => tonic::Code::Aborted,
            ErrorCodes::OutOfRange => tonic::Code::OutOfRange,
            ErrorCodes::Unimplemented => tonic::Code::Unimplemented,
            ErrorCodes::Internal => tonic::Code::Internal,
            ErrorCodes::Unavailable => tonic::Code::Unavailable,
            ErrorCodes::DataLoss => tonic::Code::DataLoss,
            ErrorCodes::Unauthenticated => tonic::Code::Unauthenticated,
            ErrorCodes::VersionMismatch => tonic::Code::Internal,
            ErrorCodes::UnprocessableEntity => tonic::Code::ResourceExhausted,
        }
    }
}

impl From<tonic::Code> for ErrorCodes {
    fn from(code: tonic::Code) -> ErrorCodes {
        match code {
            tonic::Code::Ok => ErrorCodes::Success,
            tonic::Code::Cancelled => ErrorCodes::Cancelled,
            tonic::Code::Unknown => ErrorCodes::Unknown,
            tonic::Code::InvalidArgument => ErrorCodes::InvalidArgument,
            tonic::Code::DeadlineExceeded => ErrorCodes::DeadlineExceeded,
            tonic::Code::NotFound => ErrorCodes::NotFound,
            tonic::Code::AlreadyExists => ErrorCodes::AlreadyExists,
            tonic::Code::PermissionDenied => ErrorCodes::PermissionDenied,
            tonic::Code::ResourceExhausted => ErrorCodes::ResourceExhausted,
            tonic::Code::FailedPrecondition => ErrorCodes::FailedPrecondition,
            tonic::Code::Aborted => ErrorCodes::Aborted,
            tonic::Code::OutOfRange => ErrorCodes::OutOfRange,
            tonic::Code::Unimplemented => ErrorCodes::Unimplemented,
            tonic::Code::Internal => ErrorCodes::Internal,
            tonic::Code::Unavailable => ErrorCodes::Unavailable,
            tonic::Code::DataLoss => ErrorCodes::DataLoss,
            tonic::Code::Unauthenticated => ErrorCodes::Unauthenticated,
        }
    }
}

#[derive(Debug, Error)]
#[error("Tonic error: {0}")]
pub struct TonicError(#[from] pub tonic::Status);

impl ChromaError for TonicError {
    fn code(&self) -> ErrorCodes {
        self.0.code().into()
    }
}

impl From<tonic::Status> for Box<dyn ChromaError> {
    fn from(value: tonic::Status) -> Self {
        Box::new(TonicError(value))
    }
}

#[derive(Debug, Error)]
#[error("Field missing from gRPC response: {0}")]
pub struct TonicMissingFieldError(pub &'static str);

impl ChromaError for TonicMissingFieldError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}
