// Defines 17 standard error codes based on the error codes defined in the
// gRPC spec. https://grpc.github.io/grpc/core/md_doc_statuscodes.html
// Custom errors can use these codes in order to allow for generic handling
use std::error::Error;

#[cfg(feature = "tonic")]
mod tonic;
#[cfg(feature = "tonic")]
pub use tonic::*;

#[cfg(feature = "sqlx")]
mod sqlx;
#[cfg(feature = "sqlx")]
pub use sqlx::*;

#[cfg(feature = "validator")]
mod validator;
#[cfg(feature = "validator")]
pub use validator::*;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ErrorCodes {
    // OK is returned on success, we use "Success" since Ok is a keyword in Rust.
    Success = 0,
    // CANCELLED indicates the operation was cancelled (typically by the caller).
    Cancelled = 1,
    // UNKNOWN indicates an unknown error.
    Unknown = 2,
    // INVALID_ARGUMENT indicates client specified an invalid argument.
    InvalidArgument = 3,
    // DEADLINE_EXCEEDED means operation expired before completion.
    DeadlineExceeded = 4,
    // NOT_FOUND means some requested entity (e.g., file or directory) was not found.
    NotFound = 5,
    // ALREADY_EXISTS means an entity that we attempted to create (e.g., file or directory) already exists.
    AlreadyExists = 6,
    // PERMISSION_DENIED indicates the caller does not have permission to execute the specified operation.
    PermissionDenied = 7,
    // RESOURCE_EXHAUSTED indicates some resource has been exhausted, perhaps a per-user quota, or perhaps the entire file system is out of space.
    ResourceExhausted = 8,
    // FAILED_PRECONDITION indicates operation was rejected because the system is not in a state required for the operation's execution.
    FailedPrecondition = 9,
    // ABORTED indicates the operation was aborted.
    Aborted = 10,
    // OUT_OF_RANGE means operation was attempted past the valid range.
    OutOfRange = 11,
    // UNIMPLEMENTED indicates operation is not implemented or not supported/enabled.
    Unimplemented = 12,
    // INTERNAL errors are internal errors.
    Internal = 13,
    // UNAVAILABLE indicates service is currently unavailable.
    Unavailable = 14,
    // DATA_LOSS indicates unrecoverable data loss or corruption.
    DataLoss = 15,
    // UNAUTHENTICATED indicates the request does not have valid authentication credentials for the operation.
    Unauthenticated = 16,
    // VERSION_MISMATCH indicates a version mismatch. This is not from the gRPC spec and is specific to Chroma.
    VersionMismatch = 17,
    // UNPROCESSABLE_ENTITY indicates the request is valid but cannot be processed.
    UnprocessableEntity = 18,
}

impl ErrorCodes {
    pub fn name(&self) -> &'static str {
        match self {
            ErrorCodes::InvalidArgument => "InvalidArgumentError",
            ErrorCodes::NotFound => "NotFoundError",
            ErrorCodes::Internal => "InternalError",
            ErrorCodes::VersionMismatch => "VersionMismatchError",
            _ => "ChromaError",
        }
    }
}

#[cfg(feature = "http")]
impl From<ErrorCodes> for http::StatusCode {
    fn from(error_code: ErrorCodes) -> Self {
        match error_code {
            ErrorCodes::Success => http::StatusCode::OK,
            ErrorCodes::Cancelled => http::StatusCode::BAD_REQUEST,
            ErrorCodes::Unknown => http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCodes::InvalidArgument => http::StatusCode::BAD_REQUEST,
            ErrorCodes::DeadlineExceeded => http::StatusCode::GATEWAY_TIMEOUT,
            ErrorCodes::NotFound => http::StatusCode::NOT_FOUND,
            ErrorCodes::AlreadyExists => http::StatusCode::CONFLICT,
            ErrorCodes::PermissionDenied => http::StatusCode::FORBIDDEN,
            ErrorCodes::ResourceExhausted => http::StatusCode::TOO_MANY_REQUESTS,
            ErrorCodes::FailedPrecondition => http::StatusCode::PRECONDITION_FAILED,
            ErrorCodes::Aborted => http::StatusCode::BAD_REQUEST,
            ErrorCodes::OutOfRange => http::StatusCode::BAD_REQUEST,
            ErrorCodes::Unimplemented => http::StatusCode::NOT_IMPLEMENTED,
            ErrorCodes::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCodes::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
            ErrorCodes::DataLoss => http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCodes::Unauthenticated => http::StatusCode::UNAUTHORIZED,
            ErrorCodes::VersionMismatch => http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCodes::UnprocessableEntity => http::StatusCode::UNPROCESSABLE_ENTITY,
        }
    }
}

#[cfg(feature = "http")]
impl From<http::StatusCode> for ErrorCodes {
    fn from(value: http::StatusCode) -> Self {
        match value {
            http::StatusCode::OK => ErrorCodes::Success,
            http::StatusCode::BAD_REQUEST => ErrorCodes::InvalidArgument,
            http::StatusCode::UNAUTHORIZED => ErrorCodes::Unauthenticated,
            http::StatusCode::FORBIDDEN => ErrorCodes::PermissionDenied,
            http::StatusCode::NOT_FOUND => ErrorCodes::NotFound,
            http::StatusCode::CONFLICT => ErrorCodes::AlreadyExists,
            http::StatusCode::TOO_MANY_REQUESTS => ErrorCodes::ResourceExhausted,
            http::StatusCode::INTERNAL_SERVER_ERROR => ErrorCodes::Internal,
            http::StatusCode::SERVICE_UNAVAILABLE => ErrorCodes::Unavailable,
            http::StatusCode::NOT_IMPLEMENTED => ErrorCodes::Unimplemented,
            http::StatusCode::GATEWAY_TIMEOUT => ErrorCodes::DeadlineExceeded,
            http::StatusCode::PRECONDITION_FAILED => ErrorCodes::FailedPrecondition,
            http::StatusCode::UNPROCESSABLE_ENTITY => ErrorCodes::UnprocessableEntity,
            _ => ErrorCodes::Unknown,
        }
    }
}

pub trait ChromaError: Error + Send {
    fn code(&self) -> ErrorCodes;
    fn boxed(self) -> Box<dyn ChromaError>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
    fn should_trace_error(&self) -> bool {
        true
    }
}

impl Error for Box<dyn ChromaError> {}

impl ChromaError for Box<dyn ChromaError> {
    fn code(&self) -> ErrorCodes {
        self.as_ref().code()
    }
}

impl ChromaError for std::io::Error {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Unknown
    }
}
