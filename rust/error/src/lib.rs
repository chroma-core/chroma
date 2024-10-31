// Defines 17 standard error codes based on the error codes defined in the
// gRPC spec. https://grpc.github.io/grpc/core/md_doc_statuscodes.html
// Custom errors can use these codes in order to allow for generic handling
use std::error::Error;

#[derive(PartialEq, Debug)]
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
}

pub trait ChromaError: Error + Send {
    fn code(&self) -> ErrorCodes;
}

impl Error for Box<dyn ChromaError> {}

impl ChromaError for Box<dyn ChromaError> {
    fn code(&self) -> ErrorCodes {
        self.as_ref().code()
    }
}

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
        }
    }
}
