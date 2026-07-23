use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use thiserror::Error;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[allow(dead_code)]
pub struct WorkQueueRecord {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub compaction_offset: i64,
    pub insertion_order: u64,
}

#[derive(Error, Debug, Clone)]
#[allow(dead_code)]
pub enum WorkQueueError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("ETag mismatch - another instance is active")]
    ETagMismatch,

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("SysDB error: {0}")]
    SysDb(String),

    #[error("Failed to finish async invocation: {0}")]
    TryFinishFailed(String),

    #[error("Failed to check invocations: {0}")]
    CheckInvocationsFailed(String),
}

impl ChromaError for WorkQueueError {
    fn code(&self) -> ErrorCodes {
        match self {
            WorkQueueError::Storage(_) => ErrorCodes::Internal,
            WorkQueueError::Serialization(_) => ErrorCodes::Internal,
            WorkQueueError::ETagMismatch => ErrorCodes::AlreadyExists,
            WorkQueueError::InvalidState(_) => ErrorCodes::InvalidArgument,
            WorkQueueError::SysDb(_) => ErrorCodes::Internal,
            WorkQueueError::TryFinishFailed(_) => ErrorCodes::Internal,
            WorkQueueError::CheckInvocationsFailed(_) => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            // ETagMismatch is expected during normal operation
            WorkQueueError::ETagMismatch => false,
            // All other errors should be traced
            _ => true,
        }
    }
}
