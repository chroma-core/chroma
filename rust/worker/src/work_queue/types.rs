use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use thiserror::Error;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[allow(dead_code)]
pub struct WorkQueueRecord {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub insertion_order: u64,
}

#[derive(Error, Debug)]
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
}

impl ChromaError for WorkQueueError {
    fn code(&self) -> ErrorCodes {
        match self {
            WorkQueueError::Storage(_) => ErrorCodes::Internal,
            WorkQueueError::Serialization(_) => ErrorCodes::Internal,
            WorkQueueError::ETagMismatch => ErrorCodes::AlreadyExists,
            WorkQueueError::InvalidState(_) => ErrorCodes::InvalidArgument,
        }
    }
}

// Stub types for future sysdb integration
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum FinishResult {
    Success,
    NeedsRepair,
}
