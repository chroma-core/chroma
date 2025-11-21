use super::{AttachedFunctionUuid, CollectionUuid, ConversionError};
use crate::{
    chroma_proto::{FilePaths, FlushSegmentCompactionInfo},
    SegmentUuid,
};
use chroma_error::{ChromaError, ErrorCodes};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct SegmentFlushInfo {
    pub segment_id: SegmentUuid,
    pub file_paths: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct AttachedFunctionUpdateInfo {
    pub attached_function_id: AttachedFunctionUuid,
    pub completion_offset: u64,
}

#[derive(Error, Debug)]
pub enum FinishAttachedFunctionError {
    #[error("Failed to finish attached function: {0}")]
    FailedToFinishAttachedFunction(#[from] tonic::Status),
    #[error("Attached function not found")]
    AttachedFunctionNotFound,
}

impl ChromaError for FinishAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishAttachedFunctionError::FailedToFinishAttachedFunction(_) => ErrorCodes::Internal,
            FinishAttachedFunctionError::AttachedFunctionNotFound => ErrorCodes::NotFound,
        }
    }
}

#[derive(Error, Debug)]
pub enum FinishCreateAttachedFunctionError {
    #[error("Failed to finish creating attached function: {0}")]
    FailedToFinishCreateAttachedFunction(#[from] tonic::Status),
    #[error("Attached function not found")]
    AttachedFunctionNotFound,
}

impl ChromaError for FinishCreateAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishCreateAttachedFunctionError::FailedToFinishCreateAttachedFunction(_) => {
                ErrorCodes::Internal
            }
            FinishCreateAttachedFunctionError::AttachedFunctionNotFound => ErrorCodes::NotFound,
        }
    }
}

#[derive(Error, Debug)]
pub enum GetMinCompletionOffsetError {
    #[error("Failed to get min completion offset: {0}")]
    FailedToGetMinCompletionOffset(#[from] tonic::Status),
}

impl ChromaError for GetMinCompletionOffsetError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum AdvanceAttachedFunctionError {
    #[error("Failed to advance attached function: {0}")]
    FailedToAdvanceAttachedFunction(#[from] tonic::Status),
    #[error("Attached function not found - nonce mismatch or attached function doesn't exist")]
    AttachedFunctionNotFound,
}

impl ChromaError for AdvanceAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            AdvanceAttachedFunctionError::FailedToAdvanceAttachedFunction(_) => {
                ErrorCodes::Internal
            }
            AdvanceAttachedFunctionError::AttachedFunctionNotFound => ErrorCodes::NotFound,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdvanceAttachedFunctionResponse {
    pub completion_offset: u64,
}

impl TryInto<FlushSegmentCompactionInfo> for &SegmentFlushInfo {
    type Error = SegmentFlushInfoConversionError;

    fn try_into(self) -> Result<FlushSegmentCompactionInfo, Self::Error> {
        let mut file_paths = HashMap::new();
        for (key, value) in self.file_paths.clone() {
            file_paths.insert(key, FilePaths { paths: value });
        }

        Ok(FlushSegmentCompactionInfo {
            segment_id: self.segment_id.to_string(),
            file_paths,
        })
    }
}

#[derive(Error, Debug)]
pub enum SegmentFlushInfoConversionError {
    #[error("Invalid segment id, valid UUID required")]
    InvalidSegmentId,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

#[derive(Debug)]
pub struct FlushCompactionResponse {
    pub collection_id: CollectionUuid,
    pub collection_version: i32,
    pub last_compaction_time: i64,
}

impl FlushCompactionResponse {
    pub fn new(
        collection_id: CollectionUuid,
        collection_version: i32,
        last_compaction_time: i64,
    ) -> Self {
        FlushCompactionResponse {
            collection_id,
            collection_version,
            last_compaction_time,
        }
    }
}

#[derive(Error, Debug)]
pub enum FlushCompactionResponseConversionError {
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
    #[error("Invalid collection id, valid UUID required")]
    InvalidUuid,
    #[error("Invalid attached function nonce, valid UUID required")]
    InvalidAttachedFunctionNonce,
    #[error("Invalid timestamp format")]
    InvalidTimestamp,
}

impl ChromaError for FlushCompactionResponseConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushCompactionResponseConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::InvalidAttachedFunctionNonce => {
                ErrorCodes::InvalidArgument
            }
            FlushCompactionResponseConversionError::InvalidTimestamp => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::DecodeError(e) => e.code(),
        }
    }
}
