use super::{AttachedFunctionUuid, CollectionUuid, ConversionError};
use crate::{
    chroma_proto::{
        FilePaths, FlushCollectionCompactionAndAttachedFunctionResponse, FlushSegmentCompactionInfo,
    },
    SegmentUuid,
};
use chroma_error::{ChromaError, ErrorCodes};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SegmentFlushInfo {
    pub segment_id: SegmentUuid,
    pub file_paths: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct AttachedFunctionUpdateInfo {
    pub attached_function_id: AttachedFunctionUuid,
    pub attached_function_run_nonce: uuid::Uuid,
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
    pub next_nonce: uuid::Uuid,
    pub next_run: std::time::SystemTime,
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

#[derive(Debug)]
pub struct FlushCompactionAndAttachedFunctionResponse {
    pub collection_id: CollectionUuid,
    pub collection_version: i32,
    pub last_compaction_time: i64,
    // Completion offset updated during register
    pub completion_offset: u64,
    // NOTE: next_nonce and next_run are no longer returned
    // They were already set by PrepareAttachedFunction via advance_attached_function()
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

impl TryFrom<FlushCollectionCompactionAndAttachedFunctionResponse> for FlushCompactionResponse {
    type Error = FlushCompactionResponseConversionError;

    fn try_from(
        value: FlushCollectionCompactionAndAttachedFunctionResponse,
    ) -> Result<Self, Self::Error> {
        let id = Uuid::parse_str(&value.collection_id)
            .map_err(|_| FlushCompactionResponseConversionError::InvalidUuid)?;
        Ok(FlushCompactionResponse {
            collection_id: CollectionUuid(id),
            collection_version: value.collection_version,
            last_compaction_time: value.last_compaction_time,
        })
    }
}

impl TryFrom<FlushCollectionCompactionAndAttachedFunctionResponse>
    for FlushCompactionAndAttachedFunctionResponse
{
    type Error = FlushCompactionResponseConversionError;

    fn try_from(
        value: FlushCollectionCompactionAndAttachedFunctionResponse,
    ) -> Result<Self, Self::Error> {
        let id = Uuid::parse_str(&value.collection_id)
            .map_err(|_| FlushCompactionResponseConversionError::InvalidUuid)?;

        // Note: next_nonce and next_run are no longer populated by the server
        // They were already set by PrepareAttachedFunction via advance_attached_function()
        // We only use completion_offset from the response

        Ok(FlushCompactionAndAttachedFunctionResponse {
            collection_id: CollectionUuid(id),
            collection_version: value.collection_version,
            last_compaction_time: value.last_compaction_time,
            completion_offset: value.completion_offset,
        })
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
    #[error("Missing next_run timestamp")]
    MissingNextRun,
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
            FlushCompactionResponseConversionError::MissingNextRun => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::InvalidTimestamp => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::DecodeError(e) => e.code(),
        }
    }
}
