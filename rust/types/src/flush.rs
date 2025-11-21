use super::{AttachedFunctionUuid, CollectionUuid, ConversionError, Schema};
use crate::{
    chroma_proto::{
        FilePaths, FlushCollectionCompactionAndAttachedFunctionResponse, FlushSegmentCompactionInfo,
    },
    SegmentUuid,
};
use chroma_error::{ChromaError, ErrorCodes};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SegmentFlushInfo {
    pub segment_id: SegmentUuid,
    pub file_paths: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct CollectionFlushInfo {
    pub tenant_id: String,
    pub collection_id: CollectionUuid,
    pub log_position: i64,
    pub collection_version: i32,
    pub segment_flush_info: Arc<[SegmentFlushInfo]>,
    pub total_records_post_compaction: u64,
    pub size_bytes_post_compaction: u64,
    pub schema: Option<Schema>,
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

#[derive(Debug)]
pub struct FlushCompactionAndAttachedFunctionResponse {
    pub collections: Vec<FlushCompactionResponse>,
    // Completion offset updated during register
    pub completion_offset: u64,
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

impl TryFrom<FlushCollectionCompactionAndAttachedFunctionResponse>
    for FlushCompactionAndAttachedFunctionResponse
{
    type Error = FlushCompactionResponseConversionError;

    fn try_from(
        value: FlushCollectionCompactionAndAttachedFunctionResponse,
    ) -> Result<Self, Self::Error> {
        // Parse all collections from the repeated field
        let mut collections = Vec::with_capacity(value.collections.len());
        for collection in value.collections {
            let id = Uuid::parse_str(&collection.collection_id)
                .map_err(|_| FlushCompactionResponseConversionError::InvalidUuid)?;
            collections.push(FlushCompactionResponse {
                collection_id: CollectionUuid(id),
                collection_version: collection.collection_version,
                last_compaction_time: collection.last_compaction_time,
            });
        }

        // Extract completion_offset from attached_function_state
        // Note: next_nonce and next_run are no longer used by the client
        // They were already set by PrepareAttachedFunction via advance_attached_function()
        let completion_offset = value
            .attached_function_state
            .as_ref()
            .map(|state| state.completion_offset)
            .unwrap_or(0);

        Ok(FlushCompactionAndAttachedFunctionResponse {
            collections,
            completion_offset,
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
    #[error("Invalid timestamp format")]
    InvalidTimestamp,
    #[error("Missing collections in response")]
    MissingCollections,
}

impl ChromaError for FlushCompactionResponseConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushCompactionResponseConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::InvalidAttachedFunctionNonce => {
                ErrorCodes::InvalidArgument
            }
            FlushCompactionResponseConversionError::InvalidTimestamp => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::MissingCollections => {
                ErrorCodes::InvalidArgument
            }
            FlushCompactionResponseConversionError::DecodeError(e) => e.code(),
        }
    }
}
