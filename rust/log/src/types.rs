use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{CollectionUuid, RecordConversionError};
use thiserror::Error;

/// CollectionInfo is a struct that contains information about a collection for the
/// compacting process.
/// Fields:
/// - collection_id: the id of the collection that needs to be compacted
/// - first_log_offset: the offset of the first log entry in the collection that needs to be compacted
/// - first_log_ts: the timestamp of the first log entry in the collection that needs to be compacted
#[derive(Debug)]
pub struct CollectionInfo {
    pub collection_id: CollectionUuid,
    pub first_log_offset: i64,
    pub first_log_ts: i64,
}

#[derive(Error, Debug)]
pub enum PullLogsError {
    #[error("Failed to fetch")]
    FailedToPullLogs(#[from] tonic::Status),
    #[error("Failed to convert proto embedding record into EmbeddingRecord")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for PullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            PullLogsError::FailedToPullLogs(_) => ErrorCodes::Internal,
            PullLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum PushLogsError {
    #[error("Failed to push logs")]
    FailedToPushLogs(#[from] tonic::Status),
    #[error("Failed to convert records to proto")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for PushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            PushLogsError::FailedToPushLogs(_) => ErrorCodes::Internal,
            PushLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GetCollectionsWithNewDataError {
    #[error("Failed to fetch")]
    FailedGetCollectionsWithNewData(#[from] tonic::Status),
}

impl ChromaError for GetCollectionsWithNewDataError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(_) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum UpdateCollectionLogOffsetError {
    #[error("Failed to update collection log offset")]
    FailedToUpdateCollectionLogOffset(#[from] tonic::Status),
}

impl ChromaError for UpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(_) => {
                ErrorCodes::Internal
            }
        }
    }
}
