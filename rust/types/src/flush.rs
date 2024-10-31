use super::{CollectionUuid, ConversionError};
use crate::chroma_proto::{
    FilePaths, FlushCollectionCompactionResponse, FlushSegmentCompactionInfo,
};
use chroma_error::{ChromaError, ErrorCodes};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct SegmentFlushInfo {
    pub segment_id: Uuid,
    pub file_paths: HashMap<String, Vec<String>>,
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

impl TryFrom<FlushCollectionCompactionResponse> for FlushCompactionResponse {
    type Error = FlushCompactionResponseConversionError;

    fn try_from(value: FlushCollectionCompactionResponse) -> Result<Self, Self::Error> {
        let id = Uuid::parse_str(&value.collection_id)
            .map_err(|_| FlushCompactionResponseConversionError::InvalidUuid)?;
        Ok(FlushCompactionResponse {
            collection_id: CollectionUuid(id),
            collection_version: value.collection_version,
            last_compaction_time: value.last_compaction_time,
        })
    }
}

#[derive(Error, Debug)]
pub enum FlushCompactionResponseConversionError {
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
    #[error("Invalid collection id, valid UUID required")]
    InvalidUuid,
}

impl ChromaError for FlushCompactionResponseConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushCompactionResponseConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            FlushCompactionResponseConversionError::DecodeError(e) => e.code(),
        }
    }
}
