use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter, IndexUuid};
use chroma_types::{Segment, SegmentScope, SegmentType, SegmentUuid};
use thiserror::Error;
use uuid::Uuid;

use super::utils::{distance_function_from_segment, hnsw_params_from_segment};

#[allow(dead_code)]
const HNSW_PATH: &str = "hnsw_path";
#[allow(dead_code)]
const VERSION_MAP_PATH: &str = "version_map_path";
#[allow(dead_code)]
const POSTING_LIST_PATH: &str = "posting_list_path";

#[allow(dead_code)]
pub(crate) struct SpannSegmentWriter {
    index: SpannIndexWriter,
    id: SegmentUuid,
}

#[derive(Error, Debug)]
pub enum SpannSegmentWriterError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Distance function not found")]
    DistanceFunctionNotFound,
    #[error("Hnsw index id parsing error")]
    IndexIdParsingError,
    #[error("Hnsw Invalid file path")]
    HnswInvalidFilePath,
    #[error("Version map Invalid file path")]
    VersionMapInvalidFilePath,
    #[error("Postings list invalid file path")]
    PostingListInvalidFilePath,
    #[error("Spann index creation error")]
    SpannIndexWriterConstructionError,
}

impl ChromaError for SpannSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::InvalidArgument => ErrorCodes::InvalidArgument,
            Self::IndexIdParsingError => ErrorCodes::Internal,
            Self::DistanceFunctionNotFound => ErrorCodes::Internal,
            Self::HnswInvalidFilePath => ErrorCodes::Internal,
            Self::VersionMapInvalidFilePath => ErrorCodes::Internal,
            Self::PostingListInvalidFilePath => ErrorCodes::Internal,
            Self::SpannIndexWriterConstructionError => ErrorCodes::Internal,
        }
    }
}

impl SpannSegmentWriter {
    #[allow(dead_code)]
    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        hnsw_provider: &HnswIndexProvider,
        dimensionality: usize,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        if segment.r#type != SegmentType::Spann || segment.scope != SegmentScope::VECTOR {
            return Err(SpannSegmentWriterError::InvalidArgument);
        }
        let distance_function = match distance_function_from_segment(segment) {
            Ok(distance_function) => distance_function,
            Err(_) => {
                return Err(SpannSegmentWriterError::DistanceFunctionNotFound);
            }
        };
        let (hnsw_id, hnsw_params) = match segment.file_path.get(HNSW_PATH) {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    (
                        Some(IndexUuid(index_uuid)),
                        Some(hnsw_params_from_segment(segment)),
                    )
                }
                None => {
                    return Err(SpannSegmentWriterError::HnswInvalidFilePath);
                }
            },
            None => (None, None),
        };
        let versions_map_id = match segment.file_path.get(VERSION_MAP_PATH) {
            Some(version_map_path) => match version_map_path.first() {
                Some(version_map_id) => {
                    let version_map_uuid = match Uuid::parse_str(version_map_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    Some(version_map_uuid)
                }
                None => {
                    return Err(SpannSegmentWriterError::VersionMapInvalidFilePath);
                }
            },
            None => None,
        };
        let posting_list_id = match segment.file_path.get(POSTING_LIST_PATH) {
            Some(posting_list_path) => match posting_list_path.first() {
                Some(posting_list_id) => {
                    let posting_list_uuid = match Uuid::parse_str(posting_list_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    Some(posting_list_uuid)
                }
                None => {
                    return Err(SpannSegmentWriterError::PostingListInvalidFilePath);
                }
            },
            None => None,
        };

        let index_writer = match SpannIndexWriter::from_id(
            hnsw_provider,
            hnsw_id.as_ref(),
            versions_map_id.as_ref(),
            posting_list_id.as_ref(),
            hnsw_params,
            &segment.collection,
            distance_function,
            dimensionality,
            blockfile_provider,
        )
        .await
        {
            Ok(index_writer) => index_writer,
            Err(_) => {
                return Err(SpannSegmentWriterError::SpannIndexWriterConstructionError);
            }
        };

        Ok(SpannSegmentWriter {
            index: index_writer,
            id: segment.id,
        })
    }
}
