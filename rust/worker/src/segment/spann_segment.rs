use std::collections::HashMap;

use arrow::error;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter};
use chroma_types::{Segment, SegmentScope, SegmentType};
use thiserror::Error;
use uuid::Uuid;

use super::utils::{distance_function_from_segment, hnsw_params_from_segment};

const HNSW_PATH: &str = "hnsw_path";
const VERSION_MAP_PATH: &str = "version_map_path";
const POSTING_LIST_PATH: &str = "posting_list_path";

pub(crate) struct SpannSegmentWriter {
    index: SpannIndexWriter,
    id: Uuid,
}

#[derive(Error, Debug)]
pub enum SpannSegmentWriterError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Distance function not found")]
    DistanceFunctionNotFound,
    #[error("Hnsw index id parsing error")]
    IndexIdParsingError,
    #[error("HNSW index creation error")]
    HnswIndexCreationError,
    #[error("Hnsw Invalid file path")]
    HnswInvalidFilePath,
    #[error("Version map Invalid file path")]
    VersionMapInvalidFilePath,
    #[error("Failure in loading the versions map")]
    VersionMapLoadError,
    #[error("Failure in forking the posting list")]
    PostingListForkError,
    #[error("Postings list invalid file path")]
    PostingListInvalidFilePath,
    #[error("Posting list creation error")]
    PostingListCreationError,
}

impl ChromaError for SpannSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::InvalidArgument => ErrorCodes::InvalidArgument,
            Self::IndexIdParsingError => ErrorCodes::Internal,
            Self::HnswIndexCreationError => ErrorCodes::Internal,
            Self::DistanceFunctionNotFound => ErrorCodes::Internal,
            Self::HnswInvalidFilePath => ErrorCodes::Internal,
            Self::VersionMapInvalidFilePath => ErrorCodes::Internal,
            Self::VersionMapLoadError => ErrorCodes::Internal,
            Self::PostingListForkError => ErrorCodes::Internal,
            Self::PostingListInvalidFilePath => ErrorCodes::Internal,
            Self::PostingListCreationError => ErrorCodes::Internal,
        }
    }
}

impl SpannSegmentWriter {
    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        dimensionality: usize,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        if segment.r#type != SegmentType::Spann || segment.scope != SegmentScope::VECTOR {
            return Err(SpannSegmentWriterError::InvalidArgument);
        }
        // Load HNSW index.
        let hnsw_index = match segment.file_path.get(HNSW_PATH) {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    let distance_function = match distance_function_from_segment(segment) {
                        Ok(distance_function) => distance_function,
                        Err(e) => {
                            return Err(SpannSegmentWriterError::DistanceFunctionNotFound);
                        }
                    };
                    match SpannIndexWriter::hnsw_index_from_id(
                        &hnsw_provider,
                        &index_uuid,
                        &segment.collection,
                        distance_function,
                        dimensionality,
                    )
                    .await
                    {
                        Ok(index) => index,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::HnswIndexCreationError);
                        }
                    }
                }
                None => {
                    return Err(SpannSegmentWriterError::HnswInvalidFilePath);
                }
            },
            // Create a new index.
            None => {
                let hnsw_params = hnsw_params_from_segment(segment);

                let distance_function = match distance_function_from_segment(segment) {
                    Ok(distance_function) => distance_function,
                    Err(e) => {
                        return Err(SpannSegmentWriterError::DistanceFunctionNotFound);
                    }
                };

                match SpannIndexWriter::create_hnsw_index(
                    &hnsw_provider,
                    &segment.collection,
                    distance_function,
                    dimensionality,
                    hnsw_params,
                )
                .await
                {
                    Ok(index) => index,
                    Err(_) => {
                        return Err(SpannSegmentWriterError::HnswIndexCreationError);
                    }
                }
            }
        };
        // Load version map. Empty if file path is not set.
        let mut version_map = HashMap::new();
        if let Some(version_map_path) = segment.file_path.get(VERSION_MAP_PATH) {
            version_map = match version_map_path.first() {
                Some(version_map_id) => {
                    let version_map_uuid = match Uuid::parse_str(version_map_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    match SpannIndexWriter::load_versions_map(&version_map_uuid, blockfile_provider)
                        .await
                    {
                        Ok(index) => index,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::VersionMapLoadError);
                        }
                    }
                }
                None => {
                    return Err(SpannSegmentWriterError::VersionMapInvalidFilePath);
                }
            }
        }
        // Fork the posting list map.
        let posting_list_writer = match segment.file_path.get(POSTING_LIST_PATH) {
            Some(posting_list_path) => match posting_list_path.first() {
                Some(posting_list_id) => {
                    let posting_list_uuid = match Uuid::parse_str(posting_list_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    match SpannIndexWriter::fork_postings_list(
                        &posting_list_uuid,
                        blockfile_provider,
                    )
                    .await
                    {
                        Ok(writer) => writer,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::PostingListForkError);
                        }
                    }
                }
                None => {
                    return Err(SpannSegmentWriterError::PostingListInvalidFilePath);
                }
            },
            // Create a new index.
            None => match SpannIndexWriter::create_posting_list(blockfile_provider).await {
                Ok(writer) => writer,
                Err(_) => {
                    return Err(SpannSegmentWriterError::PostingListCreationError);
                }
            },
        };

        let index_writer =
            SpannIndexWriter::new(hnsw_index, hnsw_provider, posting_list_writer, version_map);

        Ok(SpannSegmentWriter {
            index: index_writer,
            id: segment.id,
        })
    }
}
