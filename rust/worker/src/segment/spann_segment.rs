use std::collections::HashMap;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::{SpannIndexFlusher, SpannIndexWriterConstructionError};
use chroma_index::IndexUuid;
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter};
use chroma_types::SegmentUuid;
use chroma_types::{MaterializedLogOperation, Segment, SegmentScope, SegmentType};
use thiserror::Error;
use tonic::async_trait;
use uuid::Uuid;

use super::{
    record_segment::ApplyMaterializedLogError,
    utils::{distance_function_from_segment, hnsw_params_from_segment},
    MaterializedLogRecord, SegmentFlusher, SegmentWriter,
};

#[allow(dead_code)]
const HNSW_PATH: &str = "hnsw_path";
#[allow(dead_code)]
const VERSION_MAP_PATH: &str = "version_map_path";
#[allow(dead_code)]
const POSTING_LIST_PATH: &str = "posting_list_path";
const MAX_HEAD_ID_BF_PATH: &str = "max_head_id_path";

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
    #[error("Max head id invalid file path")]
    MaxHeadIdInvalidFilePath,
    #[error("Spann index creation error")]
    SpannIndexWriterConstructionError,
    #[error("Not implemented")]
    NotImplemented,
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
            Self::MaxHeadIdInvalidFilePath => ErrorCodes::Internal,
            Self::NotImplemented => ErrorCodes::Internal,
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
        let (hnsw_id, m, ef_construction, ef_search) = match segment.file_path.get(HNSW_PATH) {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    let hnsw_params = hnsw_params_from_segment(segment);
                    (
                        Some(IndexUuid(index_uuid)),
                        Some(hnsw_params.m),
                        Some(hnsw_params.ef_construction),
                        Some(hnsw_params.ef_search),
                    )
                }
                None => {
                    return Err(SpannSegmentWriterError::HnswInvalidFilePath);
                }
            },
            None => (None, None, None, None),
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

        let max_head_id_bf_id = match segment.file_path.get(MAX_HEAD_ID_BF_PATH) {
            Some(max_head_id_bf_path) => match max_head_id_bf_path.first() {
                Some(max_head_id_bf_id) => {
                    let max_head_id_bf_uuid = match Uuid::parse_str(max_head_id_bf_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    Some(max_head_id_bf_uuid)
                }
                None => {
                    return Err(SpannSegmentWriterError::MaxHeadIdInvalidFilePath);
                }
            },
            None => None,
        };

        let index_writer = match SpannIndexWriter::from_id(
            hnsw_provider,
            hnsw_id.as_ref(),
            versions_map_id.as_ref(),
            posting_list_id.as_ref(),
            max_head_id_bf_id.as_ref(),
            m,
            ef_construction,
            ef_search,
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

    async fn add(
        &self,
        record: &MaterializedLogRecord<'_>,
    ) -> Result<(), SpannIndexWriterConstructionError> {
        self.index
            .add(record.offset_id, record.merged_embeddings())
            .await
    }
}

struct SpannSegmentFlusher {
    index_flusher: SpannIndexFlusher,
}

impl<'a> SegmentWriter<'a> for SpannSegmentWriter {
    async fn apply_materialized_log_chunk(
        &self,
        records: chroma_types::Chunk<super::MaterializedLogRecord<'a>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for (record, _) in records.iter() {
            match record.final_operation {
                MaterializedLogOperation::AddNew => {
                    self.add(record)
                        .await
                        .map_err(|_| ApplyMaterializedLogError::BlockfileSet)?;
                }
                // TODO(Sanket): Implement other operations.
                _ => {
                    todo!()
                }
            }
        }
        Ok(())
    }

    async fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let index_flusher = self
            .index
            .commit()
            .await
            .map_err(|_| Box::new(SpannSegmentWriterError::SpannIndexWriterConstructionError));
        match index_flusher {
            Err(e) => Err(e),
            Ok(index_flusher) => Ok(SpannSegmentFlusher { index_flusher }),
        }
    }
}

#[async_trait]
impl SegmentFlusher for SpannSegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let index_flusher_res = self.index_flusher.flush().await;
        match index_flusher_res {
            Err(e) => Err(Box::new(e)),
            Ok(index_ids) => {
                let mut index_id_map = HashMap::new();
                index_id_map.insert(HNSW_PATH.to_string(), vec![index_ids.hnsw_id.to_string()]);
                index_id_map.insert(
                    VERSION_MAP_PATH.to_string(),
                    vec![index_ids.versions_map_id.to_string()],
                );
                index_id_map.insert(
                    POSTING_LIST_PATH.to_string(),
                    vec![index_ids.pl_id.to_string()],
                );
                index_id_map.insert(
                    MAX_HEAD_ID_BF_PATH.to_string(),
                    vec![index_ids.max_head_id_id.to_string()],
                );
                Ok(index_id_map)
            }
        }
    }
}
