use std::collections::HashMap;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannIndexFlusher;
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

const HNSW_PATH: &str = "hnsw_path";
const VERSION_MAP_PATH: &str = "version_map_path";
const POSTING_LIST_PATH: &str = "posting_list_path";
const MAX_HEAD_ID_BF_PATH: &str = "max_head_id_path";

pub(crate) struct SpannSegmentWriter {
    index: SpannIndexWriter,
    #[allow(dead_code)]
    id: SegmentUuid,
}

#[derive(Error, Debug)]
pub enum SpannSegmentWriterError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Segment metadata does not contain distance function")]
    DistanceFunctionNotFound,
    #[error("Error parsing index uuid from string")]
    IndexIdParsingError,
    #[error("Invalid file path for HNSW index")]
    HnswInvalidFilePath,
    #[error("Invalid file path for version map")]
    VersionMapInvalidFilePath,
    #[error("Invalid file path for posting list")]
    PostingListInvalidFilePath,
    #[error("Invalid file path for max head id")]
    MaxHeadIdInvalidFilePath,
    #[error("Error constructing spann index writer")]
    SpannSegmentWriterCreateError,
    #[error("Error adding record to spann index writer")]
    SpannSegmentWriterAddRecordError,
    #[error("Error committing spann index writer")]
    SpannSegmentWriterCommitError,
    #[error("Error flushing spann index writer")]
    SpannSegmentWriterFlushError,
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
            Self::SpannSegmentWriterCreateError => ErrorCodes::Internal,
            Self::MaxHeadIdInvalidFilePath => ErrorCodes::Internal,
            Self::NotImplemented => ErrorCodes::Internal,
            Self::SpannSegmentWriterCommitError => ErrorCodes::Internal,
            Self::SpannSegmentWriterFlushError => ErrorCodes::Internal,
            Self::SpannSegmentWriterAddRecordError => ErrorCodes::Internal,
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
                    (Some(IndexUuid(index_uuid)), None, None, None)
                }
                None => {
                    return Err(SpannSegmentWriterError::HnswInvalidFilePath);
                }
            },
            None => {
                let hnsw_params = hnsw_params_from_segment(segment);
                (
                    None,
                    Some(hnsw_params.m),
                    Some(hnsw_params.ef_construction),
                    Some(hnsw_params.ef_search),
                )
            }
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
                return Err(SpannSegmentWriterError::SpannSegmentWriterCreateError);
            }
        };

        Ok(SpannSegmentWriter {
            index: index_writer,
            id: segment.id,
        })
    }

    async fn add(&self, record: &MaterializedLogRecord<'_>) -> Result<(), SpannSegmentWriterError> {
        self.index
            .add(record.offset_id, record.merged_embeddings())
            .await
            .map_err(|_| SpannSegmentWriterError::SpannSegmentWriterAddRecordError)
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
            .map_err(|_| SpannSegmentWriterError::SpannSegmentWriterCommitError);
        match index_flusher {
            Err(e) => Err(Box::new(e)),
            Ok(index_flusher) => Ok(SpannSegmentFlusher { index_flusher }),
        }
    }
}

#[async_trait]
impl SegmentFlusher for SpannSegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let index_flusher_res = self
            .index_flusher
            .flush()
            .await
            .map_err(|_| SpannSegmentWriterError::SpannSegmentWriterFlushError);
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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, path::PathBuf};

    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_distance::DistanceFunction;
    use chroma_index::{hnsw_provider::HnswIndexProvider, Index};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, CollectionUuid, LogRecord, Metadata, MetadataValue, Operation, OperationRecord,
        SegmentUuid, SpannPostingList,
    };

    use crate::segment::{
        spann_segment::SpannSegmentWriter, LogMaterializer, SegmentFlusher, SegmentWriter,
    };

    #[tokio::test]
    async fn test_spann_segment_writer() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            rx,
        );
        let collection_id = CollectionUuid::new();
        let segment_id = SegmentUuid::new();
        let mut metadata_hash_map = Metadata::new();
        metadata_hash_map.insert(
            "hnsw:space".to_string(),
            MetadataValue::Str("l2".to_string()),
        );
        metadata_hash_map.insert("hnsw:M".to_string(), MetadataValue::Int(16));
        metadata_hash_map.insert("hnsw:construction_ef".to_string(), MetadataValue::Int(100));
        metadata_hash_map.insert("hnsw:search_ef".to_string(), MetadataValue::Int(100));
        let mut spann_segment = chroma_types::Segment {
            id: segment_id,
            collection: collection_id,
            r#type: chroma_types::SegmentType::Spann,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: Some(metadata_hash_map),
            file_path: HashMap::new(),
        };
        let spann_writer = SpannSegmentWriter::from_segment(
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
        )
        .await
        .expect("Error creating spann segment writer");
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about cats.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(vec![4.0, 5.0, 6.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
        ];
        let chunked_log = Chunk::new(data.into());
        // Materialize the logs.
        let materializer = LogMaterializer::new(None, chunked_log, None);
        let materialized_log = materializer
            .materialize()
            .await
            .expect("Error materializing logs");
        spann_writer
            .apply_materialized_log_chunk(materialized_log)
            .await
            .expect("Error applying materialized log");
        let flusher = spann_writer
            .commit()
            .await
            .expect("Error committing spann writer");
        spann_segment.file_path = flusher.flush().await.expect("Error flushing spann writer");
        assert_eq!(spann_segment.file_path.len(), 4);
        assert!(spann_segment.file_path.contains_key("hnsw_path"));
        assert!(spann_segment.file_path.contains_key("version_map_path"),);
        assert!(spann_segment.file_path.contains_key("posting_list_path"),);
        assert!(spann_segment.file_path.contains_key("max_head_id_path"),);
        // Load this segment and check if the embeddings are present. New cache
        // so that the previous cache is not used.
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage,
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            rx,
        );
        let spann_writer = SpannSegmentWriter::from_segment(
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
        )
        .await
        .expect("Error creating spann segment writer");
        assert_eq!(spann_writer.index.dimensionality, 3);
        assert_eq!(
            spann_writer.index.distance_function,
            DistanceFunction::Euclidean
        );
        // Next head id should be 2 since one centroid is already taken up.
        assert_eq!(
            spann_writer
                .index
                .next_head_id
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        {
            let read_guard = spann_writer.index.versions_map.read();
            assert_eq!(read_guard.versions_map.len(), 2);
            assert_eq!(
                *read_guard
                    .versions_map
                    .get(&1)
                    .expect("Doc offset id 1 not found"),
                1
            );
            assert_eq!(
                *read_guard
                    .versions_map
                    .get(&2)
                    .expect("Doc offset id 2 not found"),
                1
            );
        }
        {
            // Test HNSW.
            let hnsw_index = spann_writer.index.hnsw_index.inner.read();
            assert_eq!(hnsw_index.len(), 1);
            let r = hnsw_index
                .get(1)
                .expect("Expect one centroid")
                .expect("Expect centroid embedding");
            assert_eq!(r.len(), 3);
            assert_eq!(r[0], 1.0);
            assert_eq!(r[1], 2.0);
            assert_eq!(r[2], 3.0);
        }
        // Test PL.
        let read_guard = spann_writer.index.posting_list_writer.lock().await;
        let res = read_guard
            .get_owned::<u32, &SpannPostingList<'_>>("", 1)
            .await
            .expect("Expected posting list to be present")
            .expect("Expected posting list to be present");
        assert_eq!(res.0.len(), 2);
        assert_eq!(res.1.len(), 2);
        assert_eq!(res.2.len(), 6);
        assert_eq!(res.0[0], 1);
        assert_eq!(res.0[1], 2);
        assert_eq!(res.1[0], 1);
        assert_eq!(res.1[1], 1);
        assert_eq!(res.2[0], 1.0);
        assert_eq!(res.2[1], 2.0);
        assert_eq!(res.2[2], 3.0);
        assert_eq!(res.2[3], 4.0);
        assert_eq!(res.2[4], 5.0);
        assert_eq!(res.2[5], 6.0);
    }
}
