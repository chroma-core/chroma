use super::blockfile_record::ApplyMaterializedLogError;
use super::blockfile_record::RecordSegmentReader;
use super::types::{
    BorrowedMaterializedLogRecord, HydratedMaterializedLogRecord, MaterializeLogsResult,
};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::GarbageCollectionContext;
use chroma_index::spann::types::SpannMetrics;
use chroma_index::spann::types::{
    SpannIndexFlusher, SpannIndexReader, SpannIndexReaderError, SpannIndexWriterError, SpannPosting,
};
use chroma_index::spann::utils::rng_query;
use chroma_index::spann::utils::RngQueryError;
use chroma_index::IndexUuid;
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter};
use chroma_types::Collection;
use chroma_types::InternalSpannConfiguration;
use chroma_types::SegmentUuid;
use chroma_types::HNSW_PATH;
use chroma_types::MAX_HEAD_ID_BF_PATH;
use chroma_types::POSTING_LIST_PATH;
use chroma_types::VERSION_MAP_PATH;
use chroma_types::{MaterializedLogOperation, Segment, SegmentScope, SegmentType};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub struct SpannSegmentWriter {
    index: SpannIndexWriter,
    pub id: SegmentUuid,
}

impl Debug for SpannSegmentWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedSpannSegmentWriter")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum SpannSegmentWriterError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Collection is missing a spann configuration")]
    MissingSpannConfiguration,
    #[error("Error parsing index uuid from string {0}")]
    IndexIdParsingError(#[source] uuid::Error),
    #[error("Invalid file path for HNSW index")]
    HnswInvalidFilePath,
    #[error("Invalid file path for version map")]
    VersionMapInvalidFilePath,
    #[error("Invalid file path for posting list")]
    PostingListInvalidFilePath,
    #[error("Invalid file path for max head id")]
    MaxHeadIdInvalidFilePath,
    #[error("Error constructing spann index writer {0}")]
    SpannSegmentWriterCreateError(#[source] SpannIndexWriterError),
    #[error("Error mutating record to spann index writer {0}")]
    SpannSegmentWriterMutateRecordError(#[source] SpannIndexWriterError),
    #[error("Error committing spann index writer {0}")]
    SpannSegmentWriterCommitError(#[source] SpannIndexWriterError),
    #[error("Error flushing spann index writer {0}")]
    SpannSegmentWriterFlushError(#[source] SpannIndexWriterError),
    #[error("Error garbage collecting {0}")]
    GarbageCollectError(#[source] SpannIndexWriterError),
}

impl ChromaError for SpannSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::InvalidArgument => ErrorCodes::InvalidArgument,
            Self::IndexIdParsingError(_) => ErrorCodes::Internal,
            Self::HnswInvalidFilePath => ErrorCodes::Internal,
            Self::VersionMapInvalidFilePath => ErrorCodes::Internal,
            Self::PostingListInvalidFilePath => ErrorCodes::Internal,
            Self::SpannSegmentWriterCreateError(e) => e.code(),
            Self::MaxHeadIdInvalidFilePath => ErrorCodes::Internal,
            Self::SpannSegmentWriterCommitError(e) => e.code(),
            Self::SpannSegmentWriterFlushError(e) => e.code(),
            Self::SpannSegmentWriterMutateRecordError(e) => e.code(),
            Self::MissingSpannConfiguration => ErrorCodes::Internal,
            Self::GarbageCollectError(e) => e.code(),
        }
    }
}

impl SpannSegmentWriter {
    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        hnsw_provider: &HnswIndexProvider,
        dimensionality: usize,
        gc_context: GarbageCollectionContext,
        metrics: SpannMetrics,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        if segment.r#type != SegmentType::Spann || segment.scope != SegmentScope::VECTOR {
            return Err(SpannSegmentWriterError::InvalidArgument);
        }
        let params = collection
            .config
            .get_spann_config()
            .ok_or_else(|| SpannSegmentWriterError::MissingSpannConfiguration)?;

        let hnsw_id = match segment.file_path.get(HNSW_PATH) {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError(e));
                        }
                    };
                    Some(IndexUuid(index_uuid))
                }
                None => {
                    return Err(SpannSegmentWriterError::HnswInvalidFilePath);
                }
            },
            None => None,
        };
        let versions_map_id = match segment.file_path.get(VERSION_MAP_PATH) {
            Some(version_map_path) => match version_map_path.first() {
                Some(version_map_id) => {
                    let version_map_uuid = match Uuid::parse_str(version_map_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError(e));
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
                        Err(e) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError(e));
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
                        Err(e) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError(e));
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
            &segment.collection,
            dimensionality,
            blockfile_provider,
            params,
            gc_context,
            metrics,
        )
        .await
        {
            Ok(index_writer) => index_writer,
            Err(e) => {
                tracing::error!("Error creating spann index writer {:?}", e);
                return Err(SpannSegmentWriterError::SpannSegmentWriterCreateError(e));
            }
        };

        Ok(SpannSegmentWriter {
            index: index_writer,
            id: segment.id,
        })
    }

    async fn add(
        &self,
        record: &HydratedMaterializedLogRecord<'_, '_>,
    ) -> Result<(), SpannSegmentWriterError> {
        self.index
            .add(record.get_offset_id(), record.merged_embeddings_ref())
            .await
            .map_err(|e| {
                tracing::error!("Error adding record to spann index writer {:?}", e);
                SpannSegmentWriterError::SpannSegmentWriterMutateRecordError(e)
            })
    }

    async fn delete(
        &self,
        record: &BorrowedMaterializedLogRecord<'_>,
    ) -> Result<(), SpannSegmentWriterError> {
        self.index
            .delete(record.get_offset_id())
            .await
            .map_err(|e| {
                tracing::error!("Error deleting record from spann index writer {:?}", e);
                SpannSegmentWriterError::SpannSegmentWriterMutateRecordError(e)
            })
    }

    async fn update(
        &self,
        record: &HydratedMaterializedLogRecord<'_, '_>,
    ) -> Result<(), SpannSegmentWriterError> {
        self.index
            .update(record.get_offset_id(), record.merged_embeddings_ref())
            .await
            .map_err(|e| {
                tracing::error!("Error updating record in spann index writer {:?}", e);
                SpannSegmentWriterError::SpannSegmentWriterMutateRecordError(e)
            })
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized_chunk: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        tracing::info!(
            "Applying {} materialized logs to spann segment writer",
            materialized_chunk.len()
        );
        for record in materialized_chunk {
            match record.get_operation() {
                MaterializedLogOperation::AddNew => {
                    let record = record
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(ApplyMaterializedLogError::Materialization)?;
                    self.add(&record).await.map_err(|e| {
                        tracing::error!("Error adding record to spann index writer {:?}", e);
                        ApplyMaterializedLogError::SpannSegmentError(e)
                    })?;
                }
                MaterializedLogOperation::UpdateExisting
                | MaterializedLogOperation::OverwriteExisting => {
                    let record = record
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(ApplyMaterializedLogError::Materialization)?;
                    self.update(&record).await.map_err(|e| {
                        tracing::error!("Error updating record in spann index writer {:?}", e);
                        ApplyMaterializedLogError::SpannSegmentError(e)
                    })?;
                }
                MaterializedLogOperation::DeleteExisting => {
                    self.delete(&record).await.map_err(|e| {
                        tracing::error!("Error deleting record from spann index writer {:?}", e);
                        ApplyMaterializedLogError::SpannSegmentError(e)
                    })?;
                }
                MaterializedLogOperation::Initial => panic!(
                    "Invariant violation. Mat records should not contain logs in initial state"
                ),
            }
        }
        tracing::info!(
            "Applied {} materialized logs to spann segment writer",
            materialized_chunk.len()
        );
        Ok(())
    }

    pub async fn garbage_collect(&mut self) -> Result<(), Box<dyn ChromaError>> {
        let r = self.index.garbage_collect().await.map_err(|e| {
            tracing::error!("Error garbage collecting spann index writer {:?}", e);
            Box::new(SpannSegmentWriterError::GarbageCollectError(e))
        });
        match r {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    pub async fn commit(self) -> Result<SpannSegmentFlusher, Box<dyn ChromaError>> {
        tracing::info!("Committing spann segment writer {}", self.id);
        let index_flusher = self.index.commit().await.map_err(|e| {
            tracing::error!("Error committing spann index writer {:?}", e);
            SpannSegmentWriterError::SpannSegmentWriterCommitError(e)
        });
        match index_flusher {
            Err(e) => Err(Box::new(e)),
            Ok(index_flusher) => Ok(SpannSegmentFlusher {
                id: self.id,
                index_flusher,
            }),
        }
    }

    pub fn hnsw_index_uuid(&self) -> IndexUuid {
        self.index.hnsw_index.inner.read().id
    }
}

pub struct SpannSegmentFlusher {
    pub id: SegmentUuid,
    index_flusher: SpannIndexFlusher,
}

impl Debug for SpannSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpannSegmentFlusher").finish()
    }
}

impl SpannSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        tracing::info!("Flushing spann segment flusher {}", self.id);
        let index_flusher_res = self.index_flusher.flush().await.map_err(|e| {
            tracing::error!("Error flushing spann index segment {}: {:?}", self.id, e);
            SpannSegmentWriterError::SpannSegmentWriterFlushError(e)
        });
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
                tracing::info!(
                    "Flushed file paths for spann segment flusher {:?}",
                    index_id_map
                );
                Ok(index_id_map)
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum SpannSegmentReaderError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Collection is missing a spann configuration")]
    MissingSpannConfiguration,
    #[error("Error parsing index uuid from string {0}")]
    IndexIdParsingError(#[source] uuid::Error),
    #[error("Invalid file path for HNSW index")]
    HnswInvalidFilePath,
    #[error("Invalid file path for version map")]
    VersionMapInvalidFilePath,
    #[error("Invalid file path for posting list")]
    PostingListInvalidFilePath,
    #[error("Error constructing spann index reader {0}")]
    SpannSegmentReaderCreateError(#[source] SpannIndexReaderError),
    #[error("Spann segment is uninitialized")]
    UninitializedSegment,
    #[error("Error fetching posting list for key {0}")]
    KeyReadError(#[source] SpannIndexReaderError),
    #[error("Error performing rng query {0}")]
    RngError(#[from] RngQueryError),
}

impl ChromaError for SpannSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::InvalidArgument => ErrorCodes::InvalidArgument,
            Self::IndexIdParsingError(_) => ErrorCodes::Internal,
            Self::HnswInvalidFilePath => ErrorCodes::Internal,
            Self::VersionMapInvalidFilePath => ErrorCodes::Internal,
            Self::PostingListInvalidFilePath => ErrorCodes::Internal,
            Self::SpannSegmentReaderCreateError(e) => e.code(),
            Self::UninitializedSegment => ErrorCodes::Internal,
            Self::KeyReadError(e) => e.code(),
            Self::MissingSpannConfiguration => ErrorCodes::Internal,
            Self::RngError(e) => e.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpannSegmentReader<'me> {
    pub index_reader: SpannIndexReader<'me>,
    #[allow(dead_code)]
    id: SegmentUuid,
    pub params: InternalSpannConfiguration,
}

impl<'me> SpannSegmentReader<'me> {
    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        hnsw_provider: &HnswIndexProvider,
        dimensionality: usize,
    ) -> Result<SpannSegmentReader<'me>, SpannSegmentReaderError> {
        if segment.r#type != SegmentType::Spann || segment.scope != SegmentScope::VECTOR {
            return Err(SpannSegmentReaderError::InvalidArgument);
        }
        let params = collection
            .config
            .get_spann_config()
            .ok_or(SpannSegmentReaderError::MissingSpannConfiguration)?;

        let hnsw_id = match segment.file_path.get(HNSW_PATH) {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            return Err(SpannSegmentReaderError::IndexIdParsingError(e));
                        }
                    };
                    Some(IndexUuid(index_uuid))
                }
                None => {
                    return Err(SpannSegmentReaderError::HnswInvalidFilePath);
                }
            },
            None => None,
        };
        let versions_map_id = match segment.file_path.get(VERSION_MAP_PATH) {
            Some(version_map_path) => match version_map_path.first() {
                Some(version_map_id) => {
                    let version_map_uuid = match Uuid::parse_str(version_map_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            return Err(SpannSegmentReaderError::IndexIdParsingError(e));
                        }
                    };
                    Some(version_map_uuid)
                }
                None => {
                    return Err(SpannSegmentReaderError::VersionMapInvalidFilePath);
                }
            },
            None => None,
        };
        let posting_list_id = match segment.file_path.get(POSTING_LIST_PATH) {
            Some(posting_list_path) => match posting_list_path.first() {
                Some(posting_list_id) => {
                    let posting_list_uuid = match Uuid::parse_str(posting_list_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            return Err(SpannSegmentReaderError::IndexIdParsingError(e));
                        }
                    };
                    Some(posting_list_uuid)
                }
                None => {
                    return Err(SpannSegmentReaderError::PostingListInvalidFilePath);
                }
            },
            None => None,
        };

        let index_reader = match SpannIndexReader::from_id(
            hnsw_id.as_ref(),
            hnsw_provider,
            &segment.collection,
            params.space.clone().into(),
            dimensionality,
            params.ef_search,
            posting_list_id.as_ref(),
            versions_map_id.as_ref(),
            blockfile_provider,
        )
        .await
        {
            Ok(index_writer) => index_writer,
            Err(e) => match e {
                SpannIndexReaderError::UninitializedIndex => {
                    return Err(SpannSegmentReaderError::UninitializedSegment);
                }
                _ => {
                    tracing::error!("Error creating spann segment reader {:?}", e);
                    return Err(SpannSegmentReaderError::SpannSegmentReaderCreateError(e));
                }
            },
        };

        Ok(SpannSegmentReader {
            index_reader,
            id: segment.id,
            params,
        })
    }

    pub async fn fetch_posting_list(
        &self,
        head_id: u32,
    ) -> Result<Vec<SpannPosting>, SpannSegmentReaderError> {
        self.index_reader
            .fetch_posting_list(head_id)
            .await
            .map_err(|e| {
                tracing::error!("Error fetching posting list for head {}:{:?}", head_id, e);
                SpannSegmentReaderError::KeyReadError(e)
            })
    }

    pub async fn rng_query(
        &self,
        normalized_query: &[f32],
    ) -> Result<(Vec<usize>, Vec<f32>, Vec<Vec<f32>>), SpannSegmentReaderError> {
        let r = rng_query(
            normalized_query,
            self.index_reader.hnsw_index.clone(),
            self.params.search_nprobe as usize,
            self.params.search_rng_epsilon,
            self.params.search_rng_factor,
            self.params.space.clone().into(),
            false,
        )
        .await?;
        Ok(r)
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
    use chroma_config::{registry::Registry, Configurable};
    use chroma_index::{
        config::{
            HnswGarbageCollectionConfig, HnswGarbageCollectionPolicyConfig,
            PlGarbageCollectionConfig, PlGarbageCollectionPolicyConfig, RandomSamplePolicyConfig,
        },
        hnsw_provider::HnswIndexProvider,
        spann::types::{GarbageCollectionContext, SpannMetrics},
        Index,
    };
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, Collection, CollectionUuid, InternalCollectionConfiguration,
        InternalSpannConfiguration, LogRecord, Operation, OperationRecord, SegmentUuid,
        SpannPostingList,
    };

    use crate::{
        distributed_spann::{SpannSegmentReader, SpannSegmentWriter},
        types::materialize_logs,
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
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let collection_id = CollectionUuid::new();
        let segment_id = SegmentUuid::new();
        let params = InternalSpannConfiguration::default();
        let mut spann_segment = chroma_types::Segment {
            id: segment_id,
            collection: collection_id,
            r#type: chroma_types::SegmentType::Spann,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: None,
            file_path: HashMap::new(),
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");

        let collection = chroma_types::Collection {
            collection_id,
            name: "test".to_string(),
            config: chroma_types::InternalCollectionConfiguration {
                vector_index: chroma_types::VectorIndexConfiguration::Spann(params),
                embedding_function: None,
            },
            metadata: None,
            dimension: None,
            tenant: "test".to_string(),
            database: "test".to_string(),
            ..Default::default()
        };

        let spann_writer = SpannSegmentWriter::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
            gc_context,
            SpannMetrics::default(),
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
        let materialized_log = materialize_logs(&None, chunked_log, None)
            .await
            .expect("Error materializing logs");
        spann_writer
            .apply_materialized_log_chunk(&None, &materialized_log)
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
        let hnsw_provider = HnswIndexProvider::new(
            storage,
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let spann_writer = SpannSegmentWriter::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann segment writer");
        assert_eq!(spann_writer.index.dimensionality, 3);
        assert_eq!(
            spann_writer.index.params,
            InternalSpannConfiguration::default()
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
            let read_guard = spann_writer.index.versions_map.read().await;
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

    #[tokio::test]
    async fn test_spann_segment_reader() {
        // Tests that after the writer writes and flushes data, reader is able
        // to read it.
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
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let collection_id = CollectionUuid::new();
        let segment_id = SegmentUuid::new();
        let params = InternalSpannConfiguration::default();
        let mut spann_segment = chroma_types::Segment {
            id: segment_id,
            collection: collection_id,
            r#type: chroma_types::SegmentType::Spann,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: None,
            file_path: HashMap::new(),
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");

        let collection = Collection {
            collection_id,
            config: InternalCollectionConfiguration {
                vector_index: chroma_types::VectorIndexConfiguration::Spann(params),
                embedding_function: None,
            },
            ..Default::default()
        };

        let spann_writer = SpannSegmentWriter::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
            gc_context,
            SpannMetrics::default(),
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
        let materialized_log = materialize_logs(&None, chunked_log, None)
            .await
            .expect("Error materializing logs");
        spann_writer
            .apply_materialized_log_chunk(&None, &materialized_log)
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
        let hnsw_provider = HnswIndexProvider::new(
            storage,
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let spann_reader = SpannSegmentReader::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
        )
        .await
        .expect("Error creating segment reader");
        let (non_deleted_centers, deleted_centers) = spann_reader
            .index_reader
            .hnsw_index
            .inner
            .read()
            .get_all_ids()
            .expect("Error getting all ids from hnsw index");
        assert_eq!(non_deleted_centers.len(), 1);
        assert_eq!(deleted_centers.len(), 0);
        assert_eq!(non_deleted_centers[0], 1);
        let mut pl = spann_reader
            .index_reader
            .posting_lists
            .get_range(.., ..)
            .await
            .expect("Error getting all data from reader");
        pl.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(pl.len(), 1);
        assert_eq!(pl[0].2.doc_offset_ids, &[1, 2]);
        assert_eq!(pl[0].2.doc_versions, &[1, 1]);
        assert_eq!(pl[0].2.doc_embeddings, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut versions_map = spann_reader
            .index_reader
            .versions_map
            .get_range(.., ..)
            .await
            .expect("Error gettting all data from reader");
        versions_map.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            versions_map
                .into_iter()
                .map(|t| (t.1, t.2))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 1)]
        );
    }

    #[tokio::test]
    async fn test_spann_segment_writer_with_gc() {
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
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let collection_id = CollectionUuid::new();

        let collection = Collection {
            collection_id,
            config: InternalCollectionConfiguration::default_spann(),
            ..Default::default()
        };

        let segment_id = SegmentUuid::new();
        let mut spann_segment = chroma_types::Segment {
            id: segment_id,
            collection: collection_id,
            r#type: chroma_types::SegmentType::Spann,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: None,
            file_path: HashMap::new(),
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig {
                    enabled: true,
                    policy: PlGarbageCollectionPolicyConfig::RandomSample(
                        RandomSamplePolicyConfig { sample_size: 1.0 },
                    ),
                },
                HnswGarbageCollectionConfig {
                    enabled: true,
                    policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
                },
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let spann_writer = SpannSegmentWriter::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
            gc_context,
            SpannMetrics::default(),
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
        let materialized_log = materialize_logs(&None, chunked_log, None)
            .await
            .expect("Error materializing logs");
        spann_writer
            .apply_materialized_log_chunk(&None, &materialized_log)
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
        let hnsw_provider = HnswIndexProvider::new(
            storage,
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig {
                    enabled: true,
                    policy: PlGarbageCollectionPolicyConfig::RandomSample(
                        RandomSamplePolicyConfig { sample_size: 1.0 },
                    ),
                },
                HnswGarbageCollectionConfig {
                    enabled: true,
                    policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
                },
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");

        let collection = Collection {
            collection_id,
            config: InternalCollectionConfiguration::default_spann(),
            ..Default::default()
        };

        let spann_writer = SpannSegmentWriter::from_segment(
            &collection,
            &spann_segment,
            &blockfile_provider,
            &hnsw_provider,
            3,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann segment writer");
        assert_eq!(spann_writer.index.dimensionality, 3);
        assert_eq!(
            spann_writer.index.params,
            InternalSpannConfiguration::default()
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
            let read_guard = spann_writer.index.versions_map.read().await;
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
