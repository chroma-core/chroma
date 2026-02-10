use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    spann::quantized_spann::{
        QuantizedSpannError, QuantizedSpannFlusher, QuantizedSpannIds, QuantizedSpannIndexWriter,
    },
    usearch::{USearchIndex, USearchIndexProvider},
    IndexUuid,
};
use chroma_types::{
    Collection, MaterializedLogOperation, Schema, SchemaError, Segment, SegmentScope, SegmentType,
    SegmentUuid, OFFSET_ID_TO_DATA, QUANTIZED_SPANN_CLUSTER, QUANTIZED_SPANN_EMBEDDING_METADATA,
    QUANTIZED_SPANN_QUANTIZED_CENTROID, QUANTIZED_SPANN_RAW_CENTROID,
    QUANTIZED_SPANN_SCALAR_METADATA,
};
use thiserror::Error;

use crate::blockfile_record::ApplyMaterializedLogError;
use crate::types::{ChromaSegmentFlusher, MaterializeLogsResult};

#[derive(Error, Debug)]
pub enum QuantizedSpannSegmentError {
    #[error("quantized spann config error: {0}")]
    Config(String),
    #[error("quantized spann data error: {0}")]
    Data(String),
    #[error(transparent)]
    Index(#[from] QuantizedSpannError),
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

impl ChromaError for QuantizedSpannSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Config(_) => ErrorCodes::InvalidArgument,
            Self::Data(_) => ErrorCodes::Internal,
            Self::Index(e) => e.code(),
            Self::Schema(e) => e.code(),
        }
    }
}

#[derive(Clone)]
pub struct QuantizedSpannSegmentWriter {
    blockfile_provider: BlockfileProvider,
    pub id: SegmentUuid,
    index: QuantizedSpannIndexWriter<USearchIndex>,
    usearch_provider: USearchIndexProvider,
}

impl Debug for QuantizedSpannSegmentWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannSegmentWriter")
            .field("id", &self.id)
            .finish()
    }
}

impl QuantizedSpannSegmentWriter {
    pub async fn from_segment(
        cluster_block_size: usize,
        collection: &Collection,
        vector_segment: &Segment,
        record_segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannSegmentError> {
        if vector_segment.r#type != SegmentType::QuantizedSpann
            || vector_segment.scope != SegmentScope::VECTOR
        {
            return Err(QuantizedSpannSegmentError::Config(
                "segment type must be QuantizedSpann with VECTOR scope".to_string(),
            ));
        }

        let schema = match &collection.schema {
            Some(schema) => schema.clone(),
            None => Schema::try_from(&collection.config)?,
        };

        let (spann_config, space) = schema.get_spann_config().ok_or_else(|| {
            QuantizedSpannSegmentError::Config("missing spann configuration".to_string())
        })?;
        let distance_function: DistanceFunction = space.into();
        let cmek = schema.cmek.clone();

        let dimensionality = collection.dimension.ok_or_else(|| {
            QuantizedSpannSegmentError::Config("collection dimension not set".to_string())
        })? as usize;

        // Extract file paths from vector segment metadata.
        let file_path_keys = [
            QUANTIZED_SPANN_CLUSTER,
            QUANTIZED_SPANN_EMBEDDING_METADATA,
            QUANTIZED_SPANN_QUANTIZED_CENTROID,
            QUANTIZED_SPANN_RAW_CENTROID,
            QUANTIZED_SPANN_SCALAR_METADATA,
        ];

        let mut parsed = Vec::new();
        for key in &file_path_keys {
            if let Some(paths) = vector_segment.file_path.get(*key) {
                let path = paths.first().ok_or_else(|| {
                    QuantizedSpannSegmentError::Config(format!("empty file path for {key}"))
                })?;
                let (prefix, id) = Segment::extract_prefix_and_id(path).map_err(|e| {
                    QuantizedSpannSegmentError::Config(format!(
                        "failed to parse file path for {key}: {e}"
                    ))
                })?;
                parsed.push((prefix, id));
            }
        }

        if !parsed.is_empty() && parsed.len() != file_path_keys.len() {
            return Err(QuantizedSpannSegmentError::Config(
                "partial file paths: all or none must be present".to_string(),
            ));
        }

        let prefix_path = if parsed.is_empty() {
            vector_segment.construct_prefix_path(&collection.tenant, &collection.database_id)
        } else {
            let prefix = parsed[0].0;
            for (i, (p, _)) in parsed.iter().enumerate().skip(1) {
                if *p != prefix {
                    return Err(QuantizedSpannSegmentError::Config(format!(
                        "inconsistent prefix path for {}",
                        file_path_keys[i]
                    )));
                }
            }
            prefix.to_string()
        };

        let index = if !parsed.is_empty() {
            // Open the raw embedding reader from the record segment if available.
            let raw_embedding_reader = match record_segment.file_path.get(OFFSET_ID_TO_DATA) {
                Some(paths) => match paths.first() {
                    Some(path) => {
                        let (prefix, id) = Segment::extract_prefix_and_id(path).map_err(|e| {
                            QuantizedSpannSegmentError::Config(format!(
                                "failed to parse record segment file path: {e}"
                            ))
                        })?;
                        let options = BlockfileReaderOptions::new(id, prefix.to_string());
                        let reader = blockfile_provider.read(options).await.map_err(|e| {
                            QuantizedSpannSegmentError::Config(format!(
                                "failed to open record segment reader: {e}"
                            ))
                        })?;
                        Some(reader)
                    }
                    None => None,
                },
                None => None,
            };

            // Order matches file_path_keys: cluster[0], embedding_metadata[1],
            // quantized_centroid[2], raw_centroid[3], scalar_metadata[4].
            let file_ids = QuantizedSpannIds {
                embedding_metadata_id: parsed[1].1,
                prefix_path: prefix_path.clone(),
                quantized_centroid_id: IndexUuid(parsed[2].1),
                quantized_cluster_id: parsed[0].1,
                raw_centroid_id: IndexUuid(parsed[3].1),
                scalar_metadata_id: parsed[4].1,
            };
            QuantizedSpannIndexWriter::open(
                cluster_block_size,
                vector_segment.collection,
                spann_config,
                dimensionality,
                distance_function,
                file_ids,
                cmek,
                prefix_path.clone(),
                raw_embedding_reader,
                blockfile_provider,
                usearch_provider,
            )
            .await?
        } else {
            QuantizedSpannIndexWriter::create(
                cluster_block_size,
                vector_segment.collection,
                spann_config,
                dimensionality,
                distance_function,
                cmek,
                prefix_path.clone(),
                usearch_provider,
            )
            .await?
        };

        Ok(Self {
            blockfile_provider: blockfile_provider.clone(),
            id: vector_segment.id,
            index,
            usearch_provider: usearch_provider.clone(),
        })
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        materialized_chunk: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in materialized_chunk {
            match record.get_operation() {
                MaterializedLogOperation::AddNew
                | MaterializedLogOperation::OverwriteExisting => {
                    let embedding =
                        record.embeddings_ref_from_log().ok_or_else(|| {
                            QuantizedSpannSegmentError::Data(
                                "embedding missing for add/overwrite operation".to_string(),
                            )
                        })?;
                    self.index
                        .add(record.get_offset_id(), embedding)
                        .await
                        .map_err(QuantizedSpannSegmentError::from)?;
                }
                MaterializedLogOperation::UpdateExisting => {
                    if let Some(embedding) = record.embeddings_ref_from_log() {
                        self.index
                            .add(record.get_offset_id(), embedding)
                            .await
                            .map_err(QuantizedSpannSegmentError::from)?;
                    }
                }
                MaterializedLogOperation::DeleteExisting => {
                    self.index.remove(record.get_offset_id());
                }
                MaterializedLogOperation::Initial => panic!(
                    "Invariant violation. Materialized records should not contain logs in initial state"
                ),
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .finish(&self.usearch_provider)
            .await
            .map_err(|e| Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>)
    }

    pub async fn commit(self) -> Result<QuantizedSpannSegmentFlusher, Box<dyn ChromaError>> {
        let flusher = self
            .index
            .commit(&self.blockfile_provider, &self.usearch_provider)
            .await
            .map_err(|e| Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>)?;
        Ok(QuantizedSpannSegmentFlusher {
            flusher,
            id: self.id,
        })
    }
}

pub struct QuantizedSpannSegmentFlusher {
    flusher: QuantizedSpannFlusher,
    pub id: SegmentUuid,
}

impl Debug for QuantizedSpannSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannSegmentFlusher")
            .field("id", &self.id)
            .finish()
    }
}

impl QuantizedSpannSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let ids = Box::pin(self.flusher.flush())
            .await
            .map_err(|e| Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>)?;

        let mut file_paths = HashMap::new();
        file_paths.insert(
            QUANTIZED_SPANN_CLUSTER.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.quantized_cluster_id,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_EMBEDDING_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.embedding_metadata_id,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_QUANTIZED_CENTROID.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.quantized_centroid_id.0,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_RAW_CENTROID.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.raw_centroid_id.0,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_SCALAR_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.scalar_metadata_id,
            )],
        );

        Ok(file_paths)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
        },
        provider::BlockfileProvider,
        BlockfileWriterOptions,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_index::usearch::USearchIndexProvider;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, Collection, CollectionUuid, DataRecord, DatabaseUuid,
        InternalCollectionConfiguration, InternalSpannConfiguration, LogRecord, Operation,
        OperationRecord, Schema, Segment, SegmentScope, SegmentType, SegmentUuid,
        VectorIndexConfiguration, OFFSET_ID_TO_DATA, QUANTIZED_SPANN_CLUSTER,
        QUANTIZED_SPANN_EMBEDDING_METADATA, QUANTIZED_SPANN_QUANTIZED_CENTROID,
        QUANTIZED_SPANN_RAW_CENTROID, QUANTIZED_SPANN_SCALAR_METADATA,
    };

    use super::QuantizedSpannSegmentWriter;
    use crate::types::materialize_logs;

    const CLUSTER_BLOCK_SIZE: usize = 2 * 1024 * 1024;
    const DIMENSION: usize = 4;
    const NUM_CYCLES: usize = 3;
    const BATCH_SIZE: usize = 10;
    const TOTAL_POINTS: usize = NUM_CYCLES * BATCH_SIZE;

    fn test_blockfile_provider(storage: Storage) -> BlockfileProvider {
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
    }

    fn test_usearch_provider(storage: Storage) -> USearchIndexProvider {
        let usearch_cache = new_non_persistent_cache_for_test();
        USearchIndexProvider::new(storage, usearch_cache)
    }

    fn test_collection(collection_id: CollectionUuid, db_id: DatabaseUuid) -> Collection {
        let params = InternalSpannConfiguration::default();
        let config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(params),
            embedding_function: None,
        };
        let schema = Schema::try_from(&config).expect("failed to create schema from test config");
        Collection {
            collection_id,
            name: "test".to_string(),
            config,
            metadata: None,
            dimension: Some(DIMENSION as i32),
            tenant: "test".to_string(),
            database: "test".to_string(),
            database_id: db_id,
            schema: Some(schema),
            ..Default::default()
        }
    }

    /// Generate embeddings for all points upfront. Index i corresponds to
    /// offset_id (i + 1).
    fn test_embeddings() -> Vec<[f32; DIMENSION]> {
        (0..TOTAL_POINTS)
            .map(|i| {
                let v = i as f32;
                [v, v + 0.1, v + 0.2, v + 0.3]
            })
            .collect()
    }

    fn make_log_records(start_id: usize, count: usize) -> Vec<LogRecord> {
        (0..count)
            .map(|i| {
                let id = start_id + i;
                let val = id as f32;
                LogRecord {
                    log_offset: (id + 1) as i64,
                    record: OperationRecord {
                        id: format!("point_{id}"),
                        embedding: Some(vec![val, val + 0.1, val + 0.2, val + 0.3]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn test_quantized_spann_segment_writer_persist() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let collection_id = CollectionUuid::new();
        let db_id = DatabaseUuid::new();
        let collection = test_collection(collection_id, db_id);
        let embeddings = test_embeddings();

        // === Pre-populate raw embedding blockfile ===
        // The record segment needs a blockfile mapping offset_id -> DataRecord
        // so the quantized spann writer can read raw embeddings on reopen.
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let raw_writer = blockfile_provider
            .write::<u32, &DataRecord<'_>>(
                BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
            )
            .await
            .expect("failed to create raw embedding writer");

        for (i, embedding) in embeddings.iter().enumerate() {
            let offset_id = (i + 1) as u32;
            let record = DataRecord {
                id: "",
                embedding: embedding.as_slice(),
                metadata: None,
                document: None,
            };
            raw_writer
                .set("", offset_id, &record)
                .await
                .expect("failed to write raw embedding");
        }

        let raw_flusher = raw_writer
            .commit::<u32, &DataRecord<'_>>()
            .await
            .expect("failed to commit raw embeddings");
        let raw_embedding_id = raw_flusher.id();
        raw_flusher
            .flush::<u32, &DataRecord<'_>>()
            .await
            .expect("failed to flush raw embeddings");

        // === Segments ===
        let segment_id = SegmentUuid::new();
        let mut vector_segment = Segment {
            id: segment_id,
            collection: collection_id,
            r#type: SegmentType::QuantizedSpann,
            scope: SegmentScope::VECTOR,
            metadata: None,
            file_path: HashMap::new(),
        };
        let record_segment = Segment {
            id: SegmentUuid::new(),
            collection: collection_id,
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            metadata: None,
            file_path: HashMap::from([(
                OFFSET_ID_TO_DATA.to_string(),
                vec![raw_embedding_id.to_string()],
            )]),
        };

        let expected_prefix = format!(
            "tenant/test/database/{}/collection/{}/segment/{}",
            db_id, collection_id, segment_id,
        );

        let file_path_keys = [
            QUANTIZED_SPANN_CLUSTER,
            QUANTIZED_SPANN_EMBEDDING_METADATA,
            QUANTIZED_SPANN_QUANTIZED_CENTROID,
            QUANTIZED_SPANN_RAW_CENTROID,
            QUANTIZED_SPANN_SCALAR_METADATA,
        ];

        let next_offset_id = Arc::new(AtomicU32::new(1));

        for cycle in 0..NUM_CYCLES {
            let blockfile_provider = test_blockfile_provider(storage.clone());
            let usearch_provider = test_usearch_provider(storage.clone());

            let mut writer = QuantizedSpannSegmentWriter::from_segment(
                CLUSTER_BLOCK_SIZE,
                &collection,
                &vector_segment,
                &record_segment,
                &blockfile_provider,
                &usearch_provider,
            )
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle}: from_segment failed: {e}"));

            let start_id = cycle * BATCH_SIZE;
            let logs = make_log_records(start_id, BATCH_SIZE);
            let chunked = Chunk::new(logs.into());
            let materialized = materialize_logs(&None, chunked, Some(next_offset_id.clone()))
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: materialize failed: {e}"));

            writer
                .apply_materialized_log_chunk(&materialized)
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: apply failed: {e}"));

            writer
                .finish()
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: finish failed: {e}"));

            let flusher = Box::pin(writer.commit())
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: commit failed: {e}"));

            vector_segment.file_path = flusher
                .flush()
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: flush failed: {e}"));

            // Verify 5 file path keys with correct prefix.
            assert_eq!(
                vector_segment.file_path.len(),
                file_path_keys.len(),
                "cycle {cycle}: expected {} file path keys",
                file_path_keys.len(),
            );
            for key in &file_path_keys {
                let paths = vector_segment
                    .file_path
                    .get(*key)
                    .unwrap_or_else(|| panic!("cycle {cycle}: missing key {key}"));
                assert_eq!(
                    paths.len(),
                    1,
                    "cycle {cycle}: key {key} should have 1 path"
                );
                assert!(
                    paths[0].starts_with(&expected_prefix),
                    "cycle {cycle}: path '{}' should start with '{expected_prefix}'",
                    paths[0],
                );
            }
        }

        // Verify reopen succeeds after all cycles.
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        QuantizedSpannSegmentWriter::from_segment(
            CLUSTER_BLOCK_SIZE,
            &collection,
            &vector_segment,
            &record_segment,
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("final reopen failed");

        // Open the scalar metadata blockfile and verify versions exist for all points.
        let scalar_path = &vector_segment.file_path[QUANTIZED_SPANN_SCALAR_METADATA][0];
        let (prefix, id) = Segment::extract_prefix_and_id(scalar_path)
            .expect("failed to parse scalar metadata path");
        let reader = blockfile_provider
            .read::<u32, u32>(BlockfileReaderOptions::new(id, prefix.to_string()))
            .await
            .expect("failed to open scalar metadata reader");

        let versions: Vec<(&str, u32, u32)> = reader
            .get_range("version"..="version", ..)
            .await
            .expect("failed to read versions")
            .collect();

        assert_eq!(
            versions.len(),
            TOTAL_POINTS,
            "expected {TOTAL_POINTS} version entries, got {}",
            versions.len(),
        );

        // Verify every offset_id 1..=TOTAL_POINTS has a version entry.
        let version_ids: std::collections::HashSet<u32> =
            versions.iter().map(|(_, id, _)| *id).collect();
        for offset_id in 1..=TOTAL_POINTS as u32 {
            assert!(
                version_ids.contains(&offset_id),
                "missing version entry for offset_id {offset_id}",
            );
        }
    }
}
