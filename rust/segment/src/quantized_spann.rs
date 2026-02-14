use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileReader,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    spann::quantized_spann::{
        QuantizedSpannError, QuantizedSpannFlusher, QuantizedSpannIds, QuantizedSpannIndexWriter,
        PREFIX_CENTER, PREFIX_ROTATION, PREFIX_VERSION, SINGLETON_KEY,
    },
    usearch::{USearchIndex, USearchIndexConfig, USearchIndexProvider},
    IndexUuid, OpenMode, VectorIndex, VectorIndexProvider,
};
use chroma_types::{
    default_construction_ef_spann, default_m_spann, default_search_ef_spann, Collection,
    MaterializedLogOperation, QuantizedCluster, QuantizedClusterOwned, Schema, SchemaError,
    Segment, SegmentScope, SegmentType, SegmentUuid, OFFSET_ID_TO_DATA, QUANTIZED_SPANN_CLUSTER,
    QUANTIZED_SPANN_EMBEDDING_METADATA, QUANTIZED_SPANN_QUANTIZED_CENTROID,
    QUANTIZED_SPANN_RAW_CENTROID, QUANTIZED_SPANN_SCALAR_METADATA,
};
use faer::{col::ColRef, Mat};
use thiserror::Error;

use crate::blockfile_record::ApplyMaterializedLogError;
use crate::types::{ChromaSegmentFlusher, MaterializeLogsResult};

#[derive(Error, Debug)]
pub enum QuantizedSpannSegmentError {
    #[error("quantized spann config error: {0}")]
    Config(String),
    #[error("quantized spann data error: {0}")]
    Data(String),
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
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
            Self::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
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

#[derive(Clone)]
pub struct QuantizedSpannSegmentReader {
    // Centroid index (for navigate)
    quantized_centroid: USearchIndex,

    // Quantization parameters (for rotate + scoring)
    dimension: usize,
    distance_function: DistanceFunction,
    rotation: Arc<Mat<f32>>,

    // Blockfile readers
    quantized_cluster_reader: BlockfileReader<'static, u32, QuantizedCluster<'static>>,
    versions_reader: BlockfileReader<'static, u32, u32>,
}

impl Debug for QuantizedSpannSegmentReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannSegmentReader").finish()
    }
}

impl QuantizedSpannSegmentReader {
    pub async fn from_segment(
        collection: &Collection,
        vector_segment: &Segment,
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

        let dimension = collection.dimension.ok_or_else(|| {
            QuantizedSpannSegmentError::Config("collection dimension not set".to_string())
        })? as usize;

        // Parse file paths — all 5 must be present for a readable segment.
        let file_path_keys = [
            QUANTIZED_SPANN_CLUSTER,
            QUANTIZED_SPANN_EMBEDDING_METADATA,
            QUANTIZED_SPANN_QUANTIZED_CENTROID,
            QUANTIZED_SPANN_RAW_CENTROID,
            QUANTIZED_SPANN_SCALAR_METADATA,
        ];

        let mut parsed = Vec::new();
        for key in &file_path_keys {
            let paths = vector_segment.file_path.get(*key).ok_or_else(|| {
                QuantizedSpannSegmentError::Data("uninitialized segment".to_string())
            })?;
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

        // Validate all prefixes are consistent.
        let prefix_path = parsed[0].0;
        for (i, (p, _)) in parsed.iter().enumerate().skip(1) {
            if *p != prefix_path {
                return Err(QuantizedSpannSegmentError::Config(format!(
                    "inconsistent prefix path for {}",
                    file_path_keys[i]
                )));
            }
        }
        let prefix_path = prefix_path.to_string();

        // Decompose parsed file IDs.
        // Order matches file_path_keys: cluster[0], embedding_metadata[1],
        // quantized_centroid[2], raw_centroid[3], scalar_metadata[4].
        let cluster_id = parsed[0].1;
        let embedding_metadata_id = parsed[1].1;
        let quantized_centroid_id = IndexUuid(parsed[2].1);
        // parsed[3] is raw_centroid — not needed for the reader.
        let scalar_metadata_id = parsed[4].1;

        // Step 1: Open embedding_metadata → load rotation matrix + center.
        let emb_meta_options =
            BlockfileReaderOptions::new(embedding_metadata_id, prefix_path.clone());
        let emb_meta_reader = blockfile_provider
            .read::<u32, &'static [f32]>(emb_meta_options)
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!(
                    "failed to open embedding metadata reader: {e}"
                ))
            })?;

        let columns = emb_meta_reader
            .get_range(PREFIX_ROTATION..=PREFIX_ROTATION, ..)
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!("failed to read rotation matrix: {e}"))
            })?
            .collect::<Vec<_>>();

        if columns.len() != dimension {
            return Err(QuantizedSpannSegmentError::Data(format!(
                "rotation matrix column count mismatch: expected {dimension}, got {}",
                columns.len()
            )));
        }
        for (_prefix, _key, col) in &columns {
            if col.len() != dimension {
                return Err(QuantizedSpannSegmentError::Data(format!(
                    "rotation matrix column length mismatch: expected {dimension}, got {}",
                    col.len()
                )));
            }
        }
        let rotation = Mat::from_fn(dimension, dimension, |i, j| columns[j].2[i]).into();

        let center = emb_meta_reader
            .get(PREFIX_CENTER, SINGLETON_KEY)
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!("failed to read quantization center: {e}"))
            })?
            .map(Arc::<[f32]>::from)
            .unwrap_or_else(|| vec![0.0; dimension].into());

        // Step 2: Open quantized centroid usearch index (read-only).
        let max_neighbors = spann_config.max_neighbors.unwrap_or(default_m_spann());
        let ef_construction = spann_config
            .ef_construction
            .unwrap_or(default_construction_ef_spann());
        let ef_search = spann_config.ef_search.unwrap_or(default_search_ef_spann());

        let usearch_config = USearchIndexConfig {
            collection_id: vector_segment.collection,
            cmek: schema.cmek.clone(),
            prefix_path: prefix_path.clone(),
            dimensions: dimension,
            distance_function: distance_function.clone(),
            connectivity: max_neighbors,
            expansion_add: ef_construction,
            expansion_search: ef_search,
            quantization_center: Some(center),
        };
        let quantized_centroid = usearch_provider
            .open(&usearch_config, OpenMode::Open(quantized_centroid_id))
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!(
                    "failed to open quantized centroid index: {e}"
                ))
            })?;

        // Step 3: Open quantized cluster blockfile reader.
        let cluster_options = BlockfileReaderOptions::new(cluster_id, prefix_path.clone());
        let quantized_cluster_reader =
            blockfile_provider
                .read(cluster_options)
                .await
                .map_err(|e| {
                    QuantizedSpannSegmentError::Data(format!(
                        "failed to open quantized cluster reader: {e}"
                    ))
                })?;

        // Step 4: Open scalar_metadata blockfile reader (for version lookups)
        //         and preload all version blocks into the blockfile cache.
        let scalar_options = BlockfileReaderOptions::new(scalar_metadata_id, prefix_path.clone());
        let versions_reader = blockfile_provider.read(scalar_options).await.map_err(|e| {
            QuantizedSpannSegmentError::Data(format!("failed to open scalar metadata reader: {e}"))
        })?;
        versions_reader
            .load_blocks_for_prefixes(std::iter::once(PREFIX_VERSION))
            .await;

        Ok(Self {
            dimension,
            distance_function,
            quantized_centroid,
            quantized_cluster_reader,
            rotation,
            versions_reader,
        })
    }

    /// Rotate a query vector into the quantized space.
    /// Applies normalization for cosine distance, then multiplies by the rotation matrix.
    pub fn rotate(&self, query: &[f32]) -> Result<Vec<f32>, QuantizedSpannSegmentError> {
        if query.len() != self.dimension {
            return Err(QuantizedSpannSegmentError::DimensionMismatch {
                expected: self.dimension,
                actual: query.len(),
            });
        }

        let rotated = match self.distance_function {
            DistanceFunction::Cosine => {
                let normalized = normalize(query);
                self.rotation.as_ref() * ColRef::from_slice(&normalized)
            }
            _ => self.rotation.as_ref() * ColRef::from_slice(query),
        };
        Ok(rotated.iter().copied().collect())
    }

    /// Find nearest cluster heads using the quantized centroid index.
    /// `rotated_query` must be the output of `rotate()`.
    pub fn navigate(
        &self,
        rotated_query: &[f32],
        count: usize,
    ) -> Result<Vec<u32>, QuantizedSpannSegmentError> {
        if rotated_query.len() != self.dimension {
            return Err(QuantizedSpannSegmentError::DimensionMismatch {
                expected: self.dimension,
                actual: rotated_query.len(),
            });
        }

        let result = self
            .quantized_centroid
            .search(rotated_query, count)
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!("centroid search failed: {e}"))
            })?;
        Ok(result.keys)
    }

    /// Read a single cluster from the blockfile.
    pub async fn get_cluster(
        &self,
        cluster_id: u32,
    ) -> Result<QuantizedClusterOwned, QuantizedSpannSegmentError> {
        let cluster = self
            .quantized_cluster_reader
            .get("", cluster_id)
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!(
                    "failed to read cluster {cluster_id}: {e}"
                ))
            })?;

        match cluster {
            Some(c) => Ok(c.into()),
            None => Err(QuantizedSpannSegmentError::Data(format!(
                "cluster {cluster_id} not found"
            ))),
        }
    }

    /// Read the global version for a single point.
    pub async fn get_version(&self, id: u32) -> Result<u32, QuantizedSpannSegmentError> {
        let version = self
            .versions_reader
            .get(PREFIX_VERSION, id)
            .await
            .map_err(|e| {
                QuantizedSpannSegmentError::Data(format!("failed to read version for id {id}: {e}"))
            })?;

        match version {
            Some(v) => Ok(v),
            None => Err(QuantizedSpannSegmentError::Data(format!(
                "version for id {id} not found"
            ))),
        }
    }

    /// Return the configured distance function.
    pub fn distance_function(&self) -> &DistanceFunction {
        &self.distance_function
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
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
        OperationRecord, Schema, Segment, SegmentScope, SegmentType, SegmentUuid, Space,
        VectorIndexConfiguration, OFFSET_ID_TO_DATA, QUANTIZED_SPANN_CLUSTER,
        QUANTIZED_SPANN_EMBEDDING_METADATA, QUANTIZED_SPANN_QUANTIZED_CENTROID,
        QUANTIZED_SPANN_RAW_CENTROID, QUANTIZED_SPANN_SCALAR_METADATA,
    };
    use rand::{Rng, SeedableRng};

    use super::{QuantizedSpannSegmentReader, QuantizedSpannSegmentWriter};
    use crate::types::materialize_logs;

    const CLUSTER_BLOCK_SIZE: usize = 2 * 1024 * 1024;
    const DIMENSION: usize = 1024;
    const NUM_CYCLES: usize = 4;
    const BATCH_SIZE: usize = 32;
    const SEED: u64 = 42;
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
        let params = InternalSpannConfiguration {
            ef_construction: 32,
            ef_search: 16,
            max_neighbors: 8,
            merge_threshold: 2,
            nreplica_count: 2,
            reassign_neighbor_count: 6,
            search_nprobe: 4,
            search_rng_epsilon: 4.0,
            search_rng_factor: 1.0,
            space: Space::Cosine,
            split_threshold: 8,
            write_nprobe: 4,
            write_rng_epsilon: 4.0,
            write_rng_factor: 1.0,
            ..Default::default()
        };
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

    /// Generate embeddings for all points upfront with fixed-seed RNG.
    /// Index i corresponds to offset_id (i + 1).
    fn test_embeddings() -> Vec<[f32; DIMENSION]> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
        (0..TOTAL_POINTS)
            .map(|_| std::array::from_fn(|_| rng.gen()))
            .collect()
    }

    fn make_log_records(start_id: usize, embeddings: &[[f32; DIMENSION]]) -> Vec<LogRecord> {
        embeddings
            .iter()
            .enumerate()
            .map(|(i, emb)| {
                let id = start_id + i;
                LogRecord {
                    log_offset: (id + 1) as i64,
                    record: OperationRecord {
                        id: format!("point_{id}"),
                        embedding: Some(emb.to_vec()),
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
            let logs = make_log_records(start_id, &embeddings[start_id..start_id + BATCH_SIZE]);
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

        // === Reader verification ===
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        let reader = QuantizedSpannSegmentReader::from_segment(
            &collection,
            &vector_segment,
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("failed to open reader");

        // Verify versions exist for all points.
        let versions = reader
            .versions_reader
            .get_range(super::PREFIX_VERSION..=super::PREFIX_VERSION, ..)
            .await
            .expect("failed to read versions")
            .collect::<Vec<_>>();

        assert_eq!(
            versions.len(),
            TOTAL_POINTS,
            "expected {TOTAL_POINTS} version entries, got {}",
            versions.len(),
        );

        let version_ids = versions
            .iter()
            .map(|(_, id, _)| *id)
            .collect::<HashSet<_>>();
        for offset_id in 1..=TOTAL_POINTS as u32 {
            assert!(
                version_ids.contains(&offset_id),
                "missing version entry for offset_id {offset_id}",
            );
        }

        // Scan all cluster IDs from the blockfile.
        let all_clusters = reader
            .quantized_cluster_reader
            .get_range(.., ..)
            .await
            .expect("failed to scan clusters")
            .map(|(_, key, _)| key)
            .collect::<Vec<u32>>();
        assert!(
            !all_clusters.is_empty(),
            "expected at least one cluster in blockfile",
        );

        // Rotate a query vector.
        let query = embeddings[0].as_slice();
        let rotated = reader.rotate(query).expect("rotate failed");
        assert_eq!(rotated.len(), DIMENSION);

        // Navigate — request enough to cover all clusters.
        let navigate_ids = reader
            .navigate(&rotated, TOTAL_POINTS)
            .expect("navigate failed");
        assert!(!navigate_ids.is_empty());

        // Verify navigate returns valid cluster IDs.
        let cluster_id_set = all_clusters.iter().copied().collect::<HashSet<_>>();
        for id in &navigate_ids {
            assert!(
                cluster_id_set.contains(id),
                "navigate returned unknown cluster id {id}",
            );
        }

        // Query each cluster using get_cluster + get_version + query_quantized_cluster.
        use chroma_index::spann::utils::query_quantized_cluster;
        use futures::future::try_join_all;

        let mut all_results = HashMap::new();

        for cluster_id in &all_clusters {
            let cluster = reader
                .get_cluster(*cluster_id)
                .await
                .unwrap_or_else(|e| panic!("get_cluster {cluster_id} failed: {e}"));

            let versions = try_join_all(cluster.ids.iter().map(|&id| reader.get_version(id)))
                .await
                .unwrap_or_else(|e| panic!("get_version for cluster {cluster_id} failed: {e}"));

            let global_versions = cluster
                .ids
                .iter()
                .copied()
                .zip(versions)
                .collect::<HashMap<_, _>>();

            let cluster_ref = chroma_types::QuantizedCluster::from(&cluster);
            let result = query_quantized_cluster(
                &cluster_ref,
                &rotated,
                reader.distance_function(),
                |id, version| global_versions.get(&id) == Some(&version),
            );

            // Verify sorted distances.
            for w in result.distances.windows(2) {
                assert!(
                    w[0] <= w[1],
                    "bruteforce results not sorted: {} > {}",
                    w[0],
                    w[1],
                );
            }

            for (offset_id, measure) in result.keys.into_iter().zip(result.distances) {
                all_results.entry(offset_id).or_insert(measure);
            }
        }

        // Verify all points are present.
        for offset_id in 1..=TOTAL_POINTS as u32 {
            assert!(
                all_results.contains_key(&offset_id),
                "missing offset_id {offset_id} in bruteforce results",
            );
        }

        // Verify P95 relative error within 1% (BITS=4, dim=1024 quantization bound).
        let mut rel_errors = all_results
            .iter()
            .map(|(offset_id, measure)| {
                let emb = embeddings[(*offset_id - 1) as usize].as_slice();
                let rotated_emb = reader.rotate(emb).expect("rotate failed");
                let exact = reader.distance_function().distance(&rotated, &rotated_emb);
                (measure - exact).abs() / exact.abs().max(f32::EPSILON)
            })
            .collect::<Vec<_>>();
        rel_errors.sort_by(|a, b| a.total_cmp(b));
        let p90 = rel_errors[rel_errors.len() * 90 / 100];
        assert!(p90 < 1e-2, "P90 relative error {p90} exceeds 1% bound",);
    }
}
