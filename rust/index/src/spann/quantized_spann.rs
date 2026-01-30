use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, BlockfileFlusher, BlockfileReader,
    BlockfileWriterOptions,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Cmek, DataRecord, QuantizedCluster};
use dashmap::{DashMap, DashSet};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};
use thiserror::Error;
use uuid::Uuid;

use chroma_blockstore::provider::BlockfileProvider;

use crate::{
    quantization::Code,
    usearch::{USearchIndex, USearchIndexConfig, USearchIndexProvider},
    IndexUuid, OpenMode, SearchResult, VectorIndex, VectorIndexProvider,
};

// TODO: Re-enable when split() is implemented
// use super::utils::{cluster, query_quantized_cluster, KMeansAlgorithmInput};

// Blockfile prefixes
const PREFIX_CENTER: &str = "center";
const PREFIX_LENGTH: &str = "length";
const PREFIX_NEXT_CLUSTER: &str = "next";
const PREFIX_ROTATION: &str = "rotation";
const PREFIX_VERSION: &str = "version";

/// In-memory staging for a quantized cluster head.
struct QuantizedDelta {
    center: Arc<[f32]>,
    codes: Vec<Arc<[u8]>>,
    ids: Vec<u32>,
    length: usize,
    versions: Vec<u32>,
}

/// Configuration for quantized SPANN index.
#[derive(Clone)]
pub struct QuantizedSpannConfig {
    // === Shared ===
    pub cmek: Option<Cmek>,
    pub prefix_path: String,
    pub dimensions: usize,
    pub distance_function: DistanceFunction,

    // === SPANN ===
    pub spann_nprobe: usize,
    pub spann_replica_count: usize,
    pub spann_rng_epsilon: f32,
    pub spann_rng_factor: f32,
    pub spann_split_threshold: usize,
    pub spann_merge_threshold: usize,

    // === Blockfile IDs ===
    pub embedding_metadata_id: Option<Uuid>,
    pub quantized_centroid_id: Option<IndexUuid>,
    pub quantized_cluster_id: Option<Uuid>,
    pub raw_centroid_id: Option<IndexUuid>,
    pub raw_embedding_id: Option<Uuid>,
    pub scalar_metadata_id: Option<Uuid>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannError {
    #[error("Centroid index error: {0}")]
    CentroidIndex(Box<dyn ChromaError>),
    #[error("Blockfile error: {0}")]
    Blockfile(Box<dyn ChromaError>),
    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
}

impl ChromaError for QuantizedSpannError {
    fn code(&self) -> ErrorCodes {
        match self {
            QuantizedSpannError::CentroidIndex(err) => err.code(),
            QuantizedSpannError::Blockfile(err) => err.code(),
            QuantizedSpannError::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
        }
    }
}

/// Mutable quantized SPANN index, generic over centroid index.
pub struct MutableQuantizedSpannIndex<I: VectorIndex> {
    // === Config ===
    config: QuantizedSpannConfig,

    // === Centroid Index ===
    next_cluster_id: Arc<AtomicU32>,
    quantized_centroid: I,
    raw_centroid: I,

    // === Quantization ===
    center: Arc<[f32]>,
    rotation: Mat<f32>,

    // === In-Memory State ===
    deltas: Arc<DashMap<u32, QuantizedDelta>>,
    embeddings: Arc<DashMap<u32, Arc<[f32]>>>,
    tombstones: Arc<DashSet<u32>>,
    versions: Arc<DashMap<u32, u32>>,

    // === Blockfile Readers ===
    quantized_cluster_reader: Option<BlockfileReader<'static, u32, QuantizedCluster<'static>>>,
    // NOTE(sicheng): This is the record segment's id_to_data blockfile reader.
    // This is a temporary solution for loading raw embeddings; a dedicated
    // raw embedding store may be introduced in the future.
    raw_embedding_reader: Option<BlockfileReader<'static, u32, DataRecord<'static>>>,
}

impl<I: VectorIndex> MutableQuantizedSpannIndex<I> {
    pub async fn add(&self, key: u32, vector: &[f32]) -> Result<(), QuantizedSpannError> {
        let rotated = self.rotate(vector);
        self.embeddings.insert(key, rotated.clone());
        self.insert(key, rotated).await
    }

    pub fn remove(&self, key: u32) {
        self.upgrade(key);
    }
}

impl<I: VectorIndex> MutableQuantizedSpannIndex<I> {
    /// Append a point to an existing cluster. Returns new length, or None if cluster not found.
    fn append(&self, cluster_id: u32, id: u32, version: u32, code: Arc<[u8]>) -> Option<usize> {
        let mut delta = self.deltas.get_mut(&cluster_id)?;
        delta.codes.push(code);
        delta.ids.push(id);
        delta.length += 1;
        delta.versions.push(version);
        Some(delta.length)
    }

    /// Balance a cluster: scrub then trigger split/merge if needed.
    async fn balance(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(len) = self.scrub(cluster_id).await? else {
            return Ok(());
        };

        if len > self.config.spann_split_threshold {
            self.split(cluster_id).await?;
        } else if len > 0 && len < self.config.spann_merge_threshold {
            self.merge(cluster_id).await?;
        }

        Ok(())
    }

    /// Get the centroid for a cluster, cloning to release the lock.
    fn centroid(&self, cluster_id: u32) -> Option<Arc<[f32]>> {
        self.deltas.get(&cluster_id).map(|d| d.center.clone())
    }

    /// Create a new cluster and register it in the centroid index.
    fn create(&self, delta: QuantizedDelta) -> Result<u32, QuantizedSpannError> {
        let id = self.next_cluster_id.fetch_add(1, Ordering::Relaxed);
        let center = delta.center.clone();
        self.deltas.insert(id, delta);
        self.raw_centroid
            .add(id, &center)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        self.quantized_centroid
            .add(id, &center)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        Ok(id)
    }

    /// Remove a cluster from deltas and load raw embeddings for its valid points.
    /// Returns the delta if the cluster existed.
    async fn detach(&self, cluster_id: u32) -> Result<Option<QuantizedDelta>, QuantizedSpannError> {
        let Some((_, delta)) = self.deltas.remove(&cluster_id) else {
            return Ok(None);
        };

        let ids = delta
            .ids
            .iter()
            .zip(delta.versions.iter())
            .filter_map(|(id, version)| self.is_valid(*id, *version).then_some(*id))
            .collect::<Vec<_>>();
        self.load_raw(&ids).await?;

        Ok(Some(delta))
    }

    /// Compute distance between two vectors using the configured distance function.
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_function.distance(a, b)
    }

    /// Remove a cluster from both centroid indexes and register as tombstone.
    fn drop(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        self.raw_centroid
            .remove(cluster_id)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        self.quantized_centroid
            .remove(cluster_id)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        self.tombstones.insert(cluster_id);
        Ok(())
    }

    /// Insert a rotated vector into the index.
    async fn insert(&self, key: u32, vector: Arc<[f32]>) -> Result<(), QuantizedSpannError> {
        let version = self.upgrade(key);
        let candidates = self.navigate(&vector)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        if rng_cluster_ids.is_empty() {
            let code = Code::<Vec<u8>>::quantize(&vector, &vector).as_ref().into();
            let delta = QuantizedDelta {
                center: vector,
                codes: vec![code],
                ids: vec![key],
                length: 1,
                versions: vec![version],
            };
            self.create(delta)?;
        } else {
            let mut staging = Vec::new();
            for cluster_id in rng_cluster_ids {
                if let Some(centroid) = self.centroid(cluster_id) {
                    let code = Code::<Vec<u8>>::quantize(&vector, &centroid)
                        .as_ref()
                        .into();
                    if self
                        .append(cluster_id, key, version, code)
                        .is_some_and(|len| len > self.config.spann_split_threshold)
                    {
                        staging.push(cluster_id);
                    }
                }
            }

            for cluster_id in staging {
                Box::pin(self.balance(cluster_id)).await?;
            }
        }

        Ok(())
    }

    /// Check if a point is valid (version matches current version).
    fn is_valid(&self, id: u32, version: u32) -> bool {
        self.versions.get(&id).is_some_and(|v| *v == version)
    }

    /// Load cluster data from reader into deltas (reconciliation).
    async fn load(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

        if self
            .deltas
            .get(&cluster_id)
            .is_none_or(|d| d.ids.len() >= d.length as usize)
        {
            return Ok(());
        }

        let Some(persisted) = reader
            .get("", cluster_id)
            .await
            .map_err(QuantizedSpannError::Blockfile)?
        else {
            return Ok(());
        };

        let code_size = persisted.codes.len() / persisted.ids.len().max(1);
        if let Some(mut delta) = self.deltas.get_mut(&cluster_id) {
            if delta.ids.len() < delta.length as usize {
                for ((id, version), code) in persisted
                    .ids
                    .iter()
                    .zip(persisted.versions.iter())
                    .zip(persisted.codes.chunks(code_size))
                {
                    delta.codes.push(Arc::from(code));
                    delta.ids.push(*id as u32);
                    delta.versions.push(*version as u32);
                }
            }
        }

        Ok(())
    }

    /// Load raw embeddings for given ids into the embeddings cache.
    async fn load_raw(&self, ids: &[u32]) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.raw_embedding_reader else {
            return Ok(());
        };

        let missing_ids = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect::<Vec<_>>();

        reader
            .load_data_for_keys(missing_ids.iter().map(|id| (String::new(), *id)))
            .await;

        for id in missing_ids {
            if let Some(record) = reader
                .get("", id)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
            {
                self.embeddings.insert(id, self.rotate(record.embedding));
            }
        }

        Ok(())
    }

    /// Merge a small cluster into a nearby cluster.
    async fn merge(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(source_center) = self.centroid(cluster_id) else {
            return Ok(());
        };

        let neighbors = self.navigate(&source_center)?;
        let Some(target_id) = neighbors.keys.iter().copied().find(|id| *id != cluster_id) else {
            return Ok(());
        };

        let Some(target_center) = self.centroid(target_id) else {
            return Ok(());
        };

        let Some(source_delta) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        self.drop(cluster_id)?;
        for (id, version) in source_delta.ids.iter().zip(source_delta.versions.iter()) {
            let Some(embedding) = self.embeddings.get(id).map(|e| e.clone()) else {
                continue;
            };

            let dist_to_target = self.distance(&embedding, &target_center);
            let dist_to_source = self.distance(&embedding, &source_center);

            if dist_to_target <= dist_to_source {
                let code = Code::<Vec<u8>>::quantize(&embedding, &target_center)
                    .as_ref()
                    .into();
                self.append(target_id, *id, *version, code);
            } else {
                self.insert(*id, embedding).await?;
            }
        }

        Ok(())
    }

    /// Query the centroid index for the nearest cluster heads.
    fn navigate(&self, query: &[f32]) -> Result<SearchResult, QuantizedSpannError> {
        self.raw_centroid
            .search(query, self.config.spann_nprobe)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))
    }

    /// Apply epsilon and RNG filtering to navigate results.
    /// Returns up to `replica_count` cluster heads that pass both filters.
    fn rng_select(&self, candidates: &SearchResult) -> SearchResult {
        let first_distance = candidates.distances.first().copied().unwrap_or(0.0);
        let mut result = SearchResult::default();
        let mut selected_centroids = Vec::<Arc<_>>::with_capacity(self.config.spann_replica_count);

        for (cluster_id, distance) in candidates.keys.iter().zip(candidates.distances.iter()) {
            // Epsilon filter
            if (distance - first_distance).abs()
                > self.config.spann_rng_epsilon * first_distance.abs()
            {
                continue;
            }

            let Some(center) = self.centroid(*cluster_id) else {
                continue;
            };

            // RNG filter
            if selected_centroids.iter().any(|sel| {
                self.config.spann_rng_factor * self.distance(&center, sel).abs() <= distance.abs()
            }) {
                continue;
            }

            result.keys.push(*cluster_id);
            result.distances.push(*distance);
            selected_centroids.push(center);

            if result.keys.len() >= self.config.spann_replica_count {
                break;
            }
        }

        result
    }

    /// Normalize (if cosine) and rotate a vector for RaBitQ quantization.
    fn rotate(&self, vector: &[f32]) -> Arc<[f32]> {
        let rotated = match self.config.distance_function {
            DistanceFunction::Cosine => {
                let normalized = normalize(vector);
                &self.rotation * ColRef::from_slice(&normalized)
            }
            _ => &self.rotation * ColRef::from_slice(vector),
        };
        rotated.iter().copied().collect()
    }

    /// Scrub a cluster: load from reader, remove invalid entries, update length.
    /// Does NOT trigger split/merge - use balance() for that.
    /// Returns the new length after scrubbing, or None if cluster not found.
    async fn scrub(&self, cluster_id: u32) -> Result<Option<usize>, QuantizedSpannError> {
        self.load(cluster_id).await?;

        let new_len = if let Some(mut delta) = self.deltas.get_mut(&cluster_id) {
            // Scrub: keep only valid entries
            let mut i = 0;
            while i < delta.ids.len() {
                if self.is_valid(delta.ids[i], delta.versions[i]) {
                    i += 1;
                } else {
                    delta.codes.swap_remove(i);
                    delta.ids.swap_remove(i);
                    delta.versions.swap_remove(i);
                }
            }
            delta.length = delta.ids.len();
            Some(delta.length)
        } else {
            None
        };

        Ok(new_len)
    }

    /// Split a large cluster into two smaller clusters using 2-means clustering.
    /// TODO: Implement split logic with new delta structure.
    async fn split(&self, _cluster_id: u32) -> Result<(), QuantizedSpannError> {
        Ok(())
    }

    /// Increment and return the next version for a key.
    fn upgrade(&self, key: u32) -> u32 {
        let mut entry = self.versions.entry(key).or_default();
        *entry += 1;
        *entry
    }
}

impl MutableQuantizedSpannIndex<USearchIndex> {
    /// Open or create a quantized SPANN index.
    ///
    /// If centroid IDs are `None`, creates new centroid indexes.
    /// If centroid IDs are `Some(id)`, forks from the existing centroid index.
    /// Similarly, other blockfile IDs in config control create vs fork for each blockfile.
    pub async fn open(
        config: QuantizedSpannConfig,
        usearch_config: USearchIndexConfig,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        let dim = config.dimensions;

        // Step 1: Load embedding_metadata (rotation matrix + quantization center)
        let (rotation, center) = if let Some(id) = config.embedding_metadata_id {
            let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
            let reader = blockfile_provider
                .read::<u32, &'static [f32]>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Load rotation matrix columns
            let columns = reader
                .get_range(PREFIX_ROTATION..=PREFIX_ROTATION, ..)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .collect::<Vec<_>>();

            // Validate number of columns
            if columns.len() != dim {
                return Err(QuantizedSpannError::DimensionMismatch {
                    expected: dim,
                    got: columns.len(),
                });
            }

            // Validate each column length
            for (_prefix, _key, col) in &columns {
                if col.len() != dim {
                    return Err(QuantizedSpannError::DimensionMismatch {
                        expected: dim,
                        got: col.len(),
                    });
                }
            }

            // Construct rotation matrix column by column
            let rotation = Mat::from_fn(dim, dim, |i, j| columns[j].2[i]);

            // Load quantization center
            let center = reader
                .get(PREFIX_CENTER, 0)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .map(Arc::<[f32]>::from)
                .unwrap_or_else(|| vec![0.0; dim].into());

            (rotation, center)
        } else {
            // Sample new random rotation matrix
            let dist = UnitaryMat {
                dim,
                standard_normal: StandardNormal,
            };
            let rotation = dist.sample(&mut ThreadRng::default());
            (rotation, vec![0.0; dim].into())
        };

        // Step 2: Open centroid indexes
        let raw_centroid_mode = match config.raw_centroid_id {
            Some(id) => OpenMode::Fork(id),
            None => OpenMode::Create,
        };
        let raw_centroid = usearch_provider
            .open(&usearch_config, raw_centroid_mode)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        let quantized_centroid_mode = match config.quantized_centroid_id {
            Some(id) => OpenMode::Fork(id),
            None => OpenMode::Create,
        };
        let quantized_usearch_config = USearchIndexConfig {
            quantization_center: Some(center.clone()),
            ..usearch_config
        };
        let quantized_centroid = usearch_provider
            .open(&quantized_usearch_config, quantized_centroid_mode)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        // Step 3: Load scalar metadata (next_cluster_id, versions, cluster_lengths)
        let (next_cluster_id, versions, cluster_lengths) =
            if let Some(id) = config.scalar_metadata_id {
                let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
                let reader = blockfile_provider
                    .read::<u32, u32>(options)
                    .await
                    .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

                // Load cluster lengths
                let cluster_lengths: DashMap<u32, usize> = DashMap::new();
                for (_prefix, key, value) in reader
                    .get_range(PREFIX_LENGTH..=PREFIX_LENGTH, ..)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    cluster_lengths.insert(key, value as usize);
                }

                // Load next_cluster_id
                let next_cluster_id = reader
                    .get(PREFIX_NEXT_CLUSTER, 0)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                    .unwrap_or(0);

                // Load versions
                let versions: DashMap<u32, u32> = DashMap::new();
                for (_prefix, key, value) in reader
                    .get_range(PREFIX_VERSION..=PREFIX_VERSION, ..)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    versions.insert(key, value);
                }

                (next_cluster_id, versions, cluster_lengths)
            } else {
                (0u32, DashMap::new(), DashMap::new())
            };

        // Open quantized cluster reader if ID exists
        let quantized_cluster_reader = if let Some(id) = config.quantized_cluster_id {
            let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
            Some(
                blockfile_provider
                    .read(options)
                    .await
                    .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?,
            )
        } else {
            None
        };

        // Open raw embedding reader if ID exists
        let raw_embedding_reader = if let Some(id) = config.raw_embedding_id {
            let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
            Some(
                blockfile_provider
                    .read(options)
                    .await
                    .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?,
            )
        } else {
            None
        };

        // Step 4: Initialize deltas from cluster_lengths by getting centers from raw_centroid
        let deltas = DashMap::new();
        for entry in cluster_lengths.iter() {
            let cluster_id = *entry.key();
            let length = *entry.value();

            // Get center embedding from raw_centroid index
            if let Some(center_embedding) = raw_centroid
                .get(cluster_id)
                .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?
            {
                deltas.insert(
                    cluster_id,
                    QuantizedDelta {
                        center: center_embedding.into(),
                        codes: Vec::new(),
                        ids: Vec::new(),
                        length,
                        versions: Vec::new(),
                    },
                );
            }
        }

        Ok(Self {
            // === Config ===
            config,
            // === Centroid Index ===
            next_cluster_id: Arc::new(AtomicU32::new(next_cluster_id)),
            quantized_centroid,
            raw_centroid,
            // === Quantization ===
            center,
            rotation,
            // === In-Memory State ===
            deltas: deltas.into(),
            embeddings: DashMap::new().into(),
            tombstones: DashSet::new().into(),
            versions: versions.into(),
            // === Blockfile Readers ===
            quantized_cluster_reader,
            raw_embedding_reader,
        })
    }

    /// Commit all in-memory state to blockfile writers and return a flusher.
    ///
    /// This method consumes the index and prepares all data for persistence.
    /// Call `flush()` on the returned flusher to actually write to storage.
    pub async fn commit(
        self,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: USearchIndexProvider,
    ) -> Result<QuantizedSpannFlusher, QuantizedSpannError> {
        // === Step 1: quantized_cluster blockfile ===
        let quantized_cluster_flusher = {
            let mut cluster_ids = self
                .deltas
                .iter()
                .filter_map(|e| (!e.value().ids.is_empty()).then_some(*e.key()))
                .collect::<Vec<_>>();

            for cluster_id in &cluster_ids {
                self.scrub(*cluster_id).await?;
            }

            // Add deleted cluster ids
            for cluster_id in self.tombstones.iter() {
                cluster_ids.push(*cluster_id);
            }

            // Sort for ordered mutations
            cluster_ids.sort_unstable();

            // Create writer
            let mut options = BlockfileWriterOptions::new(self.config.prefix_path.clone());
            if let Some(id) = self.config.quantized_cluster_id {
                options = options.fork(id);
            }
            if let Some(cmek) = &self.config.cmek {
                options = options.with_cmek(cmek.clone());
            }
            let writer = blockfile_provider
                .write::<u32, QuantizedCluster<'_>>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Apply changes in order
            for cluster_id in cluster_ids {
                if let Some(delta) = self.deltas.get(&cluster_id) {
                    let codes: Vec<u8> =
                        delta.codes.iter().flat_map(|c| c.iter()).copied().collect();
                    let ids: Vec<u64> = delta.ids.iter().map(|&id| id as u64).collect();
                    let versions: Vec<u64> = delta.versions.iter().map(|&v| v as u64).collect();
                    let cluster_ref = QuantizedCluster {
                        center: &delta.center,
                        codes: &codes,
                        ids: &ids,
                        versions: &versions,
                    };
                    writer
                        .set("", cluster_id, cluster_ref)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                } else {
                    writer
                        .delete::<u32, QuantizedCluster<'_>>("", cluster_id)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            writer
                .commit::<u32, QuantizedCluster<'_>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 2: scalar_metadata blockfile ===
        // Stores: next_cluster_id, lengths, versions
        // Always create fresh, write in alphabetical prefix order: length < next < version
        // NOTE(sicheng): Must come after quantized_cluster because scrubbing may change lengths
        let scalar_metadata_flusher = {
            let mut options = BlockfileWriterOptions::new(self.config.prefix_path.clone());
            if let Some(cmek) = &self.config.cmek {
                options = options.with_cmek(cmek.clone());
            }
            let writer = blockfile_provider
                .write::<u32, u32>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // 1. PREFIX_LENGTH - sorted by cluster_id
            let mut lengths = self
                .deltas
                .iter()
                .map(|e| (*e.key(), e.value().length as u32))
                .collect::<Vec<_>>();
            lengths.sort_unstable();
            for (cluster_id, length) in lengths {
                writer
                    .set(PREFIX_LENGTH, cluster_id, length)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            // 2. PREFIX_NEXT_CLUSTER - single entry
            let next_id = self.next_cluster_id.load(Ordering::Relaxed);
            writer
                .set(PREFIX_NEXT_CLUSTER, 0u32, next_id)
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 3. PREFIX_VERSION - sorted by point_id
            let mut versions = self
                .versions
                .iter()
                .map(|e| (*e.key(), *e.value()))
                .collect::<Vec<_>>();
            versions.sort_unstable();
            for (point_id, version) in versions {
                writer
                    .set(PREFIX_VERSION, point_id, version)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            writer
                .commit::<u32, u32>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 3: embedding_metadata blockfile ===
        // Stores: quantization center and rotation matrix columns
        // Write in alphabetical prefix order: center < rotation
        let embedding_metadata_flusher = {
            let mut options = BlockfileWriterOptions::new(self.config.prefix_path.clone());
            if let Some(id) = self.config.embedding_metadata_id {
                options = options.fork(id);
            }
            if let Some(cmek) = &self.config.cmek {
                options = options.with_cmek(cmek.clone());
            }
            let writer = blockfile_provider
                .write::<u32, Vec<f32>>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // 1. PREFIX_CENTER - quantization center (always write, may be updated)
            writer
                .set(PREFIX_CENTER, 0u32, self.center.to_vec())
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 2. PREFIX_ROTATION - rotation matrix columns (only write for new indexes)
            if self.config.embedding_metadata_id.is_none() {
                let dim = self.config.dimensions;
                for col_idx in 0..dim {
                    let column: Vec<f32> =
                        (0..dim).map(|row| self.rotation[(row, col_idx)]).collect();
                    writer
                        .set(PREFIX_ROTATION, col_idx as u32, column)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            writer
                .commit::<u32, Vec<f32>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        Ok(QuantizedSpannFlusher {
            config: self.config,
            embedding_metadata_flusher,
            quantized_centroid: self.quantized_centroid,
            quantized_cluster_flusher,
            raw_centroid: self.raw_centroid,
            scalar_metadata_flusher,
            usearch_provider,
        })
    }
}

/// Flusher for persisting a quantized SPANN index to storage.
pub struct QuantizedSpannFlusher {
    config: QuantizedSpannConfig,
    embedding_metadata_flusher: BlockfileFlusher,
    quantized_centroid: USearchIndex,
    quantized_cluster_flusher: BlockfileFlusher,
    raw_centroid: USearchIndex,
    scalar_metadata_flusher: BlockfileFlusher,
    usearch_provider: USearchIndexProvider,
}

impl QuantizedSpannFlusher {
    /// Flush all data to storage and return updated config with IDs.
    pub async fn flush(self) -> Result<QuantizedSpannConfig, QuantizedSpannError> {
        // Get IDs before flushing
        let embedding_metadata_id = self.embedding_metadata_flusher.id();
        let quantized_cluster_id = self.quantized_cluster_flusher.id();
        let scalar_metadata_id = self.scalar_metadata_flusher.id();

        // Flush blockfiles
        self.embedding_metadata_flusher
            .flush::<u32, Vec<f32>>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.quantized_cluster_flusher
            .flush::<u32, QuantizedCluster<'_>>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.scalar_metadata_flusher
            .flush::<u32, u32>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;

        // Flush centroid indexes
        let quantized_centroid_id = self
            .usearch_provider
            .flush(&self.quantized_centroid)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        let raw_centroid_id = self
            .usearch_provider
            .flush(&self.raw_centroid)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        // Return updated config with all IDs
        Ok(QuantizedSpannConfig {
            embedding_metadata_id: Some(embedding_metadata_id),
            quantized_centroid_id: Some(quantized_centroid_id),
            quantized_cluster_id: Some(quantized_cluster_id),
            raw_centroid_id: Some(raw_centroid_id),
            scalar_metadata_id: Some(scalar_metadata_id),
            ..self.config
        })
    }
}
