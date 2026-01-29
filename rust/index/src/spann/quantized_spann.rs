use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use chroma_blockstore::BlockfileReader;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Cmek, DataRecord, QuantizedCluster, QuantizedClusterOwned};
use dashmap::DashMap;
use faer::{col::ColRef, Mat};
use thiserror::Error;

use crate::{quantization::Code, SearchResult, VectorIndex};

use super::utils::query_quantized_cluster;

/// In-memory staging for a quantized cluster head.
struct QuantizedDelta {
    cluster: QuantizedClusterOwned,
    length: usize,
}

/// Configuration for quantized SPANN index.
#[derive(Clone)]
struct QuantizedSpannConfig {
    // === Shared ===
    cmek: Option<Cmek>,
    prefix_path: String,
    dimensions: usize,
    distance_function: DistanceFunction,

    // === Rebuild Criteria ===
    drift_threshold: f32,

    // === SPANN ===
    spann_nprobe: usize,
    spann_replica_count: usize,
    spann_rng_epsilon: f32,
    spann_rng_factor: f32,
    spann_split_threshold: usize,
    spann_merge_threshold: usize,

    // === USearch ===
    usearch_connectivity: usize,
    usearch_expansion_add: usize,
    usearch_expansion_search: usize,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannError {
    #[error("Get not supported for quantized SPANN index")]
    GetNotSupported,
    #[error("Centroid index error: {0}")]
    CentroidIndex(#[from] Box<dyn ChromaError>),
    #[error("Blockfile error: {0}")]
    Blockfile(#[from] chroma_blockstore::BlockfileError),
}

impl ChromaError for QuantizedSpannError {
    fn code(&self) -> ErrorCodes {
        match self {
            QuantizedSpannError::GetNotSupported => ErrorCodes::InvalidArgument,
            QuantizedSpannError::CentroidIndex(err) => err.code(),
            QuantizedSpannError::Blockfile(err) => err.code(),
        }
    }
}

/// Mutable quantized SPANN index, generic over centroid index.
pub struct MutableQuantizedSpannIndex<I: VectorIndex> {
    // === Config ===
    config: QuantizedSpannConfig,

    // === Centroid Index ===
    centroid: I,
    next_cluster_id: Arc<AtomicU64>,

    // === Quantization ===
    rotation: Mat<f32>,

    // === In-Memory State ===
    deltas: Arc<DashMap<u64, QuantizedDelta>>,
    embeddings: Arc<DashMap<u64, Arc<[f32]>>>,
    versions: Arc<DashMap<u64, u64>>,

    // === Blockfile Readers ===
    quantized_cluster_reader: Option<BlockfileReader<'static, u64, QuantizedCluster<'static>>>,
    // NOTE: This is the record segment's id_to_data blockfile reader.
    // This is a temporary solution for loading raw embeddings; a dedicated
    // raw embedding store may be introduced in the future.
    raw_embedding_reader: Option<BlockfileReader<'static, u32, DataRecord<'static>>>,
}

impl<I: VectorIndex> MutableQuantizedSpannIndex<I> {
    pub async fn add(&self, key: u64, vector: &[f32]) -> Result<(), QuantizedSpannError> {
        let rotated = self.rotate(vector);
        self.embeddings.insert(key, rotated.clone());
        self.insert(key, rotated).await
    }

    pub fn remove(&self, key: u64) {
        self.upgrade(key);
    }

    pub async fn search(
        &self,
        _query: &[f32],
        _count: usize,
    ) -> Result<SearchResult, QuantizedSpannError> {
        todo!()
    }
}

impl<I: VectorIndex> MutableQuantizedSpannIndex<I> {
    /// Append a point to an existing cluster. Returns new length, or None if cluster not found.
    fn append(&self, cluster_id: u64, id: u64, version: u64, code: &[u8]) -> Option<usize> {
        let mut delta = self.deltas.get_mut(&cluster_id)?;
        delta.cluster.append(id, version, code);
        delta.length += 1;
        Some(delta.length)
    }

    /// Get the centroid for a cluster, cloning to release the lock.
    fn cluster_centroid(&self, cluster_id: u64) -> Option<Arc<[f32]>> {
        self.deltas
            .get(&cluster_id)
            .map(|d| d.cluster.center.clone())
    }

    /// Create a new cluster and register it in the centroid index.
    fn create(&self, cluster: QuantizedClusterOwned) -> Result<(), QuantizedSpannError> {
        let id = self.next_cluster_id.fetch_add(1, Ordering::Relaxed);
        let length = cluster.ids.len();
        let center = cluster.center.clone();
        self.deltas.insert(id, QuantizedDelta { cluster, length });
        self.centroid
            .add(id, &center)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        Ok(())
    }

    /// Compute distance between two vectors using the configured distance function.
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_function.distance(a, b)
    }

    /// Insert a rotated vector into the index.
    async fn insert(&self, key: u64, vector: Arc<[f32]>) -> Result<(), QuantizedSpannError> {
        let version = self.upgrade(key);
        let candidates = self.navigate(&vector)?;
        let rng_clusters = self.rng_select(&candidates);

        if rng_clusters.keys.is_empty() {
            // No suitable clusters found - create a new one with this point as centroid
            let mut cluster = QuantizedClusterOwned::new(vector.clone());
            let code = Code::<Vec<u8>>::quantize(&vector, &vector);
            cluster.append(key, version, code.as_ref());
            self.create(cluster)?;
        } else {
            // Append to each selected cluster, collect clusters that need scrubbing
            let mut staging = Vec::new();
            for &cluster_id in &rng_clusters.keys {
                if let Some(centroid) = self.cluster_centroid(cluster_id) {
                    let code = Code::<Vec<u8>>::quantize(&vector, &centroid);
                    if self
                        .append(cluster_id, key, version, code.as_ref())
                        .is_some_and(|len| len > self.config.spann_split_threshold)
                    {
                        staging.push(cluster_id);
                    }
                }
            }

            // Scrub staging collections
            for cluster_id in staging {
                self.scrub(cluster_id).await?;
            }
        }

        Ok(())
    }

    /// Check if a point is valid (version matches current version).
    fn is_valid(&self, id: u64, version: u64) -> bool {
        self.versions.get(&id).is_some_and(|v| *v == version)
    }

    /// Load cluster data from reader into deltas (reconciliation).
    async fn load(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        // Load from reader if available
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

        // Check if reconciliation is needed (length tracks total, ids.len() is in-memory)
        if self
            .deltas
            .get(&cluster_id)
            .is_none_or(|d| d.cluster.ids.len() >= d.length)
        {
            return Ok(());
        }

        let Some(persisted) = reader.get("", cluster_id).await? else {
            return Ok(());
        };

        // Extend delta with persisted data
        let code_size = persisted.codes.len() / persisted.ids.len().max(1);
        if let Some(mut delta) = self.deltas.get_mut(&cluster_id) {
            // Only extend if we haven't already
            if delta.cluster.ids.len() < delta.length {
                for ((id, version), code) in persisted
                    .ids
                    .iter()
                    .zip(persisted.versions.iter())
                    .zip(persisted.codes.chunks(code_size))
                {
                    delta.cluster.append(*id, *version, code);
                }
            }
        }

        Ok(())
    }

    /// Load raw embeddings for all points in a cluster into the embeddings cache.
    async fn load_raw(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.raw_embedding_reader else {
            return Ok(());
        };

        // Get ids from the cluster (only valid, non-cached entries)
        let ids = {
            let Some(delta) = self.deltas.get(&cluster_id) else {
                return Ok(());
            };
            delta
                .cluster
                .ids
                .iter()
                .zip(delta.cluster.versions.iter())
                .filter(|(id, version)| {
                    self.is_valid(**id, **version) && !self.embeddings.contains_key(id)
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>()
        };

        // Load embeddings for each id not already cached
        for id in ids {
            if let Some(data_record) = reader.get("", id as u32).await? {
                let embedding = Arc::from(data_record.embedding);
                self.embeddings.insert(id, embedding);
            }
        }

        Ok(())
    }

    /// Query the centroid index for the nearest cluster heads.
    fn navigate(&self, query: &[f32]) -> Result<SearchResult, QuantizedSpannError> {
        self.centroid
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
            // Epsilon filter: skip if relative deviation exceeds epsilon
            if (distance - first_distance).abs()
                > self.config.spann_rng_epsilon * first_distance.abs()
            {
                continue;
            }

            // Skip deleted heads (not in deltas)
            let Some(centroid) = self.cluster_centroid(*cluster_id) else {
                continue;
            };

            // RNG filter: skip if any selected centroid is closer to candidate than query
            if selected_centroids.iter().any(|sel| {
                self.config.spann_rng_factor * self.distance(&centroid, sel).abs() <= distance.abs()
            }) {
                continue;
            }

            // Accept
            result.keys.push(*cluster_id);
            result.distances.push(*distance);
            selected_centroids.push(centroid);

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

    /// Merge a small cluster into a nearby cluster.
    async fn merge(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        // Load raw embeddings for points in the cluster
        self.load_raw(cluster_id).await?;

        // Get source centroid
        let Some(source_center) = self.cluster_centroid(cluster_id) else {
            return Ok(());
        };

        // Find nearest neighbor (excluding self)
        let neighbors = self.navigate(&source_center)?;
        let Some(&target_id) = neighbors.keys.iter().find(|&&id| id != cluster_id) else {
            return Ok(());
        };

        // Get target centroid
        let Some(target_center) = self.cluster_centroid(target_id) else {
            return Ok(());
        };

        // Remove source from deltas (take ownership)
        let Some((_, source_delta)) = self.deltas.remove(&cluster_id) else {
            return Ok(());
        };

        // Remove source from centroid index
        self.centroid
            .remove(cluster_id)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        // Query source cluster against both centers to get estimated distances
        let source_borrowed: QuantizedCluster<'_> = (&source_delta.cluster).into();
        let dists_to_target = query_quantized_cluster(
            &source_borrowed,
            &target_center,
            &self.config.distance_function,
        );
        let dists_to_source = query_quantized_cluster(
            &source_borrowed,
            &source_center,
            &self.config.distance_function,
        );

        // For each point with valid embedding, decide: append to target or re-insert
        for (((id, version), dist_to_target), dist_to_source) in source_borrowed
            .ids
            .iter()
            .zip(source_borrowed.versions.iter())
            .zip(dists_to_target.distances.iter())
            .zip(dists_to_source.distances.iter())
        {
            // Embedding is required (loaded by load_raw)
            let Some(embedding) = self.embeddings.get(id).map(|e| e.clone()) else {
                continue;
            };

            if dist_to_target <= dist_to_source {
                // Closer to target: re-quantize and append with same version from source
                let code = Code::<Vec<u8>>::quantize(&embedding, &target_center);
                self.append(target_id, *id, *version, code.as_ref());
            } else {
                // Closer to source (which is gone): re-insert
                Box::pin(self.insert(*id, embedding)).await?;
            }
        }

        // Scrub target if over threshold
        if self
            .deltas
            .get(&target_id)
            .is_some_and(|d| d.length > self.config.spann_split_threshold)
        {
            Box::pin(self.scrub(target_id)).await?;
        }

        Ok(())
    }

    /// Scrub a cluster: load from reader, remove invalid entries, trigger split/merge if needed.
    async fn scrub(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        // Load from reader
        self.load(cluster_id).await?;

        // Get mutable ref to delta and scrub
        let new_len = {
            let Some(mut delta) = self.deltas.get_mut(&cluster_id) else {
                return Ok(());
            };

            let new_len = delta
                .cluster
                .scrub(|id, version| self.is_valid(id, version));
            delta.length = new_len;
            new_len
        };

        // Check thresholds for split/merge
        if new_len > self.config.spann_split_threshold {
            // TODO: self.split(cluster_id).await?;
        } else if new_len > 0 && new_len < self.config.spann_merge_threshold {
            self.merge(cluster_id).await?;
        }

        Ok(())
    }

    /// Increment and return the next version for a key.
    fn upgrade(&self, key: u64) -> u64 {
        let mut entry = self.versions.entry(key).or_default();
        *entry += 1;
        *entry
    }
}
