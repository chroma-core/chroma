use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use chroma_blockstore::BlockfileReader;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Cmek, DataRecord, QuantizedCluster, QuantizedClusterOwned};
use dashmap::{DashMap, DashSet};
use faer::{col::ColRef, Mat};
use rand::seq::SliceRandom;
use thiserror::Error;

use crate::{quantization::Code, SearchResult, VectorIndex};

use super::utils::{cluster, query_quantized_cluster, KMeansAlgorithmInput};

// K-means parameters for split. Since split threshold is typically < 1024,
// sampling all points is fine. Lambda regularizes cluster sizes for balance.
const KMEANS_NUM_SAMPLES: usize = 1024;
const KMEANS_INITIAL_LAMBDA: f32 = 100.0;

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
    deleted_clusters: Arc<DashSet<u64>>,
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

    /// Remove a cluster from deltas and load raw embeddings for its valid points.
    async fn detach(
        &self,
        cluster_id: u64,
    ) -> Result<Option<QuantizedClusterOwned>, QuantizedSpannError> {
        let Some((_, delta)) = self.deltas.remove(&cluster_id) else {
            return Ok(None);
        };

        if let Some(reader) = &self.raw_embedding_reader {
            for (id, version) in delta.cluster.ids.iter().zip(delta.cluster.versions.iter()) {
                if self.is_valid(*id, *version) && !self.embeddings.contains_key(id) {
                    if let Some(record) = reader.get("", *id as u32).await? {
                        self.embeddings.insert(*id, Arc::from(record.embedding));
                    }
                }
            }
        }

        Ok(Some(delta.cluster))
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
            let mut cluster = QuantizedClusterOwned::new(vector.clone());
            let code = Code::<Vec<u8>>::quantize(&vector, &vector);
            cluster.append(key, version, code.as_ref());
            self.create(cluster)?;
        } else {
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

            for cluster_id in staging {
                Box::pin(self.scrub(cluster_id)).await?;
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
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

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

        let code_size = persisted.codes.len() / persisted.ids.len().max(1);
        if let Some(mut delta) = self.deltas.get_mut(&cluster_id) {
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

    /// Merge a small cluster into a nearby cluster.
    async fn merge(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        let Some(source_center) = self.cluster_centroid(cluster_id) else {
            return Ok(());
        };

        let neighbors = self.navigate(&source_center)?;
        let Some(&target_id) = neighbors.keys.iter().find(|&&id| id != cluster_id) else {
            return Ok(());
        };

        let Some(target_center) = self.cluster_centroid(target_id) else {
            return Ok(());
        };

        let Some(source_cluster) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        self.centroid
            .remove(cluster_id)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        self.deleted_clusters.insert(cluster_id);

        let source_borrowed = QuantizedCluster::from(&source_cluster);
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

        for (((id, version), dist_to_target), dist_to_source) in source_borrowed
            .ids
            .iter()
            .zip(source_borrowed.versions.iter())
            .zip(dists_to_target.distances.iter())
            .zip(dists_to_source.distances.iter())
        {
            let Some(embedding) = self.embeddings.get(id).map(|e| e.clone()) else {
                continue;
            };

            if dist_to_target <= dist_to_source {
                let code = Code::<Vec<u8>>::quantize(&embedding, &target_center);
                self.append(target_id, *id, *version, code.as_ref());
            } else {
                self.insert(*id, embedding).await?;
            }
        }

        if self
            .deltas
            .get(&target_id)
            .is_some_and(|d| d.length > self.config.spann_split_threshold)
        {
            Box::pin(self.scrub(target_id)).await?;
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
            // Epsilon filter
            if (distance - first_distance).abs()
                > self.config.spann_rng_epsilon * first_distance.abs()
            {
                continue;
            }

            let Some(centroid) = self.cluster_centroid(*cluster_id) else {
                continue;
            };

            // RNG filter
            if selected_centroids.iter().any(|sel| {
                self.config.spann_rng_factor * self.distance(&centroid, sel).abs() <= distance.abs()
            }) {
                continue;
            }

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

    /// Scrub a cluster: load from reader, remove invalid entries, trigger split/merge if needed.
    async fn scrub(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        self.load(cluster_id).await?;

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

        if new_len > self.config.spann_split_threshold {
            self.split(cluster_id).await?;
        } else if new_len > 0 && new_len < self.config.spann_merge_threshold {
            self.merge(cluster_id).await?;
        }

        Ok(())
    }

    /// Split a large cluster into two smaller clusters using 2-means clustering.
    async fn split(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        let Some(old_center) = self.cluster_centroid(cluster_id) else {
            return Ok(());
        };

        let Some(source_cluster) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        // NOTE(sicheng): The split logic, including the same head optimization, are borrowed from
        // legacy spann implementation. This seems unnecessarily complex in the sense that there
        // should be cleaner and more efficient code to achieve the same effect, and we should
        // revisit this in the future.

        // Collect valid points with their embeddings from cache.
        let mut valid_points = Vec::new();
        for (id, version) in source_cluster
            .ids
            .iter()
            .zip(source_cluster.versions.iter())
        {
            if !self.is_valid(*id, *version) {
                continue;
            }
            let Some(embedding) = self.embeddings.get(id).map(|e| e.clone()) else {
                continue;
            };
            valid_points.push((*id, *version, embedding));
        }

        if valid_points.len() < 2 {
            self.deltas.insert(
                cluster_id,
                QuantizedDelta {
                    cluster: source_cluster,
                    length: valid_points.len(),
                },
            );
            return Ok(());
        }

        let embeddings: Vec<Arc<[f32]>> = valid_points.iter().map(|(_, _, e)| e.clone()).collect();
        let mut indices: Vec<usize> = (0..valid_points.len()).collect();
        indices.shuffle(&mut rand::thread_rng());
        let mut kmeans_input = KMeansAlgorithmInput::new(
            indices,
            &embeddings,
            self.config.dimensions,
            2,
            0,
            valid_points.len(),
            KMEANS_NUM_SAMPLES,
            self.config.distance_function.clone(),
            KMEANS_INITIAL_LAMBDA,
        );

        let clustering_output = match cluster(&mut kmeans_input) {
            Ok(output) => output,
            Err(_) => {
                self.deltas.insert(
                    cluster_id,
                    QuantizedDelta {
                        cluster: source_cluster,
                        length: valid_points.len(),
                    },
                );
                return Ok(());
            }
        };

        if clustering_output.num_clusters <= 1 || clustering_output.cluster_counts.contains(&0) {
            self.deltas.insert(
                cluster_id,
                QuantizedDelta {
                    cluster: source_cluster,
                    length: valid_points.len(),
                },
            );
            return Ok(());
        }

        // Reuse cluster_id if one centroid is very close to the old center.
        const SAME_HEAD_THRESHOLD: f32 = 1e-6;
        let dist0 = self.distance(&old_center, &clustering_output.cluster_centers[0]);
        let dist1 = self.distance(&old_center, &clustering_output.cluster_centers[1]);
        let same_head_cluster = if dist0 < SAME_HEAD_THRESHOLD && dist0 <= dist1 {
            Some(0)
        } else if dist1 < SAME_HEAD_THRESHOLD {
            Some(1)
        } else {
            None
        };

        for k in 0..2 {
            let new_cluster_id = if same_head_cluster == Some(k) {
                cluster_id
            } else {
                self.next_cluster_id.fetch_add(1, Ordering::Relaxed)
            };

            let centroid = &clustering_output.cluster_centers[k];
            let mut new_cluster = QuantizedClusterOwned::new(centroid.clone());

            for (idx, &label) in &clustering_output.cluster_labels {
                if label as usize != k {
                    continue;
                }
                let (id, version, embedding) = &valid_points[*idx];
                let code = Code::<Vec<u8>>::quantize(embedding, centroid);
                new_cluster.append(*id, *version, code.as_ref());
            }

            let length = new_cluster.ids.len();
            self.deltas.insert(
                new_cluster_id,
                QuantizedDelta {
                    cluster: new_cluster,
                    length,
                },
            );

            if same_head_cluster != Some(k) {
                self.centroid
                    .add(new_cluster_id, centroid)
                    .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
            }
        }

        if same_head_cluster.is_none() {
            self.centroid
                .remove(cluster_id)
                .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
            self.deleted_clusters.insert(cluster_id);
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
