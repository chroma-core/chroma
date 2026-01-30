use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, BlockfileFlusher, BlockfileReader,
    BlockfileWriterOptions,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Cmek, DataRecord, QuantizedCluster, QuantizedClusterOwned};
use dashmap::{DashMap, DashSet};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, SliceRandom, StandardNormal, ThreadRng},
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

use super::utils::{cluster, query_quantized_cluster, KMeansAlgorithmInput};

// K-means parameters for split. Since split threshold is typically < 1024,
// sampling all points is fine. Lambda regularizes cluster sizes for balance.
const KMEANS_NUM_SAMPLES: usize = 1024;
const KMEANS_INITIAL_LAMBDA: f32 = 100.0;

// Blockfile prefix for rotation matrix columns in embedding_metadata blockfile.
const ROTATION_PREFIX: &str = "rotation";

// Blockfile prefixes for scalar_metadata blockfile.
const CENTER_PREFIX: &str = "center";
const LENGTH_PREFIX: &str = "length";
const NEXT_CLUSTER_PREFIX: &str = "next";
const VERSION_PREFIX: &str = "version";

/// In-memory staging for a quantized cluster head.
struct QuantizedDelta {
    center_vector_id: u64,
    length: usize,
    cluster: QuantizedClusterOwned,
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
    pub quantized_cluster_id: Option<Uuid>,
    pub raw_embedding_id: Option<Uuid>,
    pub embedding_metadata_id: Option<Uuid>,
    pub scalar_metadata_id: Option<Uuid>,
    pub centroid_id: Option<IndexUuid>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannError {
    #[error("Get not supported for quantized SPANN index")]
    GetNotSupported,
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
            QuantizedSpannError::GetNotSupported => ErrorCodes::InvalidArgument,
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
    centroid: I,
    next_cluster_id: Arc<AtomicU64>,

    // === Quantization ===
    rotation: Mat<f32>,

    // === In-Memory State ===
    deltas: Arc<DashMap<u64, QuantizedDelta>>,
    embeddings: Arc<DashMap<u64, Arc<[f32]>>>,
    tombstones: Arc<DashSet<u64>>,
    versions: Arc<DashMap<u64, u64>>,

    // === Blockfile Readers ===
    quantized_cluster_reader: Option<BlockfileReader<'static, u64, QuantizedCluster<'static>>>,
    // NOTE(sicheng): This is the record segment's id_to_data blockfile reader.
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

    /// Balance a cluster: scrub then trigger split/merge if needed.
    async fn balance(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
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
    fn cluster_centroid(&self, cluster_id: u64) -> Option<Arc<[f32]>> {
        self.deltas
            .get(&cluster_id)
            .map(|d| d.cluster.center.clone())
    }

    /// Create a new cluster and register it in the centroid index.
    fn create(
        &self,
        cluster: QuantizedClusterOwned,
        center_vector_id: u64,
    ) -> Result<(), QuantizedSpannError> {
        let id = self.next_cluster_id.fetch_add(1, Ordering::Relaxed);
        let length = cluster.ids.len();
        let center = cluster.center.clone();
        self.deltas.insert(
            id,
            QuantizedDelta {
                center_vector_id,
                length,
                cluster,
            },
        );
        self.centroid
            .add(id, &center)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        Ok(())
    }

    /// Remove a cluster from deltas and load raw embeddings for its valid points.
    /// Returns (cluster, center_vector_id) if the cluster existed.
    async fn detach(
        &self,
        cluster_id: u64,
    ) -> Result<Option<(QuantizedClusterOwned, u64)>, QuantizedSpannError> {
        let Some((_, delta)) = self.deltas.remove(&cluster_id) else {
            return Ok(None);
        };

        let ids = delta
            .cluster
            .ids
            .iter()
            .zip(delta.cluster.versions.iter())
            .filter_map(|(id, version)| self.is_valid(*id, *version).then_some(*id))
            .collect::<Vec<_>>();
        self.load_raw(&ids).await?;

        Ok(Some((delta.cluster, delta.center_vector_id)))
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
            self.create(cluster, key)?;
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
                Box::pin(self.balance(cluster_id)).await?;
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

        let Some(persisted) = reader
            .get("", cluster_id)
            .await
            .map_err(QuantizedSpannError::Blockfile)?
        else {
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

    /// Load raw embeddings for given ids into the embeddings cache.
    async fn load_raw(&self, ids: &[u64]) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.raw_embedding_reader else {
            return Ok(());
        };

        let missing_ids = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect::<Vec<_>>();

        reader
            .load_data_for_keys(missing_ids.iter().map(|id| (String::new(), *id as u32)))
            .await;

        for id in missing_ids {
            if let Some(record) = reader
                .get("", id as u32)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
            {
                self.embeddings.insert(id, Arc::from(record.embedding));
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

        let Some((source_cluster, _)) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        self.centroid
            .remove(cluster_id)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        self.tombstones.insert(cluster_id);

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
            Box::pin(self.balance(target_id)).await?;
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

    /// Scrub a cluster: load from reader, remove invalid entries, update length.
    /// Does NOT trigger split/merge - use balance() for that.
    /// Returns the new length after scrubbing, or None if cluster not found.
    async fn scrub(&self, cluster_id: u64) -> Result<Option<usize>, QuantizedSpannError> {
        self.load(cluster_id).await?;

        let new_len = if let Some(mut delta) = self.deltas.get_mut(&cluster_id) {
            let new_len = delta
                .cluster
                .scrub(|id, version| self.is_valid(id, version));
            delta.length = new_len;
            Some(new_len)
        } else {
            None
        };

        Ok(new_len)
    }

    /// Split a large cluster into two smaller clusters using 2-means clustering.
    async fn split(&self, cluster_id: u64) -> Result<(), QuantizedSpannError> {
        let Some(old_center) = self.cluster_centroid(cluster_id) else {
            return Ok(());
        };

        let Some((source_cluster, old_center_vector_id)) = self.detach(cluster_id).await? else {
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
                    center_vector_id: old_center_vector_id,
                    length: valid_points.len(),
                    cluster: source_cluster,
                },
            );
            return Ok(());
        }

        let embeddings = valid_points.iter().map(|(_, _, e)| e.clone()).collect();
        let mut indices = (0..valid_points.len()).collect::<Vec<_>>();
        indices.shuffle(&mut ThreadRng::default());
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
                        center_vector_id: old_center_vector_id,
                        length: valid_points.len(),
                        cluster: source_cluster,
                    },
                );
                return Ok(());
            }
        };

        if clustering_output.num_clusters <= 1 || clustering_output.cluster_counts.contains(&0) {
            self.deltas.insert(
                cluster_id,
                QuantizedDelta {
                    center_vector_id: old_center_vector_id,
                    length: valid_points.len(),
                    cluster: source_cluster,
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

        let mut new_cluster_ids = [0u64; 2];
        let new_centers = &clustering_output.cluster_centers;

        for k in 0..2 {
            let new_cluster_id = if same_head_cluster == Some(k) {
                cluster_id
            } else {
                self.next_cluster_id.fetch_add(1, Ordering::Relaxed)
            };
            new_cluster_ids[k] = new_cluster_id;

            let centroid = &new_centers[k];
            let mut new_cluster = QuantizedClusterOwned::new(centroid.clone());

            for (idx, &label) in &clustering_output.cluster_labels {
                if label as usize != k {
                    continue;
                }
                let (id, version, embedding) = &valid_points[*idx];
                let code = Code::<Vec<u8>>::quantize(embedding, centroid);
                new_cluster.append(*id, *version, code.as_ref());
            }

            // Get center_vector_id from k-means output (index into valid_points)
            let center_vector_id =
                valid_points[clustering_output.cluster_center_vector_ids[k] as usize].0;

            let length = new_cluster.ids.len();
            self.deltas.insert(
                new_cluster_id,
                QuantizedDelta {
                    center_vector_id,
                    length,
                    cluster: new_cluster,
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
            self.tombstones.insert(cluster_id);
        }

        // Dedup set for examined IDs
        let examined = DashSet::new();
        // Candidates for reassignment: (id, version, prev_cluster_id, embedding)
        let mut reassignable = Vec::new();

        // Examine points in the new split clusters first
        for new_id in new_cluster_ids {
            let (points, new_center) = {
                let Some(delta) = self.deltas.get(&new_id) else {
                    continue;
                };
                let points = delta
                    .cluster
                    .ids
                    .iter()
                    .zip(delta.cluster.versions.iter())
                    .filter_map(|(id, version)| {
                        self.is_valid(*id, *version).then_some((*id, *version))
                    })
                    .collect::<Vec<_>>();
                (points, delta.cluster.center.clone())
            };

            for (id, version) in points {
                if !examined.insert(id) {
                    continue;
                }
                let Some(embedding) = self.embeddings.get(&id).map(|e| e.clone()) else {
                    continue;
                };
                // Check if point prefers old center over its new assigned center
                let dist_to_old = self.distance(&embedding, &old_center);
                let dist_to_new = self.distance(&embedding, &new_center);
                if dist_to_old < dist_to_new {
                    reassignable.push((id, version, new_id, embedding));
                }
            }
        }

        // Find nearby clusters and examine their points
        let neighbors = self.navigate(&old_center)?;
        // Collect neighbor points: (id, version, neighbor_id, neighbor_center)
        let mut neighbor_points = Vec::new();
        for neighbor_id in neighbors.keys {
            // Skip the new clusters and the original cluster
            if new_cluster_ids.contains(&neighbor_id) || neighbor_id == cluster_id {
                continue;
            }
            self.scrub(neighbor_id).await?;

            let (points, neighbor_center) = {
                let Some(delta) = self.deltas.get(&neighbor_id) else {
                    continue;
                };
                let points = delta
                    .cluster
                    .ids
                    .iter()
                    .zip(delta.cluster.versions.iter())
                    .filter_map(|(id, version)| {
                        self.is_valid(*id, *version).then_some((*id, *version))
                    })
                    .collect::<Vec<_>>();
                (points, delta.cluster.center.clone())
            };

            for (id, version) in points {
                if !examined.insert(id) {
                    continue;
                }
                neighbor_points.push((id, version, neighbor_id, neighbor_center.clone()));
            }
        }

        // Load raw embeddings for neighbor points
        let neighbor_ids = neighbor_points
            .iter()
            .map(|(id, _, _, _)| *id)
            .collect::<Vec<_>>();
        self.load_raw(&neighbor_ids).await?;

        // Check each neighbor point
        for (id, version, neighbor_id, neighbor_center) in neighbor_points {
            let Some(embedding) = self.embeddings.get(&id).map(|e| e.clone()) else {
                continue;
            };
            let dist_to_current = self.distance(&embedding, &neighbor_center);
            let dist_to_new_0 = self.distance(&embedding, &new_centers[0]);
            let dist_to_new_1 = self.distance(&embedding, &new_centers[1]);

            // Skip if point is closer to current center than BOTH new centers
            if dist_to_current <= dist_to_new_0 && dist_to_current <= dist_to_new_1 {
                continue;
            }
            // Skip if point is closer to old center than BOTH new centers
            let dist_to_old = self.distance(&embedding, &old_center);
            if dist_to_old <= dist_to_new_0 && dist_to_old <= dist_to_new_1 {
                continue;
            }
            reassignable.push((id, version, neighbor_id, embedding));
        }

        // Reassign points
        for (id, version, prev_cluster_id, embedding) in reassignable {
            // Check version is still valid before reassigning
            if !self.is_valid(id, version) {
                continue;
            }

            let candidates = self.navigate(&embedding)?;
            let rng_clusters = self.rng_select(&candidates);

            // Skip if previous cluster is in RNG results (point is already well-placed)
            if rng_clusters.keys.contains(&prev_cluster_id) {
                continue;
            }

            // Bump version and append to selected clusters (no balance)
            let new_version = self.upgrade(id);
            for cluster_id in rng_clusters.keys {
                if let Some(centroid) = self.cluster_centroid(cluster_id) {
                    let code = Code::<Vec<u8>>::quantize(&embedding, &centroid);
                    self.append(cluster_id, id, new_version, code.as_ref());
                }
            }
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

impl MutableQuantizedSpannIndex<USearchIndex> {
    /// Open or create a quantized SPANN index.
    ///
    /// If `config.centroid_id` is `None`, creates a new centroid index.
    /// If `config.centroid_id` is `Some(id)`, forks from the existing centroid index.
    /// Similarly, other blockfile IDs in config control create vs fork for each blockfile.
    pub async fn open(
        config: QuantizedSpannConfig,
        usearch_config: USearchIndexConfig,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        // Open embedding_metadata reader if ID exists, load or sample rotation matrix
        let rotation = if let Some(id) = config.embedding_metadata_id {
            let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
            let reader = blockfile_provider
                .read::<u64, &'static [f32]>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Read all columns with prefix ROTATION_PREFIX (ordered by key)
            let columns = reader
                .get_range(ROTATION_PREFIX..=ROTATION_PREFIX, ..)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .collect::<Vec<_>>();

            let dim = config.dimensions;

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

            // Construct matrix column by column
            Mat::from_fn(dim, dim, |i, j| columns[j].2[i])
        } else {
            // Sample new random rotation matrix
            let dim = config.dimensions;
            let dist = UnitaryMat {
                dim,
                standard_normal: StandardNormal,
            };
            dist.sample(&mut ThreadRng::default())
        };

        // Load scalar metadata (next_cluster_id, versions, cluster_metadata) if ID exists
        // cluster_metadata maps cluster_id -> (center_vector_id, length)
        let (next_cluster_id, versions, cluster_metadata) =
            if let Some(id) = config.scalar_metadata_id {
                let options = BlockfileReaderOptions::new(id, config.prefix_path.clone());
                let reader = blockfile_provider
                    .read::<u64, u64>(options)
                    .await
                    .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

                // Load next_cluster_id
                let next_cluster_id = reader
                    .get(NEXT_CLUSTER_PREFIX, 0)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                    .unwrap_or(0);

                // Load cluster metadata: first center_vector_ids, then lengths
                let cluster_metadata: DashMap<u64, (u64, usize)> = DashMap::new();
                for (_prefix, key, value) in reader
                    .get_range(CENTER_PREFIX..=CENTER_PREFIX, ..)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    cluster_metadata.insert(key, (value, 0));
                }
                for (_prefix, key, value) in reader
                    .get_range(LENGTH_PREFIX..=LENGTH_PREFIX, ..)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    if let Some(mut entry) = cluster_metadata.get_mut(&key) {
                        entry.1 = value as usize;
                    }
                }

                // Load versions
                let versions = DashMap::new();
                for (_prefix, key, value) in reader
                    .get_range(VERSION_PREFIX..=VERSION_PREFIX, ..)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    versions.insert(key, value);
                }

                (next_cluster_id, versions, cluster_metadata)
            } else {
                (0, DashMap::new(), DashMap::new())
            };

        // Open centroid index
        let mode = match config.centroid_id {
            Some(id) => OpenMode::Fork(id),
            None => OpenMode::Create,
        };
        let centroid = usearch_provider
            .open(&usearch_config, mode)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

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
                    .read::<_, DataRecord<'_>>(options)
                    .await
                    .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?,
            )
        } else {
            None
        };

        // Load center embeddings from raw embedding reader and apply rotation
        let embeddings = DashMap::new();
        if let Some(ref reader) = raw_embedding_reader {
            let center_vector_ids: Vec<u64> =
                cluster_metadata.iter().map(|e| e.value().0).collect();
            reader
                .load_data_for_keys(
                    center_vector_ids
                        .iter()
                        .map(|id| (String::new(), *id as u32)),
                )
                .await;
            for id in &center_vector_ids {
                if let Some(record) = reader
                    .get("", *id as u32)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?
                {
                    // Apply rotation (and normalization for cosine)
                    let rotated = match config.distance_function {
                        DistanceFunction::Cosine => {
                            let normalized = normalize(record.embedding);
                            &rotation * ColRef::from_slice(&normalized)
                        }
                        _ => &rotation * ColRef::from_slice(record.embedding),
                    };
                    embeddings.insert(*id, rotated.iter().copied().collect::<Arc<_>>());
                }
            }
        }

        // Initialize deltas from cluster_metadata and loaded embeddings
        let deltas = DashMap::new();
        for entry in cluster_metadata.iter() {
            let cluster_id = *entry.key();
            let (center_vector_id, length) = *entry.value();
            if let Some(center) = embeddings.get(&center_vector_id) {
                deltas.insert(
                    cluster_id,
                    QuantizedDelta {
                        center_vector_id,
                        length,
                        cluster: QuantizedClusterOwned::new(center.clone()),
                    },
                );
            }
        }

        Ok(Self {
            config,
            centroid,
            next_cluster_id: AtomicU64::new(next_cluster_id).into(),
            rotation,
            deltas: deltas.into(),
            tombstones: DashSet::new().into(),
            embeddings: embeddings.into(),
            versions: versions.into(),
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
                .filter_map(|e| (!e.value().cluster.ids.is_empty()).then_some(*e.key()))
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
                .write::<u64, QuantizedCluster<'_>>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Apply changes in order
            for cluster_id in cluster_ids {
                if let Some(delta) = self.deltas.get(&cluster_id) {
                    let cluster_ref = QuantizedCluster::from(&delta.cluster);
                    writer
                        .set("", cluster_id, cluster_ref)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                } else {
                    writer
                        .delete::<u64, QuantizedCluster<'_>>("", cluster_id)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            writer
                .commit::<u64, QuantizedCluster<'_>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 2: scalar_metadata blockfile ===
        // Stores: next_cluster_id, center_vector_ids, lengths, versions
        // Always create fresh, write in alphabetical prefix order: center < length < next < version
        // NOTE(sicheng): Must come after quantized_cluster because scrubbing may change lengths
        let scalar_metadata_flusher = {
            let mut options = BlockfileWriterOptions::new(self.config.prefix_path.clone());
            if let Some(cmek) = &self.config.cmek {
                options = options.with_cmek(cmek.clone());
            }
            let writer = blockfile_provider
                .write::<u64, u64>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // 1. CENTER_PREFIX - sorted by cluster_id
            let mut centers = self
                .deltas
                .iter()
                .map(|e| (*e.key(), e.value().center_vector_id))
                .collect::<Vec<_>>();
            centers.sort_unstable();
            for (cluster_id, center_vector_id) in centers {
                writer
                    .set(CENTER_PREFIX, cluster_id, center_vector_id)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            // 2. LENGTH_PREFIX - sorted by cluster_id
            let mut lengths = self
                .deltas
                .iter()
                .map(|e| (*e.key(), e.value().length as u64))
                .collect::<Vec<_>>();
            lengths.sort_unstable();
            for (cluster_id, length) in lengths {
                writer
                    .set(LENGTH_PREFIX, cluster_id, length)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            // 3. NEXT_CLUSTER_PREFIX - single entry
            let next_id = self.next_cluster_id.load(Ordering::Relaxed);
            writer
                .set(NEXT_CLUSTER_PREFIX, 0u64, next_id)
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 4. VERSION_PREFIX - sorted by point_id
            let mut versions = self
                .versions
                .iter()
                .map(|e| (*e.key(), *e.value()))
                .collect::<Vec<_>>();
            versions.sort_unstable();
            for (point_id, version) in versions {
                writer
                    .set(VERSION_PREFIX, point_id, version)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            writer
                .commit::<u64, u64>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 3: embedding_metadata blockfile ===
        // Stores: rotation matrix columns (immutable after creation)
        // Only write rotation if new index, otherwise fork unchanged
        let embedding_metadata_flusher = {
            let mut options = BlockfileWriterOptions::new(self.config.prefix_path.clone());
            if let Some(id) = self.config.embedding_metadata_id {
                options = options.fork(id);
            }
            if let Some(cmek) = &self.config.cmek {
                options = options.with_cmek(cmek.clone());
            }
            let writer = blockfile_provider
                .write::<u64, Vec<f32>>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Only write rotation matrix for new indexes (rotation is immutable)
            if self.config.embedding_metadata_id.is_none() {
                let dim = self.config.dimensions;
                for col_idx in 0..dim {
                    let column: Vec<f32> =
                        (0..dim).map(|row| self.rotation[(row, col_idx)]).collect();
                    writer
                        .set(ROTATION_PREFIX, col_idx as u64, column)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            writer
                .commit::<u64, Vec<f32>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        Ok(QuantizedSpannFlusher {
            config: self.config,
            centroid: self.centroid,
            embedding_metadata_flusher,
            quantized_cluster_flusher,
            scalar_metadata_flusher,
            usearch_provider,
        })
    }
}

/// Flusher for persisting a quantized SPANN index to storage.
pub struct QuantizedSpannFlusher {
    config: QuantizedSpannConfig,
    centroid: USearchIndex,
    embedding_metadata_flusher: BlockfileFlusher,
    quantized_cluster_flusher: BlockfileFlusher,
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
            .flush::<u64, Vec<f32>>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.quantized_cluster_flusher
            .flush::<u64, QuantizedCluster<'_>>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.scalar_metadata_flusher
            .flush::<u64, u64>()
            .await
            .map_err(QuantizedSpannError::Blockfile)?;

        // Flush centroid index
        let centroid_id = self
            .usearch_provider
            .flush(&self.centroid)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        // Return updated config with all IDs
        Ok(QuantizedSpannConfig {
            embedding_metadata_id: Some(embedding_metadata_id),
            quantized_cluster_id: Some(quantized_cluster_id),
            scalar_metadata_id: Some(scalar_metadata_id),
            centroid_id: Some(centroid_id),
            ..self.config
        })
    }
}
