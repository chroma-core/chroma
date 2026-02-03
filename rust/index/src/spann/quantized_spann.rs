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
use chroma_types::{
    Cmek, CollectionUuid, DataRecord, InternalSpannConfiguration, QuantizedCluster, Space,
};
use dashmap::{DashMap, DashSet};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};
use simsimd::SpatialSimilarity;

use thiserror::Error;

use chroma_blockstore::provider::BlockfileProvider;

use crate::{
    quantization::Code,
    spann::{types::QuantizedSpannIds, utils},
    usearch::{USearchIndex, USearchIndexConfig, USearchIndexProvider},
    OpenMode, SearchResult, VectorIndex, VectorIndexProvider,
};

// Blockfile prefixes
const PREFIX_CENTER: &str = "center";
const PREFIX_LENGTH: &str = "length";
const PREFIX_NEXT_CLUSTER: &str = "next";
const PREFIX_ROTATION: &str = "rotation";
const PREFIX_VERSION: &str = "version";

/// In-memory staging for a quantized cluster head.
#[derive(Clone)]
struct QuantizedDelta {
    center: Arc<[f32]>,
    codes: Vec<Arc<[u8]>>,
    ids: Vec<u32>,
    length: usize,
    versions: Vec<u32>,
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
pub struct QuantizedSpannIndexWriter<I: VectorIndex> {
    // === Config ===
    cmek: Option<Cmek>,
    collection_id: CollectionUuid,
    dimension: usize,
    file_ids: Option<QuantizedSpannIds>,
    params: InternalSpannConfiguration,
    prefix_path: String,

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

    // === Dedup Sets ===
    balancing: Arc<DashSet<u32>>,
}

impl<I: VectorIndex> QuantizedSpannIndexWriter<I> {
    pub async fn add(&self, id: u32, embedding: &[f32]) -> Result<(), QuantizedSpannError> {
        let rotated = self.rotate(embedding);
        self.embeddings.insert(id, rotated.clone());
        self.insert(id, rotated).await
    }

    pub fn remove(&self, id: u32) {
        self.upgrade(id);
    }
}

impl<I: VectorIndex> QuantizedSpannIndexWriter<I> {
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
        if !self.balancing.insert(cluster_id) {
            return Ok(());
        }

        let Some(len) = self.scrub(cluster_id).await? else {
            self.balancing.remove(&cluster_id);
            return Ok(());
        };

        if len > self.params.split_threshold as usize {
            self.split(cluster_id).await?;
        } else if len > 0 && len < self.params.merge_threshold as usize {
            self.merge(cluster_id).await?;
        }

        self.balancing.remove(&cluster_id);
        Ok(())
    }

    /// Get the centroid for a cluster, cloning to release the lock.
    fn centroid(&self, cluster_id: u32) -> Option<Arc<[f32]>> {
        self.deltas
            .get(&cluster_id)
            .map(|delta| delta.center.clone())
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
        match self.params.space {
            Space::L2 => DistanceFunction::Euclidean.distance(a, b),
            Space::Cosine => DistanceFunction::Cosine.distance(a, b),
            Space::Ip => DistanceFunction::InnerProduct.distance(a, b),
        }
    }

    /// Remove a cluster from both centroid indexes and register as tombstone.
    fn drop(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        self.raw_centroid
            .remove(cluster_id)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.quantized_centroid
            .remove(cluster_id)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.tombstones.insert(cluster_id);
        Ok(())
    }

    /// Insert a rotated vector into the index.
    async fn insert(&self, id: u32, embedding: Arc<[f32]>) -> Result<(), QuantizedSpannError> {
        let candidates = self.navigate(&embedding, self.params.write_nprobe as usize)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        for cluster_id in self.register(id, embedding, &rng_cluster_ids)? {
            Box::pin(self.balance(cluster_id)).await?;
        }

        Ok(())
    }

    /// Check if a point is valid (version matches current version).
    fn is_valid(&self, id: u32, version: u32) -> bool {
        self.versions
            .get(&id)
            .is_some_and(|global_version| *global_version == version)
    }

    /// Load cluster data from reader into deltas.
    async fn load(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

        if self
            .deltas
            .get(&cluster_id)
            .is_none_or(|delta| delta.ids.len() >= delta.length)
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
            if delta.ids.len() < delta.length {
                for ((id, version), code) in persisted
                    .ids
                    .iter()
                    .zip(persisted.versions.iter())
                    .zip(persisted.codes.chunks(code_size))
                {
                    delta.codes.push(Arc::from(code));
                    delta.ids.push(*id);
                    delta.versions.push(*version);
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

        let neighbors = self.navigate(&source_center, self.params.write_nprobe as usize)?;
        let Some(nearest_cluster_id) = neighbors
            .keys
            .iter()
            .copied()
            .find(|neighbor_cluster_id| *neighbor_cluster_id != cluster_id)
        else {
            return Ok(());
        };

        let Some(target_center) = self.centroid(nearest_cluster_id) else {
            return Ok(());
        };

        let Some(source_delta) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        self.drop(cluster_id)?;
        for (id, version) in source_delta.ids.iter().zip(source_delta.versions.iter()) {
            let Some(embedding) = self.embeddings.get(id).map(|emb| emb.clone()) else {
                continue;
            };

            let dist_to_target = self.distance(&embedding, &target_center);
            let dist_to_source = self.distance(&embedding, &source_center);

            if dist_to_target <= dist_to_source {
                let code = Code::<Vec<u8>>::quantize(&embedding, &target_center)
                    .as_ref()
                    .into();
                self.append(nearest_cluster_id, *id, *version, code);
            } else {
                self.reassign(cluster_id, *id, *version, embedding).await?;
            }
        }

        Ok(())
    }

    /// Query the centroid index for the nearest cluster heads.
    fn navigate(&self, query: &[f32], count: usize) -> Result<SearchResult, QuantizedSpannError> {
        self.raw_centroid
            .search(query, count)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))
    }

    /// Reassign a vector to new clusters via RNG query
    async fn reassign(
        &self,
        from_cluster_id: u32,
        id: u32,
        version: u32,
        embedding: Arc<[f32]>,
    ) -> Result<(), QuantizedSpannError> {
        if !self.is_valid(id, version) {
            return Ok(());
        }

        let candidates = self.navigate(&embedding, self.params.write_nprobe as usize)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        if rng_cluster_ids.contains(&from_cluster_id) {
            return Ok(());
        }

        if !self.is_valid(id, version) {
            return Ok(());
        }

        for cluster_id in self.register(id, embedding, &rng_cluster_ids)? {
            Box::pin(self.balance(cluster_id)).await?;
        }

        Ok(())
    }

    /// Register a vector in target clusters.
    /// Returns the clusters whose lengths exceed split threshold
    fn register(
        &self,
        id: u32,
        embedding: Arc<[f32]>,
        target_cluster_ids: &[u32],
    ) -> Result<Vec<u32>, QuantizedSpannError> {
        let version = self.upgrade(id);

        let mut registered = false;
        let mut staging = Vec::new();

        for cluster_id in target_cluster_ids {
            let Some(centroid) = self.centroid(*cluster_id) else {
                continue;
            };

            let code = Code::<Vec<u8>>::quantize(&embedding, &centroid)
                .as_ref()
                .into();

            let Some(len) = self.append(*cluster_id, id, version, code) else {
                continue;
            };

            registered = true;

            if len > self.params.split_threshold as usize {
                staging.push(*cluster_id);
            }
        }

        if !registered {
            let code = Code::<Vec<u8>>::quantize(&embedding, &embedding)
                .as_ref()
                .into();
            let delta = QuantizedDelta {
                center: embedding,
                codes: vec![code],
                ids: vec![id],
                length: 1,
                versions: vec![version],
            };
            self.spawn(delta)?;
        }

        Ok(staging)
    }

    /// Apply epsilon and RNG filtering to navigate results.
    /// Returns up to `replica_count` cluster heads that pass both filters.
    fn rng_select(&self, candidates: &SearchResult) -> SearchResult {
        let first_distance = candidates.distances.first().copied().unwrap_or(0.0);
        let mut result = SearchResult::default();
        let mut selected_centroids =
            Vec::<Arc<_>>::with_capacity(self.params.nreplica_count as usize);

        for (cluster_id, distance) in candidates.keys.iter().zip(candidates.distances.iter()) {
            // Epsilon filter
            if (distance - first_distance).abs()
                > self.params.write_rng_epsilon * first_distance.abs()
            {
                break;
            }

            let Some(center) = self.centroid(*cluster_id) else {
                continue;
            };

            // RNG filter
            if selected_centroids.iter().any(|sel| {
                self.params.write_rng_factor * self.distance(&center, sel).abs() <= distance.abs()
            }) {
                continue;
            }

            result.keys.push(*cluster_id);
            result.distances.push(*distance);
            selected_centroids.push(center);

            if result.keys.len() >= self.params.nreplica_count as usize {
                break;
            }
        }

        result
    }

    /// Normalize (if cosine) and rotate a vector for RaBitQ quantization.
    fn rotate(&self, embedding: &[f32]) -> Arc<[f32]> {
        let rotated = match self.params.space {
            Space::Cosine => {
                let normalized = normalize(embedding);
                &self.rotation * ColRef::from_slice(&normalized)
            }
            _ => &self.rotation * ColRef::from_slice(embedding),
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

    /// Spawn a new cluster and register it in the centroid index.
    fn spawn(&self, delta: QuantizedDelta) -> Result<u32, QuantizedSpannError> {
        let cluster_id = self.next_cluster_id.fetch_add(1, Ordering::Relaxed);
        let center = delta.center.clone();
        self.deltas.insert(cluster_id, delta);
        self.raw_centroid
            .add(cluster_id, &center)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.quantized_centroid
            .add(cluster_id, &center)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        Ok(cluster_id)
    }

    /// Split a large cluster into two smaller clusters using 2-means clustering.
    async fn split(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(old_center) = self.centroid(cluster_id) else {
            return Ok(());
        };
        let Some(delta) = self.detach(cluster_id).await? else {
            return Ok(());
        };

        let embeddings = delta
            .ids
            .iter()
            .zip(delta.versions.iter())
            .filter_map(|(id, version)| {
                self.is_valid(*id, *version)
                    .then(|| {
                        self.embeddings
                            .get(id)
                            .map(|emb| (*id, *version, emb.clone()))
                    })
                    .flatten()
            })
            .collect::<Vec<_>>();

        if embeddings.len() <= self.params.split_threshold as usize {
            self.deltas.insert(cluster_id, delta);
            return Ok(());
        }

        let distance_function = DistanceFunction::from(self.params.space.clone());
        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &distance_function);

        let left_distance = self.distance(&left_center, &old_center);
        let right_distance = self.distance(&right_center, &old_center);

        if left_distance.abs() < f32::EPSILON && right_distance.abs() < f32::EPSILON {
            self.deltas.insert(cluster_id, delta);
            return Ok(());
        }

        let left_delta = QuantizedDelta {
            center: left_center.clone(),
            codes: left_group
                .iter()
                .map(|(_, _, emb)| Code::<Vec<u8>>::quantize(emb, &left_center).as_ref().into())
                .collect(),
            ids: left_group.iter().map(|(id, _, _)| *id).collect(),
            length: left_group.len(),
            versions: left_group.iter().map(|(_, version, _)| *version).collect(),
        };

        let left_cluster_id = if left_distance.abs() < f32::EPSILON {
            self.deltas.insert(cluster_id, left_delta);
            cluster_id
        } else {
            self.spawn(left_delta)?
        };

        let right_delta = QuantizedDelta {
            center: right_center.clone(),
            codes: right_group
                .iter()
                .map(|(_, _, emb)| {
                    Code::<Vec<u8>>::quantize(emb, &right_center)
                        .as_ref()
                        .into()
                })
                .collect(),
            ids: right_group.iter().map(|(id, _, _)| *id).collect(),
            length: right_group.len(),
            versions: right_group.iter().map(|(_, version, _)| *version).collect(),
        };

        let right_cluster_id = if right_distance.abs() < f32::EPSILON {
            self.deltas.insert(cluster_id, right_delta);
            cluster_id
        } else {
            self.spawn(right_delta)?
        };

        if left_cluster_id != cluster_id && right_cluster_id != cluster_id {
            self.drop(cluster_id)?;
        }

        // NPA check for split points
        let evaluated = DashSet::new();

        if left_cluster_id != cluster_id {
            for (id, version, embedding) in &left_group {
                if !self.is_valid(*id, *version) {
                    continue;
                }
                if !evaluated.insert(*id) {
                    continue;
                }
                let old_dist = self.distance(embedding, &old_center);
                let new_dist = self.distance(embedding, &left_center);
                if new_dist > old_dist {
                    self.reassign(left_cluster_id, *id, *version, embedding.clone())
                        .await?;
                }
            }
        }

        if right_cluster_id != cluster_id {
            for (id, version, embedding) in &right_group {
                if !self.is_valid(*id, *version) {
                    continue;
                }
                if !evaluated.insert(*id) {
                    continue;
                }
                let old_dist = self.distance(embedding, &old_center);
                let new_dist = self.distance(embedding, &right_center);
                if new_dist > old_dist {
                    self.reassign(right_cluster_id, *id, *version, embedding.clone())
                        .await?;
                }
            }
        }

        // NPA check for neighbor points
        let mut reassign_candidates = Vec::new();
        let old_q_norm = f32::dot(&old_center, &old_center).unwrap_or(0.0).sqrt() as f32;
        let left_q_norm = if left_cluster_id == cluster_id {
            old_q_norm
        } else {
            f32::dot(&left_center, &left_center).unwrap_or(0.0).sqrt() as f32
        };
        let right_q_norm = if right_cluster_id == cluster_id {
            old_q_norm
        } else {
            f32::dot(&right_center, &right_center).unwrap_or(0.0).sqrt() as f32
        };

        let neighbors = self.navigate(&old_center, self.params.reassign_neighbor_count as usize)?;
        for neighbor_id in neighbors.keys {
            if neighbor_id == cluster_id
                || neighbor_id == left_cluster_id
                || neighbor_id == right_cluster_id
            {
                continue;
            }
            self.scrub(neighbor_id).await?;
            let Some(neighbor_delta) = self.deltas.get(&neighbor_id).map(|d| d.clone()) else {
                continue;
            };

            let c_norm = f32::dot(&neighbor_delta.center, &neighbor_delta.center)
                .unwrap_or(0.0)
                .sqrt() as f32;

            let old_r_q = old_center
                .iter()
                .zip(neighbor_delta.center.iter())
                .map(|(a, b)| a - b)
                .collect::<Vec<_>>();
            let old_c_dot_q = f32::dot(&neighbor_delta.center, &old_center).unwrap_or(0.0) as f32;

            let (left_r_q, left_c_dot_q) = if left_cluster_id == cluster_id {
                (old_r_q.clone(), old_c_dot_q)
            } else {
                let r_q = left_center
                    .iter()
                    .zip(neighbor_delta.center.iter())
                    .map(|(a, b)| a - b)
                    .collect::<Vec<_>>();
                let c_dot_q = f32::dot(&neighbor_delta.center, &left_center).unwrap_or(0.0) as f32;
                (r_q, c_dot_q)
            };

            let (right_r_q, right_c_dot_q) = if right_cluster_id == cluster_id {
                (old_r_q.clone(), old_c_dot_q)
            } else {
                let r_q = right_center
                    .iter()
                    .zip(neighbor_delta.center.iter())
                    .map(|(a, b)| a - b)
                    .collect::<Vec<_>>();
                let c_dot_q = f32::dot(&neighbor_delta.center, &right_center).unwrap_or(0.0) as f32;
                (r_q, c_dot_q)
            };

            let neighbor_r_q = vec![0.0; neighbor_delta.center.len()];
            let neighbor_c_dot_q = c_norm * c_norm;
            let neighbor_q_norm = c_norm;

            for (i, code) in neighbor_delta.codes.iter().enumerate() {
                let id = neighbor_delta.ids[i];
                let version = neighbor_delta.versions[i];

                if !self.is_valid(id, version) {
                    continue;
                }
                if !evaluated.insert(id) {
                    continue;
                }

                let code = Code::<&[u8]>::new(code.as_ref());

                let neighbor_dist = code.distance_query(
                    &distance_function,
                    &neighbor_r_q,
                    c_norm,
                    neighbor_c_dot_q,
                    neighbor_q_norm,
                );
                let left_dist = code.distance_query(
                    &distance_function,
                    &left_r_q,
                    c_norm,
                    left_c_dot_q,
                    left_q_norm,
                );
                let right_dist = code.distance_query(
                    &distance_function,
                    &right_r_q,
                    c_norm,
                    right_c_dot_q,
                    right_q_norm,
                );
                let old_dist = code.distance_query(
                    &distance_function,
                    &old_r_q,
                    c_norm,
                    old_c_dot_q,
                    old_q_norm,
                );

                if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                    continue;
                }
                if old_dist <= left_dist && old_dist <= right_dist {
                    continue;
                }

                reassign_candidates.push((neighbor_id, id, version));
            }
        }

        let candidate_ids = reassign_candidates
            .iter()
            .map(|(_, id, _)| *id)
            .collect::<Vec<_>>();
        self.load_raw(&candidate_ids).await?;

        for (from_cluster_id, id, version) in reassign_candidates {
            let Some(embedding) = self.embeddings.get(&id).map(|e| e.clone()) else {
                continue;
            };
            self.reassign(from_cluster_id, id, version, embedding)
                .await?;
        }

        Ok(())
    }

    /// Increment and return the next version for a vector.
    fn upgrade(&self, id: u32) -> u32 {
        let mut entry = self.versions.entry(id).or_default();
        *entry += 1;
        *entry
    }
}

impl QuantizedSpannIndexWriter<USearchIndex> {
    /// Commit all in-memory state to blockfile writers and return a flusher.
    ///
    /// This method consumes the index and prepares all data for persistence.
    /// Call `flush()` on the returned flusher to actually write to storage.
    pub async fn commit(
        mut self,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<QuantizedSpannFlusher, QuantizedSpannError> {
        // === Step 0: Check center drift and rebuild centroid indexes if needed ===
        let dim = self.center.len();
        let mut new_center = vec![0.0f32; dim];
        for delta in self.deltas.iter() {
            for (acc_dim, dim) in new_center.iter_mut().zip(delta.center.iter()) {
                *acc_dim += *dim;
            }
        }
        for acc_dim in new_center.iter_mut() {
            *acc_dim /= self.deltas.len().max(1) as f32;
        }

        let diff = new_center
            .iter()
            .zip(self.center.iter())
            .map(|(a, b)| a - b)
            .collect::<Vec<_>>();
        let drift_dist_sq = f32::dot(&diff, &diff).unwrap_or(0.0) as f32;
        let center_norm_sq = f32::dot(&new_center, &new_center).unwrap_or(0.0) as f32;

        self.center = if drift_dist_sq > self.params.center_drift_threshold.powi(2) * center_norm_sq
        {
            // Build USearch config from stored fields
            let usearch_config = USearchIndexConfig {
                collection_id: self.collection_id,
                cmek: self.cmek.clone(),
                prefix_path: self.prefix_path.clone(),
                dimensions: self.dimension,
                distance_function: DistanceFunction::from(self.params.space.clone()),
                connectivity: self.params.max_neighbors,
                expansion_add: self.params.ef_construction,
                expansion_search: self.params.ef_search,
                quantization_center: None,
            };

            self.raw_centroid = usearch_provider
                .open(&usearch_config, OpenMode::Create)
                .await
                .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

            let quantized_config = USearchIndexConfig {
                quantization_center: Some(new_center.clone().into()),
                ..usearch_config
            };
            self.quantized_centroid = usearch_provider
                .open(&quantized_config, OpenMode::Create)
                .await
                .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

            for entry in self.deltas.iter() {
                let cluster_id = *entry.key();
                self.raw_centroid
                    .add(cluster_id, &entry.center)
                    .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
                self.quantized_centroid
                    .add(cluster_id, &entry.center)
                    .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
            }

            new_center.into()
        } else {
            self.center
        };

        // === Step 1: Create blockfile writers ===
        let mut qc_options =
            BlockfileWriterOptions::new(self.prefix_path.clone()).ordered_mutations();
        let mut sm_options =
            BlockfileWriterOptions::new(self.prefix_path.clone()).ordered_mutations();
        let mut em_options =
            BlockfileWriterOptions::new(self.prefix_path.clone()).ordered_mutations();

        if let Some(file_ids) = &self.file_ids {
            qc_options = qc_options.fork(file_ids.quantized_cluster_id);
            em_options = em_options.fork(file_ids.embedding_metadata_id);
        }

        if let Some(cmek) = &self.cmek {
            qc_options = qc_options.with_cmek(cmek.clone());
            sm_options = sm_options.with_cmek(cmek.clone());
            em_options = em_options.with_cmek(cmek.clone());
        }

        let quantized_cluster_writer = blockfile_provider
            .write::<u32, QuantizedCluster<'_>>(qc_options)
            .await
            .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

        let scalar_metadata_writer = blockfile_provider
            .write::<u32, u32>(sm_options)
            .await
            .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

        let embedding_metadata_writer = blockfile_provider
            .write::<u32, Vec<f32>>(em_options)
            .await
            .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

        // === Step 2: Write quantized_cluster data ===
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

            // Apply changes in order
            for cluster_id in cluster_ids {
                if let Some(delta) = self.deltas.get(&cluster_id) {
                    let codes = delta
                        .codes
                        .iter()
                        .flat_map(|c| c.iter())
                        .copied()
                        .collect::<Vec<_>>();
                    let cluster_ref = QuantizedCluster {
                        center: &delta.center,
                        codes: &codes,
                        ids: &delta.ids,
                        versions: &delta.versions,
                    };
                    quantized_cluster_writer
                        .set("", cluster_id, cluster_ref)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                } else {
                    quantized_cluster_writer
                        .delete::<u32, QuantizedCluster<'_>>("", cluster_id)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            quantized_cluster_writer
                .commit::<u32, QuantizedCluster<'_>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 3: Write scalar_metadata ===
        // Stores: next_cluster_id, lengths, versions
        // Always create fresh, write in alphabetical prefix order: length < next < version
        // NOTE: Must come after Step 2 because scrubbing may change lengths
        let scalar_metadata_flusher = {
            // 1. PREFIX_LENGTH - sorted by cluster_id
            let mut lengths = self
                .deltas
                .iter()
                .map(|e| (*e.key(), e.value().length as u32))
                .collect::<Vec<_>>();
            lengths.sort_unstable();
            for (cluster_id, length) in lengths {
                scalar_metadata_writer
                    .set(PREFIX_LENGTH, cluster_id, length)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            // 2. PREFIX_NEXT_CLUSTER - single entry
            let next_id = self.next_cluster_id.load(Ordering::Relaxed);
            scalar_metadata_writer
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
                scalar_metadata_writer
                    .set(PREFIX_VERSION, point_id, version)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            scalar_metadata_writer
                .commit::<u32, u32>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        // === Step 4: Write embedding_metadata ===
        // Stores: quantization center and rotation matrix columns
        // Write in alphabetical prefix order: center < rotation
        let embedding_metadata_flusher = {
            // 1. PREFIX_CENTER - quantization center (always write, may be updated)
            embedding_metadata_writer
                .set(PREFIX_CENTER, 0u32, self.center.to_vec())
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 2. PREFIX_ROTATION - rotation matrix columns (only write for new indexes)
            if self.file_ids.is_none() {
                let dim = self.center.len();
                for col_idx in 0..dim {
                    let column = (0..dim)
                        .map(|row| self.rotation[(row, col_idx)])
                        .collect::<Vec<_>>();
                    embedding_metadata_writer
                        .set(PREFIX_ROTATION, col_idx as u32, column)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            embedding_metadata_writer
                .commit::<u32, Vec<f32>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)?
        };

        Ok(QuantizedSpannFlusher {
            embedding_metadata_flusher,
            quantized_centroid: self.quantized_centroid,
            quantized_cluster_flusher,
            raw_centroid: self.raw_centroid,
            scalar_metadata_flusher,
            usearch_provider: usearch_provider.clone(),
        })
    }

    /// Create a new quantized SPANN index.
    pub async fn create(
        collection_id: CollectionUuid,
        dimension: usize,
        params: InternalSpannConfiguration,
        cmek: Option<Cmek>,
        prefix_path: String,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        // Create random rotation matrix
        let dist = UnitaryMat {
            dim: dimension,
            standard_normal: StandardNormal,
        };
        let rotation = dist.sample(&mut ThreadRng::default());
        let center = Arc::<[f32]>::from(vec![0.0; dimension]);

        // Build USearch config from params
        let usearch_config = USearchIndexConfig {
            collection_id,
            cmek: cmek.clone(),
            prefix_path: prefix_path.clone(),
            dimensions: dimension,
            distance_function: DistanceFunction::from(params.space.clone()),
            connectivity: params.max_neighbors,
            expansion_add: params.ef_construction,
            expansion_search: params.ef_search,
            quantization_center: None,
        };

        // Create centroid indexes
        let raw_centroid = usearch_provider
            .open(&usearch_config, OpenMode::Create)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        let quantized_usearch_config = USearchIndexConfig {
            quantization_center: Some(center.clone()),
            ..usearch_config
        };
        let quantized_centroid = usearch_provider
            .open(&quantized_usearch_config, OpenMode::Create)
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        Ok(Self {
            // === Config ===
            cmek,
            collection_id,
            dimension,
            file_ids: None,
            params,
            prefix_path,
            // === Centroid Index ===
            next_cluster_id: Arc::new(AtomicU32::new(0)),
            quantized_centroid,
            raw_centroid,
            // === Quantization ===
            center,
            rotation,
            // === In-Memory State ===
            deltas: DashMap::new().into(),
            embeddings: DashMap::new().into(),
            tombstones: DashSet::new().into(),
            versions: DashMap::new().into(),
            // === Blockfile Readers ===
            quantized_cluster_reader: None,
            raw_embedding_reader: None,
            // === Dedup Sets ===
            balancing: DashSet::new().into(),
        })
    }

    /// Open an existing quantized SPANN index from file IDs.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        collection_id: CollectionUuid,
        dimension: usize,
        params: InternalSpannConfiguration,
        file_ids: QuantizedSpannIds,
        cmek: Option<Cmek>,
        prefix_path: String,
        raw_embedding_reader: Option<BlockfileReader<'static, u32, DataRecord<'static>>>,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        // Step 0: Load embedding_metadata (rotation matrix + quantization center)
        let options =
            BlockfileReaderOptions::new(file_ids.embedding_metadata_id, prefix_path.clone());
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
        if columns.len() != dimension {
            return Err(QuantizedSpannError::DimensionMismatch {
                expected: dimension,
                got: columns.len(),
            });
        }

        // Validate each column length
        for (_prefix, _key, col) in &columns {
            if col.len() != dimension {
                return Err(QuantizedSpannError::DimensionMismatch {
                    expected: dimension,
                    got: col.len(),
                });
            }
        }

        // Construct rotation matrix column by column
        let rotation = Mat::from_fn(dimension, dimension, |i, j| columns[j].2[i]);

        // Load quantization center
        let center = reader
            .get(PREFIX_CENTER, 0)
            .await
            .map_err(QuantizedSpannError::Blockfile)?
            .map(Arc::<[f32]>::from)
            .unwrap_or_else(|| vec![0.0; dimension].into());

        // Build USearch config from params
        let usearch_config = USearchIndexConfig {
            collection_id,
            cmek: cmek.clone(),
            prefix_path: prefix_path.clone(),
            dimensions: dimension,
            distance_function: DistanceFunction::from(params.space.clone()),
            connectivity: params.max_neighbors,
            expansion_add: params.ef_construction,
            expansion_search: params.ef_search,
            quantization_center: None,
        };

        // Step 1: Open centroid indexes
        let raw_centroid = usearch_provider
            .open(&usearch_config, OpenMode::Fork(file_ids.raw_centroid_id))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        let quantized_usearch_config = USearchIndexConfig {
            quantization_center: Some(center.clone()),
            ..usearch_config
        };
        let quantized_centroid = usearch_provider
            .open(
                &quantized_usearch_config,
                OpenMode::Fork(file_ids.quantized_centroid_id),
            )
            .await
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

        // Step 2: Load scalar metadata (next_cluster_id, versions, cluster_lengths)
        let options = BlockfileReaderOptions::new(file_ids.scalar_metadata_id, prefix_path.clone());
        let reader = blockfile_provider
            .read::<u32, u32>(options)
            .await
            .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?;

        // Load cluster lengths
        let cluster_lengths = DashMap::new();
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
        let versions = DashMap::new();
        for (_prefix, key, value) in reader
            .get_range(PREFIX_VERSION..=PREFIX_VERSION, ..)
            .await
            .map_err(QuantizedSpannError::Blockfile)?
        {
            versions.insert(key, value);
        }

        // Open quantized cluster reader
        let options =
            BlockfileReaderOptions::new(file_ids.quantized_cluster_id, prefix_path.clone());
        let quantized_cluster_reader = Some(
            blockfile_provider
                .read(options)
                .await
                .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?,
        );

        // Step 3: Initialize deltas from cluster_lengths by getting centers from raw_centroid
        let deltas = DashMap::new();
        for entry in cluster_lengths.iter() {
            let cluster_id = *entry.key();
            let length = *entry.value();

            // Get center embedding from raw_centroid index
            if let Some(center_embedding) = raw_centroid
                .get(cluster_id)
                .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?
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
            cmek,
            collection_id,
            dimension,
            file_ids: Some(file_ids),
            params,
            prefix_path,
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
            // === Dedup Sets ===
            balancing: DashSet::new().into(),
        })
    }
}

/// Flusher for persisting a quantized SPANN index to storage.
pub struct QuantizedSpannFlusher {
    embedding_metadata_flusher: BlockfileFlusher,
    quantized_centroid: USearchIndex,
    quantized_cluster_flusher: BlockfileFlusher,
    raw_centroid: USearchIndex,
    scalar_metadata_flusher: BlockfileFlusher,
    usearch_provider: USearchIndexProvider,
}

impl QuantizedSpannFlusher {
    /// Flush all data to storage and return the file IDs.
    pub async fn flush(self) -> Result<QuantizedSpannIds, QuantizedSpannError> {
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

        // Return file IDs
        Ok(QuantizedSpannIds {
            embedding_metadata_id,
            quantized_centroid_id,
            quantized_cluster_id,
            raw_centroid_id,
            scalar_metadata_id,
        })
    }
}
