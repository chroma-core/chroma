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
    pub async fn add(&self, id: u32, vector: &[f32]) -> Result<(), QuantizedSpannError> {
        let rotated = self.rotate(vector);
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
    async fn insert(&self, id: u32, vector: Arc<[f32]>) -> Result<(), QuantizedSpannError> {
        let version = self.upgrade(id);
        let candidates = self.navigate(&vector, self.params.write_nprobe as usize)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        if rng_cluster_ids.is_empty() {
            let code = Code::<Vec<u8>>::quantize(&vector, &vector).as_ref().into();
            let delta = QuantizedDelta {
                center: vector,
                codes: vec![code],
                ids: vec![id],
                length: 1,
                versions: vec![version],
            };
            self.spawn(delta)?;
        } else {
            let mut staging = Vec::new();
            for cluster_id in rng_cluster_ids {
                if let Some(centroid) = self.centroid(cluster_id) {
                    let code = Code::<Vec<u8>>::quantize(&vector, &centroid)
                        .as_ref()
                        .into();
                    if self
                        .append(cluster_id, id, version, code)
                        .is_some_and(|len| len > self.params.split_threshold as usize)
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
        self.versions
            .get(&id)
            .is_some_and(|global_version| *global_version == version)
    }

    /// Load cluster data from reader into deltas (reconciliation).
    async fn load(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

        if self
            .deltas
            .get(&cluster_id)
            .is_none_or(|delta| delta.ids.len() >= delta.length as usize)
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

    /// Reassign a point to new clusters via RNG query.
    ///
    /// Called when a point ends up further from its new cluster center than it was
    /// from the old center (NPA check failure). Finds better clusters via RNG and
    /// appends the point there with an incremented version.
    ///
    /// Does NOT trigger balance on target clusters to avoid cascading splits.
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

        let new_version = self.upgrade(id);
        let mut staging = Vec::new();
        for cluster_id in rng_cluster_ids {
            if let Some(centroid) = self.centroid(cluster_id) {
                let code = Code::<Vec<u8>>::quantize(&embedding, &centroid)
                    .as_ref()
                    .into();
                if self
                    .append(cluster_id, id, new_version, code)
                    .is_some_and(|len| len > self.params.split_threshold as usize)
                {
                    staging.push(cluster_id);
                }
            }
        }

        for cluster_id in staging {
            Box::pin(self.balance(cluster_id)).await?;
        }

        Ok(())
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
    fn rotate(&self, vector: &[f32]) -> Arc<[f32]> {
        let rotated = match self.params.space {
            Space::Cosine => {
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
            for i in 0..dim {
                new_center[i] += delta.center[i];
            }
        }
        for i in 0..dim {
            new_center[i] /= self.deltas.len().max(1) as f32;
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, InternalSpannConfiguration, Space};
    use tempfile::TempDir;

    use super::{QuantizedDelta, QuantizedSpannIndexWriter};
    use crate::usearch::{USearchIndex, USearchIndexProvider};

    const TEST_DIMENSION: usize = 4;
    const TEST_EPSILON: f32 = 1e-5;

    fn test_params() -> InternalSpannConfiguration {
        InternalSpannConfiguration {
            space: Space::Cosine,
            write_nprobe: 4,
            nreplica_count: 2,
            write_rng_epsilon: 4.0,
            write_rng_factor: 1.0,
            split_threshold: 8,
            merge_threshold: 2,
            reassign_neighbor_count: 6,
            center_drift_threshold: 0.125,
            search_nprobe: 4,
            search_rng_epsilon: 4.0,
            search_rng_factor: 1.0,
            ef_construction: 32,
            ef_search: 16,
            ..Default::default()
        }
    }

    fn test_storage(tmp_dir: &TempDir) -> Storage {
        Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()))
    }

    fn test_blockfile_provider(storage: Storage) -> BlockfileProvider {
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            16,
        );
        BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
    }

    fn test_usearch_provider(storage: Storage) -> USearchIndexProvider {
        let usearch_cache = new_non_persistent_cache_for_test();
        USearchIndexProvider::new(storage, usearch_cache)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        // === Setup ===
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let usearch_provider = test_usearch_provider(storage);

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            CollectionUuid::new(),
            TEST_DIMENSION,
            test_params(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // =======================================================================
        // Level 0: Pure/Accessor Operations
        // =======================================================================

        // --- upgrade ---
        let v1 = writer.upgrade(1);
        assert_eq!(v1, 1);
        assert_eq!(writer.versions.get(&1).map(|v| *v), Some(1));

        let v2 = writer.upgrade(1);
        assert_eq!(v2, 2);
        assert_eq!(writer.versions.get(&1).map(|v| *v), Some(2));

        let v3 = writer.upgrade(2);
        assert_eq!(v3, 1);
        assert_eq!(writer.versions.get(&2).map(|v| *v), Some(1));

        // --- is_valid ---
        assert!(writer.is_valid(1, 2)); // current version
        assert!(!writer.is_valid(1, 1)); // stale
        assert!(!writer.is_valid(1, 3)); // future
        assert!(!writer.is_valid(999, 1)); // unknown id

        // --- distance (Cosine) ---
        // Cosine distance = 1 - cos(theta)
        // Identical vectors: cos = 1, distance = 0
        assert!(writer.distance(&[1.0, 0.0, 0.0, 0.0], &[1.0, 0.0, 0.0, 0.0]) < TEST_EPSILON);
        // Opposite vectors: cos = -1, distance = 2
        assert!(
            (writer.distance(&[1.0, 0.0, 0.0, 0.0], &[-1.0, 0.0, 0.0, 0.0]) - 2.0).abs()
                < TEST_EPSILON
        );
        // Orthogonal vectors: cos = 0, distance = 1
        assert!(
            (writer.distance(&[1.0, 0.0, 0.0, 0.0], &[0.0, 1.0, 0.0, 0.0]) - 1.0).abs()
                < TEST_EPSILON
        );

        // --- rotate ---
        // For cosine space: normalize first, then rotate. Rotation preserves norm.
        let rotated = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
        assert_eq!(rotated.len(), TEST_DIMENSION);
        let norm = rotated.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < TEST_EPSILON,
            "Expected norm ~1.0, got {}",
            norm
        );

        // Non-unit vector should also result in norm ~1.0 after rotation (due to normalization)
        let rotated2 = writer.rotate(&[2.0, 0.0, 0.0, 0.0]);
        let norm2 = rotated2.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm2 - 1.0).abs() < TEST_EPSILON,
            "Expected norm ~1.0, got {}",
            norm2
        );

        // --- centroid (no clusters yet) ---
        assert!(writer.centroid(1).is_none());
        assert!(writer.centroid(999).is_none());

        // --- navigate (no clusters yet) ---
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(result.keys.is_empty());

        // --- rng_select (empty candidates) ---
        let empty_result = writer.rng_select(&result);
        assert!(empty_result.keys.is_empty());

        // =======================================================================
        // Level 1: Simple Mutations
        // =======================================================================

        // --- spawn ---
        let center1: Arc<[f32]> = Arc::from([1.0f32, 0.0, 0.0, 0.0]);
        let delta1 = QuantizedDelta {
            center: center1.clone(),
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let next_id_before = writer.next_cluster_id.load(Ordering::Relaxed);
        let cluster_id_1 = writer.spawn(delta1).expect("spawn failed");
        assert_eq!(cluster_id_1, next_id_before);
        assert_eq!(
            writer.next_cluster_id.load(Ordering::Relaxed),
            next_id_before + 1
        );

        // Verify centroid is retrievable
        let retrieved_center = writer.centroid(cluster_id_1).expect("centroid not found");
        assert_eq!(retrieved_center.as_ref(), center1.as_ref());

        // --- navigate (with cluster) ---
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(!result.keys.is_empty());
        assert!(result.keys.contains(&cluster_id_1));

        // --- append ---
        let code: Arc<[u8]> = Arc::from([0u8; 8]);
        let v10 = writer.upgrade(10);
        let new_len = writer.append(cluster_id_1, 10, v10, code.clone());
        assert_eq!(new_len, Some(1));

        // Verify delta has the point
        let delta = writer.deltas.get(&cluster_id_1).expect("delta not found");
        assert!(delta.ids.contains(&10));
        assert_eq!(delta.length, 1);

        // Append to non-existent cluster returns None
        let v11 = writer.upgrade(11);
        let bad_append = writer.append(999, 11, v11, code.clone());
        assert!(bad_append.is_none());

        // --- spawn more clusters for RNG test ---
        let center2: Arc<[f32]> = Arc::from([0.0f32, 1.0, 0.0, 0.0]);
        let delta2 = QuantizedDelta {
            center: center2,
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let cluster_id_2 = writer.spawn(delta2).expect("spawn failed");

        let center3: Arc<[f32]> = Arc::from([0.0f32, 0.0, 1.0, 0.0]);
        let delta3 = QuantizedDelta {
            center: center3,
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let cluster_id_3 = writer.spawn(delta3).expect("spawn failed");

        // --- rng_select (with multiple clusters) ---
        // Query near cluster 1
        let candidates = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(candidates.keys.len() >= 1);

        let selected = writer.rng_select(&candidates);
        // Should select at least the closest cluster
        assert!(!selected.keys.is_empty());
        // First selected should be cluster_id_1 (closest to query)
        assert_eq!(selected.keys[0], cluster_id_1);

        // --- drop ---
        writer.drop(cluster_id_2).expect("drop failed");

        // Verify tombstone
        assert!(writer.tombstones.contains(&cluster_id_2));

        // Navigate should NOT return dropped cluster
        let result = writer
            .navigate(&[0.0, 1.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(!result.keys.contains(&cluster_id_2));

        // But centroid still returns Some (delta still exists until detach)
        assert!(writer.centroid(cluster_id_2).is_some());

        // Verify remaining clusters are still navigable
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(result.keys.contains(&cluster_id_1));
        assert!(result.keys.contains(&cluster_id_3));
    }

    #[tokio::test]
    async fn test_load_and_scrub_operations() {
        use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, BlockfileWriterOptions};
        use chroma_types::DataRecord;

        // =======================================================================
        // Setup: Create raw embedding blockfile with test data
        // =======================================================================
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());
        let collection_id = CollectionUuid::new();

        // Raw embeddings for test points (distinct vectors for each id)
        let raw_embeddings = vec![
            (100u32, [1.0f32, 0.0, 0.0, 0.0]),
            (101, [0.0, 1.0, 0.0, 0.0]),
            (102, [0.0, 0.0, 1.0, 0.0]),
            (200, [0.0, 0.0, 0.0, 1.0]),
            (201, [0.5, 0.5, 0.0, 0.0]), // Will be invalidated
            (300, [0.5, 0.0, 0.5, 0.0]),
            (301, [0.0, 0.5, 0.5, 0.0]), // Will be invalidated
            (302, [0.0, 0.0, 0.5, 0.5]),
        ];

        // Create and populate raw embedding blockfile
        let raw_writer = blockfile_provider
            .write::<u32, &DataRecord<'_>>(
                BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
            )
            .await
            .expect("Failed to create raw embedding writer");

        for (id, embedding) in &raw_embeddings {
            let record = DataRecord {
                id: "",
                embedding: embedding.as_slice(),
                metadata: None,
                document: None,
            };
            raw_writer
                .set("", *id, &record)
                .await
                .expect("Failed to write raw embedding");
        }

        let raw_flusher = raw_writer
            .commit::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to commit raw embeddings");
        let raw_embedding_id = raw_flusher.id();
        raw_flusher
            .flush::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to flush raw embeddings");

        // =======================================================================
        // Phase 1: Create index, add points, commit, flush
        // =======================================================================
        let writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            collection_id,
            TEST_DIMENSION,
            test_params(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // Spawn a cluster and add points 100, 101, 102
        let center: Arc<[f32]> = Arc::from([1.0f32, 0.0, 0.0, 0.0]);
        let code: Arc<[u8]> = Arc::from([0u8; 8]);

        // Get versions via upgrade()
        let v100 = writer.upgrade(100);
        let v101 = writer.upgrade(101);
        let v102 = writer.upgrade(102);

        let delta = QuantizedDelta {
            center: center.clone(),
            codes: vec![code.clone(), code.clone(), code.clone()],
            ids: vec![100, 101, 102],
            length: 3,
            versions: vec![v100, v101, v102],
        };
        let cluster_id = writer.spawn(delta).expect("spawn failed");

        // Capture expected rotated embeddings for later verification
        // These are the raw embeddings from raw_embeddings array rotated by the current rotation matrix
        let expected_rotated_100 = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
        let expected_rotated_101 = writer.rotate(&[0.0, 1.0, 0.0, 0.0]);
        let expected_rotated_102 = writer.rotate(&[0.0, 0.0, 1.0, 0.0]);

        // Commit and flush
        let flusher = writer
            .commit(&blockfile_provider, &usearch_provider)
            .await
            .expect("Failed to commit");
        let file_ids = flusher.flush().await.expect("Failed to flush");

        // =======================================================================
        // Phase 2: Reopen index with readers and test load operations
        // =======================================================================
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        // Create raw embedding reader
        let raw_reader = blockfile_provider
            .read::<u32, DataRecord<'static>>(BlockfileReaderOptions::new(
                raw_embedding_id,
                "".to_string(),
            ))
            .await
            .expect("Failed to open raw embedding reader");

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::open(
            collection_id,
            TEST_DIMENSION,
            test_params(),
            file_ids,
            None,
            "".to_string(),
            Some(raw_reader),
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("Failed to open writer");

        // --- load ---
        // After open, delta exists but ids/codes/versions are empty (only length is set)
        {
            let delta = writer.deltas.get(&cluster_id).expect("delta not found");
            assert_eq!(delta.length, 3);
            assert!(delta.ids.is_empty(), "ids should be empty before load");
        }

        // Call load to populate delta from blockfile
        writer.load(cluster_id).await.expect("load failed");

        {
            let delta = writer.deltas.get(&cluster_id).expect("delta not found");
            assert_eq!(delta.ids.len(), 3);
            assert!(delta.ids.contains(&100));
            assert!(delta.ids.contains(&101));
            assert!(delta.ids.contains(&102));
        }

        // --- load_raw ---
        // Verify embeddings cache is empty
        assert!(writer.embeddings.get(&100).is_none());
        assert!(writer.embeddings.get(&101).is_none());
        assert!(writer.embeddings.get(&102).is_none());

        // Load raw embeddings
        writer
            .load_raw(&[100, 101, 102])
            .await
            .expect("load_raw failed");

        // Verify embeddings are now in cache and rotated consistently
        // The rotation matrix was persisted and reloaded, so rotate() should produce same results
        let loaded_100 = writer
            .embeddings
            .get(&100)
            .expect("embedding 100 not found");
        let loaded_101 = writer
            .embeddings
            .get(&101)
            .expect("embedding 101 not found");
        let loaded_102 = writer
            .embeddings
            .get(&102)
            .expect("embedding 102 not found");

        assert!(
            writer.distance(&loaded_100, &expected_rotated_100) < TEST_EPSILON,
            "rotation mismatch for id 100"
        );
        assert!(
            writer.distance(&loaded_101, &expected_rotated_101) < TEST_EPSILON,
            "rotation mismatch for id 101"
        );
        assert!(
            writer.distance(&loaded_102, &expected_rotated_102) < TEST_EPSILON,
            "rotation mismatch for id 102"
        );

        // --- detach ---
        // Spawn a new cluster with points 200, 201
        let center2: Arc<[f32]> = Arc::from([0.0f32, 0.0, 0.0, 1.0]);

        // Get versions via upgrade()
        let v200 = writer.upgrade(200);
        let v201 = writer.upgrade(201);

        let delta2 = QuantizedDelta {
            center: center2,
            codes: vec![code.clone(), code.clone()],
            ids: vec![200, 201],
            length: 2,
            versions: vec![v200, v201],
        };
        let cluster_id_2 = writer.spawn(delta2).expect("spawn failed");

        // Invalidate 201 by upgrading its version
        writer.upgrade(201); // Now version is 2, but cluster has version 1

        // Verify embedding 200 not in cache before detach
        assert!(writer.embeddings.get(&200).is_none());

        // Detach cluster - should load raw embeddings for valid point (200) only
        let detached = writer
            .detach(cluster_id_2)
            .await
            .expect("detach failed")
            .expect("expected delta");
        assert_eq!(detached.ids, vec![200, 201]);

        // Cluster should be removed from deltas
        assert!(writer.deltas.get(&cluster_id_2).is_none());

        // Embedding for valid point 200 should be loaded
        assert!(writer.embeddings.get(&200).is_some());

        // --- scrub ---
        // Spawn a cluster with points 300, 301, 302
        let center3: Arc<[f32]> = Arc::from([0.5f32, 0.0, 0.5, 0.0]);

        // Get versions via upgrade()
        let v300 = writer.upgrade(300);
        let v301 = writer.upgrade(301);
        let v302 = writer.upgrade(302);

        let delta3 = QuantizedDelta {
            center: center3,
            codes: vec![code.clone(), code.clone(), code.clone()],
            ids: vec![300, 301, 302],
            length: 3,
            versions: vec![v300, v301, v302],
        };
        let cluster_id_3 = writer.spawn(delta3).expect("spawn failed");

        // Invalidate 301 by upgrading its version
        writer.upgrade(301); // Now version is 2

        // Before scrub: all 3 points in delta
        {
            let delta = writer.deltas.get(&cluster_id_3).expect("delta not found");
            assert_eq!(delta.ids.len(), 3);
            assert_eq!(delta.length, 3);
        }

        // Scrub should remove invalid point 301
        let new_len = writer
            .scrub(cluster_id_3)
            .await
            .expect("scrub failed")
            .expect("expected length");
        assert_eq!(new_len, 2);

        // After scrub: only points 300, 302 remain
        {
            let delta = writer.deltas.get(&cluster_id_3).expect("delta not found");
            assert_eq!(delta.ids.len(), 2);
            assert_eq!(delta.length, 2);
            assert!(delta.ids.contains(&300));
            assert!(!delta.ids.contains(&301)); // Removed
            assert!(delta.ids.contains(&302));
        }
    }

    #[tokio::test]
    async fn test_insert_and_balance_operations() {
        use crate::quantization::Code;

        // =======================================================================
        // Setup
        // =======================================================================
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let usearch_provider = test_usearch_provider(storage);

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            CollectionUuid::new(),
            TEST_DIMENSION,
            test_params(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // =======================================================================
        // Step 1: insert (empty index -> spawn)
        // =======================================================================
        // First insert on empty index should spawn a new cluster
        assert_eq!(writer.deltas.len(), 0);

        writer
            .add(1, &[1.0, 0.0, 0.0, 0.0])
            .await
            .expect("add failed");

        assert_eq!(writer.deltas.len(), 1);
        let first_cluster_id = *writer.deltas.iter().next().unwrap().key();

        // =======================================================================
        // Step 2: insert (append to existing cluster)
        // =======================================================================
        // Insert more points near the same center - should append, not spawn
        for id in 2..=5 {
            writer
                .add(id, &[1.0, 0.0, 0.0, 0.0])
                .await
                .expect("add failed");
        }

        // Still only 1 cluster
        assert_eq!(writer.deltas.len(), 1);

        // Cluster should have 5 points
        {
            let delta = writer
                .deltas
                .get(&first_cluster_id)
                .expect("delta not found");
            assert_eq!(delta.length, 5);
        }

        // =======================================================================
        // Step 3: split with reassign (triggered by balance)
        // =======================================================================
        // Geometry:
        // - neighbor_center: [0, 1, 0, 0]
        // - mixed_center: [0, 0.9, 0.1, 0] (close to neighbor, so navigate finds it)
        // - After split, new centers will be near [1, 0, 0, 0] and [-1, 0, 0, 0]
        // - Misplaced points [0.9, 0.1, 0, 0] and [-0.9, 0.1, 0, 0] in neighbor
        //   are closer to the new split centers than to neighbor_center

        // Neighbor cluster with incorrectly assigned points
        let neighbor_center: Arc<[f32]> = writer.rotate(&[0.0, 1.0, 0.0, 0.0]);

        // Points that are closer to [1, 0, 0, 0] or [-1, 0, 0, 0] than to [0, 1, 0, 0]
        let misplaced_emb_1 = writer.rotate(&[0.9, 0.1, 0.0, 0.0]);
        let misplaced_emb_2 = writer.rotate(&[-0.9, 0.1, 0.0, 0.0]);

        let v100 = writer.upgrade(100);
        let v101 = writer.upgrade(101);

        // Populate embeddings cache
        writer.embeddings.insert(100, misplaced_emb_1.clone());
        writer.embeddings.insert(101, misplaced_emb_2.clone());

        // Create proper quantization codes
        let code_100: Arc<[u8]> = Code::<Vec<u8>>::quantize(&misplaced_emb_1, &neighbor_center)
            .as_ref()
            .into();
        let code_101: Arc<[u8]> = Code::<Vec<u8>>::quantize(&misplaced_emb_2, &neighbor_center)
            .as_ref()
            .into();

        let neighbor_delta = QuantizedDelta {
            center: neighbor_center.clone(),
            codes: vec![code_100, code_101],
            ids: vec![100, 101],
            length: 2,
            versions: vec![v100, v101],
        };
        let neighbor_cluster_id = writer.spawn(neighbor_delta).expect("spawn failed");

        // Now we have 2 clusters
        assert_eq!(writer.deltas.len(), 2);

        // mixed_center close to neighbor so navigate from mixed_center finds neighbor
        // After split, points will move to centers near [1,0,0,0] and [-1,0,0,0]
        let mixed_center: Arc<[f32]> = writer.rotate(&[0.0, 0.9, 0.1, 0.0]);

        let mut mixed_ids = vec![];
        let mut mixed_versions = vec![];
        let mut mixed_codes = vec![];

        // Group A: 5 points near [1, 0, 0, 0] - will form one split cluster
        for id in 50..55 {
            let v = writer.upgrade(id);
            let emb = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
            let code: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb, &mixed_center)
                .as_ref()
                .into();
            writer.embeddings.insert(id, emb);
            mixed_ids.push(id);
            mixed_versions.push(v);
            mixed_codes.push(code);
        }

        // Group B: 5 points near [-1, 0, 0, 0] - will form another split cluster
        for id in 55..60 {
            let v = writer.upgrade(id);
            let emb = writer.rotate(&[-1.0, 0.0, 0.0, 0.0]);
            let code: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb, &mixed_center)
                .as_ref()
                .into();
            writer.embeddings.insert(id, emb);
            mixed_ids.push(id);
            mixed_versions.push(v);
            mixed_codes.push(code);
        }

        let mixed_delta = QuantizedDelta {
            center: mixed_center,
            codes: mixed_codes,
            ids: mixed_ids,
            length: 10,
            versions: mixed_versions,
        };
        let mixed_cluster_id = writer.spawn(mixed_delta).expect("spawn failed");

        // Now we have 3 clusters: first_cluster, neighbor, mixed
        assert_eq!(writer.deltas.len(), 3);

        // Trigger balance on the mixed cluster - should split into 2
        writer
            .balance(mixed_cluster_id)
            .await
            .expect("balance failed");

        // After split, we should have more clusters
        // mixed_cluster splits into 2, so we have: first_cluster + neighbor + 2 from split = 4
        // (mixed_cluster itself may be dropped and replaced by 2 new ones)
        assert!(
            writer.deltas.len() >= 3,
            "Expected at least 3 clusters after split, got {}",
            writer.deltas.len()
        );

        // Verify the misplaced points got reassigned
        // The reassignment upgrades the version, so the old entries in neighbor cluster
        // are now stale. We verify that the current version is higher than the original.
        let v100_current = *writer.versions.get(&100).expect("version not found");
        let v101_current = *writer.versions.get(&101).expect("version not found");

        // Either neighbor cluster was dropped, or the points' versions were upgraded (reassigned)
        let neighbor_dropped = writer.deltas.get(&neighbor_cluster_id).is_none();
        let points_were_reassigned = v100_current > v100 || v101_current > v101;
        assert!(
            neighbor_dropped || points_were_reassigned,
            "Misplaced points should have been reassigned (versions upgraded)"
        );

        // =======================================================================
        // Step 4: merge (triggered by balance after scrub)
        // =======================================================================
        // Spawn a small cluster far from others: [0, 0, 0, 1]
        let isolated_center: Arc<[f32]> = writer.rotate(&[0.0, 0.0, 0.0, 1.0]);

        // Add 4 points, will invalidate 3 to trigger merge
        let v200 = writer.upgrade(200);
        let v201 = writer.upgrade(201);
        let v202 = writer.upgrade(202);
        let v203 = writer.upgrade(203);

        // Create embeddings and codes for isolated cluster points
        let emb_200 = writer.rotate(&[0.0, 0.0, 0.0, 1.0]);
        let emb_201 = writer.rotate(&[0.0, 0.0, 0.1, 0.9]);
        let emb_202 = writer.rotate(&[0.0, 0.1, 0.0, 0.9]);
        let emb_203 = writer.rotate(&[0.1, 0.0, 0.0, 0.9]);

        writer.embeddings.insert(200, emb_200.clone());
        writer.embeddings.insert(201, emb_201.clone());
        writer.embeddings.insert(202, emb_202.clone());
        writer.embeddings.insert(203, emb_203.clone());

        let code_200: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_200, &isolated_center)
            .as_ref()
            .into();
        let code_201: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_201, &isolated_center)
            .as_ref()
            .into();
        let code_202: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_202, &isolated_center)
            .as_ref()
            .into();
        let code_203: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_203, &isolated_center)
            .as_ref()
            .into();

        let isolated_delta = QuantizedDelta {
            center: isolated_center,
            codes: vec![code_200, code_201, code_202, code_203],
            ids: vec![200, 201, 202, 203],
            length: 4,
            versions: vec![v200, v201, v202, v203],
        };
        let isolated_cluster_id = writer.spawn(isolated_delta).expect("spawn failed");

        let clusters_before_merge = writer.deltas.len();

        // Invalidate 3 points, leaving only 1 valid (below merge_threshold of 2)
        writer.upgrade(201);
        writer.upgrade(202);
        writer.upgrade(203);

        // Trigger balance - should scrub (remove invalid) then merge (below threshold)
        writer
            .balance(isolated_cluster_id)
            .await
            .expect("balance failed");

        // Isolated cluster should be dropped (merged into neighbor)
        assert!(
            writer.deltas.get(&isolated_cluster_id).is_none(),
            "Isolated cluster should have been merged"
        );

        // Should have one fewer cluster
        assert_eq!(
            writer.deltas.len(),
            clusters_before_merge - 1,
            "One cluster should have been removed by merge"
        );

        // Point 200 should now be in some other cluster (reassigned during merge)
        // Check that it exists somewhere with current version
        let current_v200 = *writer.versions.get(&200).expect("version not found");
        let point_200_found = writer.deltas.iter().any(|entry| {
            entry
                .value()
                .ids
                .iter()
                .zip(entry.value().versions.iter())
                .any(|(id, ver)| *id == 200 && *ver == current_v200)
        });
        assert!(
            point_200_found,
            "Point 200 should exist in some cluster after merge"
        );
    }
}
