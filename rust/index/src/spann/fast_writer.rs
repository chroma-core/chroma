use std::{
    collections::HashSet,
    hash::RandomState,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc,
    },
};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileReader,
    BlockfileWriter, BlockfileWriterOptions,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_tracing::util::Stopwatch;
use chroma_types::{Cmek, CollectionUuid, InternalSpannConfiguration, SpannPostingList};
use dashmap::DashMap;
use opentelemetry::global;
use rand::seq::SliceRandom;
use uuid::Uuid;

use crate::{
    hnsw_provider::{HnswIndexFlusher, HnswIndexProvider, HnswIndexRef},
    spann::types::{
        GarbageCollectionContext, HnswGarbageCollectionPolicy, PlGarbageCollectionPolicy,
        SpannIndexFlusher, SpannIndexFlusherMetrics, SpannIndexWriterError,
    },
    Index, IndexUuid,
};

use super::utils::{cluster, KMeansAlgorithmInput, KMeansError};

#[derive(Clone, Debug)]
struct WriteStats {
    num_pl_modified: Arc<AtomicU32>,
    num_heads_created: Arc<AtomicU32>,
    num_heads_deleted: Arc<AtomicU32>,
    num_reassigns: Arc<AtomicU32>,
    num_splits: Arc<AtomicU32>,
    num_merges: Arc<AtomicU32>,
    num_reassigns_split_point: Arc<AtomicU32>,
    num_reassigns_nbrs: Arc<AtomicU32>,
    num_reassigns_merged_point: Arc<AtomicU32>,
    num_centers_fetched_rng: Arc<AtomicU64>,
    num_rng_calls: Arc<AtomicU32>,
}

impl Default for WriteStats {
    fn default() -> Self {
        Self {
            num_pl_modified: Arc::new(AtomicU32::new(0)),
            num_heads_created: Arc::new(AtomicU32::new(0)),
            num_heads_deleted: Arc::new(AtomicU32::new(0)),
            num_reassigns: Arc::new(AtomicU32::new(0)),
            num_splits: Arc::new(AtomicU32::new(0)),
            num_merges: Arc::new(AtomicU32::new(0)),
            num_reassigns_split_point: Arc::new(AtomicU32::new(0)),
            num_reassigns_nbrs: Arc::new(AtomicU32::new(0)),
            num_reassigns_merged_point: Arc::new(AtomicU32::new(0)),
            num_centers_fetched_rng: Arc::new(AtomicU64::new(0)),
            num_rng_calls: Arc::new(AtomicU32::new(0)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpannMetrics {
    pub num_pl_modified: opentelemetry::metrics::Counter<u64>,
    pub num_heads_created: opentelemetry::metrics::Counter<u64>,
    pub num_heads_deleted: opentelemetry::metrics::Counter<u64>,
    pub num_reassigns: opentelemetry::metrics::Counter<u64>,
    pub num_splits: opentelemetry::metrics::Counter<u64>,
    pub num_reassigns_split_point: opentelemetry::metrics::Counter<u64>,
    pub num_reassigns_nbrs: opentelemetry::metrics::Counter<u64>,
    pub num_reassigns_merged_point: opentelemetry::metrics::Counter<u64>,
    pub num_centers_fetched_rng: opentelemetry::metrics::Counter<u64>,
    pub num_rng_calls: opentelemetry::metrics::Counter<u64>,
    pub gc_latency: opentelemetry::metrics::Histogram<u64>,
    pub pl_commit_latency: opentelemetry::metrics::Histogram<u64>,
    pub versions_map_commit_latency: opentelemetry::metrics::Histogram<u64>,
    pub hnsw_commit_latency: opentelemetry::metrics::Histogram<u64>,
    pub pl_flush_latency: opentelemetry::metrics::Histogram<u64>,
    pub versions_map_flush_latency: opentelemetry::metrics::Histogram<u64>,
    pub hnsw_flush_latency: opentelemetry::metrics::Histogram<u64>,
    pub num_pl_entries_flushed: opentelemetry::metrics::Counter<u64>,
    pub num_versions_map_entries_flushed: opentelemetry::metrics::Counter<u64>,
}

impl Default for SpannMetrics {
    fn default() -> Self {
        let meter = global::meter("chroma");
        let num_pl_modified = meter.u64_counter("num_pl_modified").build();
        let num_heads_created = meter.u64_counter("num_heads_created").build();
        let num_heads_deleted = meter.u64_counter("num_heads_deleted").build();
        let num_reassigns = meter.u64_counter("num_reassigns").build();
        let num_splits = meter.u64_counter("num_splits").build();
        let num_reassigns_split_point = meter.u64_counter("num_reassigns_split_point").build();
        let num_reassigns_nbrs = meter.u64_counter("num_reassigns_nbrs").build();
        let num_reassigns_merged_point = meter.u64_counter("num_reassigns_merged_point").build();
        let num_centers_fetched_rng = meter.u64_counter("num_centers_fetched_rng").build();
        let num_rng_calls = meter.u64_counter("num_rng_calls").build();
        let gc_latency = meter.u64_histogram("gc_latency").build();
        let pl_commit_latency = meter.u64_histogram("pl_commit_latency").build();
        let versions_map_commit_latency =
            meter.u64_histogram("versions_map_commit_latency").build();
        let hnsw_commit_latency = meter.u64_histogram("hnsw_commit_latency").build();
        let pl_flush_latency = meter.u64_histogram("pl_flush_latency").build();
        let versions_map_flush_latency = meter.u64_histogram("versions_map_flush_latency").build();
        let hnsw_flush_latency = meter.u64_histogram("hnsw_flush_latency").build();
        let num_pl_entries_flushed = meter.u64_counter("num_pl_blocks_flushed").build();
        let num_versions_map_entries_flushed =
            meter.u64_counter("num_versions_map_blocks_flushed").build();
        Self {
            num_pl_modified,
            num_heads_created,
            num_heads_deleted,
            num_reassigns,
            num_splits,
            num_reassigns_split_point,
            num_reassigns_nbrs,
            num_reassigns_merged_point,
            num_centers_fetched_rng,
            num_rng_calls,
            gc_latency,
            pl_commit_latency,
            versions_map_commit_latency,
            hnsw_commit_latency,
            pl_flush_latency,
            versions_map_flush_latency,
            hnsw_flush_latency,
            num_pl_entries_flushed,
            num_versions_map_entries_flushed,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpannPostingListOwned {
    ids: Vec<u32>,
    versions: Vec<u32>,
    embeddings: Vec<Arc<[f32]>>,
}

#[derive(Clone, Debug)]
pub struct HeadData {
    pub centroid: Arc<[f32]>,
    pub posting_list: SpannPostingListOwned,
    pub length: u32,
}

#[derive(Clone)]
// Note: Fields of this struct are public for testing.
pub struct FastSpannIndexWriter {
    // HNSW index and its provider for centroid search.
    pub hnsw_index: HnswIndexRef,
    pub cleaned_up_hnsw_index: Option<HnswIndexRef>,
    hnsw_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    pub posting_list_writer: BlockfileWriter,
    pub next_head_id: Arc<AtomicU32>,
    pub dimensionality: usize,
    pub params: InternalSpannConfiguration,
    pub gc_context: GarbageCollectionContext,
    pub collection_id: CollectionUuid,
    pub prefix_path: String,
    metrics: SpannMetrics,
    stats: WriteStats,
    cmek: Option<Cmek>,
    pub embeddings: Arc<DashMap<u32, Arc<[f32]>, RandomState>>,
    pub heads: Arc<DashMap<u32, HeadData, RandomState>>,
    pub versions: Arc<DashMap<u32, u32, RandomState>>,
    pub posting_list_reader: Option<BlockfileReader<'static, u32, SpannPostingList<'static>>>,
    pub deleted_heads: Arc<DashMap<u32, (), RandomState>>,
}

const MAX_HEAD_OFFSET_ID: &str = "max_head_offset_id";

#[derive(Clone, Debug)]
enum ReassignReason {
    Split,
    Nearby,
    Merge,
}

/// Data for a cluster produced by a split operation, used for reassignment.
pub(crate) struct SplitClusterData {
    pub(crate) head_id: u32,
    pub(crate) doc_offset_ids: Vec<u32>,
    pub(crate) doc_versions: Vec<u32>,
    pub(crate) embeddings: Vec<Arc<[f32]>>,
    pub(crate) centroid: Arc<[f32]>,
}

/// Result of finding nearby heads from HNSW index.
struct NearbyHeadsResult {
    ids: Vec<usize>,
    distances: Vec<f32>,
    embeddings: Vec<Arc<[f32]>>,
}

impl FastSpannIndexWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hnsw_index: HnswIndexRef,
        hnsw_provider: HnswIndexProvider,
        blockfile_provider: BlockfileProvider,
        posting_list_writer: BlockfileWriter,
        next_head_id: u32,
        dimensionality: usize,
        params: InternalSpannConfiguration,
        gc_context: GarbageCollectionContext,
        collection_id: CollectionUuid,
        metrics: SpannMetrics,
        prefix_path: String,
        cmek: Option<Cmek>,
        heads: DashMap<u32, HeadData, RandomState>,
        versions: DashMap<u32, u32, RandomState>,
        posting_list_reader: Option<BlockfileReader<'static, u32, SpannPostingList<'static>>>,
    ) -> Self {
        FastSpannIndexWriter {
            hnsw_index,
            cleaned_up_hnsw_index: None,
            hnsw_provider,
            blockfile_provider,
            posting_list_writer,
            next_head_id: Arc::new(AtomicU32::new(next_head_id)),
            dimensionality,
            params,
            gc_context,
            collection_id,
            metrics,
            stats: WriteStats::default(),
            prefix_path,
            cmek,
            embeddings: Arc::new(DashMap::new()),
            heads: Arc::new(heads),
            versions: Arc::new(versions),
            posting_list_reader,
            deleted_heads: Arc::new(DashMap::new()),
        }
    }

    async fn hnsw_index_from_id(
        hnsw_provider: &HnswIndexProvider,
        id: &IndexUuid,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<HnswIndexRef, SpannIndexWriterError> {
        match hnsw_provider
            .fork(
                id,
                collection_id,
                dimensionality as i32,
                distance_function,
                ef_search,
                prefix_path,
            )
            .await
        {
            Ok(index) => Ok(index),
            Err(e) => {
                tracing::error!(
                    "Error forking hnsw index from id {:?} for collection {:?}: {:?}",
                    id,
                    collection_id,
                    e
                );
                Err(SpannIndexWriterError::HnswIndexForkError(*e))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_hnsw_index(
        hnsw_provider: &HnswIndexProvider,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<HnswIndexRef, SpannIndexWriterError> {
        match hnsw_provider
            .create(
                collection_id,
                m,
                ef_construction,
                ef_search,
                dimensionality as i32,
                distance_function,
                prefix_path,
            )
            .await
        {
            Ok(index) => Ok(index),
            Err(e) => {
                tracing::error!(
                    "Error creating hnsw index for collection {:?}: {:?}",
                    collection_id,
                    e
                );
                Err(SpannIndexWriterError::HnswIndexCreateError(*e))
            }
        }
    }

    async fn load_versions_map_and_cluster_data(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
        prefix_path: &str,
        hnsw_index: &HnswIndexRef,
    ) -> Result<(DashMap<u32, u32>, DashMap<u32, HeadData>), SpannIndexWriterError> {
        let reader_options = BlockfileReaderOptions::new(*blockfile_id, prefix_path.to_string());
        let reader = match blockfile_provider.read::<u32, u32>(reader_options).await {
            Ok(reader) => reader,
            Err(e) => {
                tracing::error!(
                    "Error creating reader for versions map blockfile {:?}: {:?}",
                    blockfile_id,
                    e
                );
                return Err(SpannIndexWriterError::VersionsMapReaderCreateError(*e));
            }
        };
        let versions_data = reader.get_range(.., ..).await.map_err(|e| {
            tracing::error!(
                "Error performing get_range for versions map blockfile {:?}: {:?}",
                blockfile_id,
                e
            );
            SpannIndexWriterError::VersionsMapDataLoadError(e)
        })?;
        let versions = DashMap::default();
        let heads = DashMap::default();
        let read_guard = hnsw_index.inner.read();
        for (prefix, key, value) in versions_data {
            // Empty prefix means its a point.
            // Non-empty prefix means its a head.
            if prefix.is_empty() {
                versions.insert(key, value);
            } else {
                let embedding = read_guard
                    .hnsw_index
                    .get(key as usize)
                    .map_err(|e| {
                        tracing::error!(
                            "Error getting embedding from hnsw index for id {}: {}",
                            key,
                            e
                        );
                        SpannIndexWriterError::HnswIndexSearchError(e)
                    })?
                    .ok_or_else(|| {
                        tracing::error!("Embedding not found in hnsw index for id {}", key);
                        SpannIndexWriterError::HeadNotFound
                    })?;

                heads.insert(
                    key,
                    HeadData {
                        centroid: Arc::from(embedding),
                        posting_list: SpannPostingListOwned {
                            ids: vec![],
                            versions: vec![],
                            embeddings: vec![],
                        },
                        length: value, // Total count from persisted data
                    },
                );
            }
        }
        Ok((versions, heads))
    }

    async fn fork_postings_list(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
        prefix_path: &str,
        cmek: Option<Cmek>,
    ) -> Result<
        (
            BlockfileWriter,
            BlockfileReader<'static, u32, SpannPostingList<'static>>,
        ),
        SpannIndexWriterError,
    > {
        let mut bf_options = BlockfileWriterOptions::new(prefix_path.to_string());
        bf_options = bf_options.unordered_mutations();
        bf_options = bf_options.fork(*blockfile_id);
        if let Some(cmek) = cmek {
            bf_options = bf_options.with_cmek(cmek);
        }
        let writer = match blockfile_provider
            .write::<u32, &SpannPostingList<'_>>(bf_options)
            .await
        {
            Ok(writer) => writer,
            Err(e) => {
                tracing::error!(
                    "Error forking postings list writer from blockfile {:?}: {:?}",
                    blockfile_id,
                    e
                );
                return Err(SpannIndexWriterError::PostingsListWriterCreateError(*e));
            }
        };
        let reader_options = BlockfileReaderOptions::new(*blockfile_id, prefix_path.to_string());
        let reader: BlockfileReader<'static, u32, SpannPostingList<'static>> =
            match blockfile_provider
                .read::<u32, SpannPostingList<'static>>(reader_options)
                .await
            {
                Ok(reader) => reader,
                Err(e) => {
                    tracing::error!("Error creating postings list reader: {:?}", e);
                    return Err(SpannIndexWriterError::PostingListGetError(Box::new(*e)));
                }
            };
        Ok((writer, reader))
    }

    async fn create_posting_list(
        blockfile_provider: &BlockfileProvider,
        prefix_path: &str,
        pl_block_size: usize,
        cmek: Option<Cmek>,
    ) -> Result<BlockfileWriter, SpannIndexWriterError> {
        let mut bf_options = BlockfileWriterOptions::new(prefix_path.to_string())
            .max_block_size_bytes(pl_block_size);
        bf_options = bf_options.unordered_mutations();
        if let Some(cmek) = cmek {
            bf_options = bf_options.with_cmek(cmek);
        }
        match blockfile_provider
            .write::<u32, &SpannPostingList<'_>>(bf_options)
            .await
        {
            Ok(writer) => Ok(writer),
            Err(e) => {
                tracing::error!("Error creating postings list writer: {:?}", e);
                Err(SpannIndexWriterError::PostingsListWriterCreateError(*e))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_id(
        hnsw_provider: &HnswIndexProvider,
        hnsw_id: Option<&IndexUuid>,
        versions_map_id: Option<&Uuid>,
        posting_list_id: Option<&Uuid>,
        max_head_id_bf_id: Option<&Uuid>,
        collection_id: &CollectionUuid,
        prefix_path: &str,
        dimensionality: usize,
        blockfile_provider: &BlockfileProvider,
        params: InternalSpannConfiguration,
        gc_context: GarbageCollectionContext,
        pl_block_size: usize,
        metrics: SpannMetrics,
        cmek: Option<Cmek>,
    ) -> Result<Self, SpannIndexWriterError> {
        let distance_function = DistanceFunction::from(params.space.clone());
        // Create the HNSW index.
        let hnsw_index = match hnsw_id {
            Some(hnsw_id) => {
                Self::hnsw_index_from_id(
                    hnsw_provider,
                    hnsw_id,
                    collection_id,
                    distance_function.clone(),
                    dimensionality,
                    params.ef_search,
                    prefix_path,
                )
                .await?
            }
            None => {
                Self::create_hnsw_index(
                    hnsw_provider,
                    collection_id,
                    distance_function.clone(),
                    dimensionality,
                    params.max_neighbors,
                    params.ef_construction,
                    params.ef_search,
                    prefix_path,
                )
                .await?
            }
        };

        // Load the versions map and heads.
        let (versions, heads) = match versions_map_id {
            Some(versions_map_id) => {
                Self::load_versions_map_and_cluster_data(
                    versions_map_id,
                    blockfile_provider,
                    prefix_path,
                    &hnsw_index,
                )
                .await?
            }
            None => (DashMap::default(), DashMap::default()),
        };

        // Fork the posting list writer. Also, get an instance of the reader.
        let (posting_list_writer, posting_list_reader) = match posting_list_id {
            Some(posting_list_id) => {
                let (writer, reader) = Self::fork_postings_list(
                    posting_list_id,
                    blockfile_provider,
                    prefix_path,
                    cmek.clone(),
                )
                .await?;
                (writer, Some(reader))
            }
            None => (
                Self::create_posting_list(
                    blockfile_provider,
                    prefix_path,
                    pl_block_size,
                    cmek.clone(),
                )
                .await?,
                None,
            ),
        };

        let max_head_id = match max_head_id_bf_id {
            Some(max_head_id_bf_id) => {
                let reader_options =
                    BlockfileReaderOptions::new(*max_head_id_bf_id, prefix_path.to_string());
                let reader = blockfile_provider.read::<&str, u32>(reader_options).await;
                match reader {
                    Ok(reader) => reader
                        .get("", MAX_HEAD_OFFSET_ID)
                        .await
                        .map_err(|e| {
                            tracing::error!("Error reading max offset id for heads: {:?}", e);
                            SpannIndexWriterError::MaxHeadIdBlockfileGetError(e)
                        })?
                        .ok_or(SpannIndexWriterError::MaxHeadIdNotFound)?,
                    Err(_) => 1,
                }
            }
            None => 1,
        };
        Ok(Self::new(
            hnsw_index,
            hnsw_provider.clone(),
            blockfile_provider.clone(),
            posting_list_writer,
            max_head_id,
            dimensionality,
            params,
            gc_context,
            *collection_id,
            metrics,
            prefix_path.to_string(),
            cmek,
            heads,
            versions,
            posting_list_reader,
        ))
    }

    async fn add_versions_map(&self, id: u32) -> u32 {
        // 0 means deleted. Version counting starts from 1.
        self.versions.insert(id, 1);
        1
    }

    async fn rng_query(
        &self,
        query: &[f32],
    ) -> Result<(Vec<usize>, Vec<f32>, Vec<Arc<[f32]>>), SpannIndexWriterError> {
        // Assumes that query is already normalized.
        let k: usize = self.params.write_nprobe as usize;
        let replica_count = self.params.nreplica_count as usize;
        let rng_epsilon = self.params.write_rng_epsilon;
        let rng_factor = self.params.write_rng_factor;
        let distance_function: DistanceFunction = self.params.space.clone().into();
        let mut nearby_ids: Vec<usize> = Vec::with_capacity(k);
        let mut nearby_distances: Vec<f32> = Vec::with_capacity(k);
        let mut embeddings: Vec<Arc<[f32]>> = Vec::with_capacity(k);
        {
            let read_guard = self.hnsw_index.inner.read();
            let (ids, distances) =
                read_guard
                    .hnsw_index
                    .query(query, k, &[], &[])
                    .map_err(|e| {
                        tracing::error!("Error querying hnsw index: {:?}", e);
                        SpannIndexWriterError::HnswIndexSearchError(e)
                    })?;
            for (id, distance) in ids.iter().zip(distances.iter()) {
                let within_epsilon = if distances[0] < 0.0 && *distance < 0.0 {
                    // Both negative: reverse the comparison
                    *distance >= (1_f32 + rng_epsilon) * distances[0]
                } else {
                    // At least one is non-negative: use normal comparison
                    *distance <= (1_f32 + rng_epsilon) * distances[0]
                };

                if within_epsilon {
                    nearby_ids.push(*id);
                    nearby_distances.push(*distance);
                }
            }
        }
        let mut nearby_ids2: Vec<usize> = Vec::with_capacity(k);
        let mut nearby_distances2: Vec<f32> = Vec::with_capacity(k);
        // Get the embeddings also for distance computation.
        for (id, distance) in nearby_ids.into_iter().zip(nearby_distances.into_iter()) {
            // Skip concurrently deleted heads.
            let Some(head_data) = self.heads.get(&(id as u32)) else {
                continue;
            };
            embeddings.push(head_data.centroid.clone());
            nearby_ids2.push(id);
            nearby_distances2.push(distance);
        }
        // Apply the RNG rule to prune.
        let mut res_ids = Vec::with_capacity(replica_count);
        let mut res_distances = Vec::with_capacity(replica_count);
        let mut res_embeddings: Vec<Arc<[f32]>> = Vec::with_capacity(replica_count);
        // Embeddings that were obtained are already normalized.
        for (id, (distance, embedding)) in nearby_ids2
            .into_iter()
            .zip(nearby_distances2.into_iter().zip(embeddings.into_iter()))
        {
            if res_ids.len() >= replica_count {
                break;
            }
            let mut rng_accepted = true;
            for nbr_embedding in res_embeddings.iter() {
                let dist = distance_function.distance(&embedding[..], &nbr_embedding[..]);

                let fails_check = if dist < 0.0 && distance < 0.0 {
                    // Both negative: reverse the comparison
                    rng_factor * dist >= distance
                } else {
                    // At least one is non-negative: use normal comparison
                    rng_factor * dist <= distance
                };

                if fails_check {
                    rng_accepted = false;
                    break;
                }
            }
            if !rng_accepted {
                continue;
            }
            res_ids.push(id);
            res_distances.push(distance);
            res_embeddings.push(embedding);
        }
        self.stats
            .num_rng_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .num_centers_fetched_rng
            .fetch_add(res_ids.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok((res_ids, res_distances, res_embeddings))
    }

    fn is_deleted(version: u32) -> bool {
        // Version 0 means deleted.
        version == 0
    }

    fn is_outdated(&self, doc_offset_id: u32, version: u32) -> Result<bool, SpannIndexWriterError> {
        let current_version = self
            .versions
            .get(&doc_offset_id)
            .ok_or(SpannIndexWriterError::VersionNotFound)?;
        if Self::is_deleted(*current_version) || version < *current_version {
            return Ok(true);
        }
        Ok(false)
    }

    /// Deletes a posting list if all its points are outdated or deleted.
    async fn try_delete_posting_list(&self, head_id: u32) -> Result<(), SpannIndexWriterError> {
        // Check if all points in the posting list are outdated
        let should_delete = {
            let Some(head_data) = self.heads.get(&head_id) else {
                return Ok(()); // Already deleted
            };

            if head_data.posting_list.ids.is_empty() {
                false // Don't delete empty posting lists (they may be newly created)
            } else {
                let mut outdated_count = 0;
                for (doc_id, version) in head_data
                    .posting_list
                    .ids
                    .iter()
                    .zip(head_data.posting_list.versions.iter())
                {
                    if self.is_outdated(*doc_id, *version)? {
                        outdated_count += 1;
                    }
                }
                outdated_count == head_data.posting_list.ids.len()
            }
        };

        if should_delete {
            // Remove from heads DashMap
            self.heads.remove(&head_id);

            // Delete from HNSW
            {
                let hnsw_write_guard = self.hnsw_index.inner.write();
                hnsw_write_guard
                    .hnsw_index
                    .delete(head_id as usize)
                    .map_err(|e| {
                        tracing::error!("Error deleting head {} from hnsw index: {}", head_id, e);
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
            }

            // Track as deleted
            self.deleted_heads.insert(head_id, ());
        }

        Ok(())
    }

    async fn collect_and_reassign_split_points(
        &self,
        split_data: &[SplitClusterData],
        old_head_embedding: &[f32],
    ) -> Result<HashSet<u32>, SpannIndexWriterError> {
        let mut assigned_ids = HashSet::new();
        let distance_function: DistanceFunction = self.params.space.clone().into();

        for cluster in split_data.iter() {
            for (index, doc_offset_id) in cluster.doc_offset_ids.iter().enumerate() {
                if assigned_ids.contains(doc_offset_id)
                    || self.is_outdated(*doc_offset_id, cluster.doc_versions[index])?
                {
                    continue;
                }

                let doc_embedding = &cluster.embeddings[index];
                let old_dist = distance_function.distance(old_head_embedding, doc_embedding);
                let new_dist = distance_function.distance(&cluster.centroid, doc_embedding);

                // NPA check.
                if new_dist > old_dist {
                    assigned_ids.insert(*doc_offset_id);
                    self.reassign(
                        *doc_offset_id,
                        cluster.doc_versions[index],
                        doc_embedding.clone(),
                        cluster.head_id,
                        ReassignReason::Split,
                    )
                    .await?;
                }
            }
            // Delete head if all points were moved out.
            self.try_delete_posting_list(cluster.head_id).await?;
        }
        Ok(assigned_ids)
    }

    fn get_nearby_heads(
        &self,
        head_embedding: &[f32],
        k: usize,
    ) -> Result<NearbyHeadsResult, SpannIndexWriterError> {
        let read_guard = self.hnsw_index.inner.read();
        let (nearest_ids, nearest_distances) = read_guard
            .hnsw_index
            .query(head_embedding, k, &[], &[])
            .map_err(|e| {
                tracing::error!("Error querying hnsw for {:?}: {:?}", head_embedding, e);
                SpannIndexWriterError::HnswIndexSearchError(e)
            })?;
        drop(read_guard); // Release HNSW lock before accessing centroids

        // Get the embeddings from heads DashMap.
        // Filter out heads that are too far from the nearest head.
        const MAX_DIST_RATIO: f32 = 2.0;
        let limit_dist = nearest_distances.first().map(|d| d * MAX_DIST_RATIO);

        let mut result = NearbyHeadsResult {
            ids: Vec::with_capacity(k),
            distances: Vec::with_capacity(k),
            embeddings: Vec::with_capacity(k),
        };
        for (id, distance) in nearest_ids.into_iter().zip(nearest_distances.into_iter()) {
            // Skip heads that are too far from the nearest head.
            if let Some(limit) = limit_dist {
                if distance > limit {
                    continue;
                }
            }
            let Some(head_data) = self.heads.get(&(id as u32)) else {
                continue;
            };
            result.ids.push(id);
            result.distances.push(distance);
            result.embeddings.push(Arc::clone(&head_data.centroid));
        }
        Ok(result)
    }

    async fn reassign(
        &self,
        doc_offset_id: u32,
        doc_version: u32,
        doc_embedding: Arc<[f32]>,
        prev_head_id: u32,
        reason: ReassignReason,
    ) -> Result<(), SpannIndexWriterError> {
        // Don't reassign if outdated by now.
        if self.is_outdated(doc_offset_id, doc_version)? {
            return Ok(());
        }
        // RNG query to find the nearest heads.
        let (nearest_head_ids, _, nearest_head_embeddings) = self.rng_query(&doc_embedding).await?;
        // Don't reassign if empty.
        if nearest_head_ids.is_empty() {
            return Ok(());
        }
        // If nearest_head_ids contain the previous_head_id then don't reassign.
        let prev_head_id = prev_head_id as usize;
        if nearest_head_ids.contains(&prev_head_id) {
            return Ok(());
        }
        // Increment version and trigger append.
        let next_version = {
            let mut version_guard = self
                .versions
                .get_mut(&doc_offset_id)
                .ok_or(SpannIndexWriterError::VersionNotFound)?;
            let current_version = *version_guard;
            if Self::is_deleted(current_version) || doc_version < current_version {
                return Ok(());
            }
            let next_ver = current_version + 1;
            *version_guard = next_ver;
            next_ver
        }; // guard dropped here

        // Append to the posting list.
        for (nearest_head_id, nearest_head_embedding) in nearest_head_ids
            .into_iter()
            .zip(nearest_head_embeddings.into_iter())
        {
            if self.is_outdated(doc_offset_id, next_version)? {
                return Ok(());
            }
            self.stats
                .num_reassigns
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match reason {
                ReassignReason::Split => {
                    self.stats
                        .num_reassigns_split_point
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                ReassignReason::Nearby => {
                    self.stats
                        .num_reassigns_nbrs
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                ReassignReason::Merge => {
                    self.stats
                        .num_reassigns_merged_point
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            self.append(
                nearest_head_id as u32,
                doc_offset_id,
                next_version,
                doc_embedding.clone(),
                nearest_head_embedding,
            )
            .await?;
        }
        Ok(())
    }

    /// Reconciles staged posting list with reader data.
    async fn reconcile_posting_list(&self, head_id: u32) -> Result<(), SpannIndexWriterError> {
        let (length, pl_len) = {
            // Concurrently deleted.
            let Some(head_data) = self.heads.get(&head_id) else {
                return Ok(());
            };
            (head_data.length as usize, head_data.posting_list.ids.len())
        }; // guard dropped here

        if pl_len == length {
            return Ok(());
        }

        // If reader has more data, read it first, then extend staged
        if let Some(reader) = self.posting_list_reader.as_ref() {
            if let Some(pl) = reader.get("", head_id).await.map_err(|e| {
                tracing::error!("Error getting posting list for head {}: {}", head_id, e);
                SpannIndexWriterError::PostingListGetError(e)
            })? {
                // Now take mut ref and extend.
                if let Some(mut head_data) = self.heads.get_mut(&head_id) {
                    // Check again - another thread may have already extended
                    if head_data.posting_list.ids.len() != head_data.length as usize {
                        head_data
                            .posting_list
                            .ids
                            .extend(pl.doc_offset_ids.iter().cloned());
                        head_data
                            .posting_list
                            .versions
                            .extend(pl.doc_versions.iter().cloned());
                        for idx in 0..pl.doc_offset_ids.len() {
                            if let Some(embedding) = self.embeddings.get(&pl.doc_offset_ids[idx]) {
                                head_data.posting_list.embeddings.push(embedding.clone());
                            } else {
                                let em: Arc<[f32]> = Arc::from(
                                    &pl.doc_embeddings[idx * self.dimensionality
                                        ..(idx + 1) * self.dimensionality],
                                );
                                self.embeddings.insert(pl.doc_offset_ids[idx], em.clone());
                                head_data.posting_list.embeddings.push(em);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn collect_and_reassign_nearby_points(
        &self,
        head_id: usize,
        head_embedding: &[f32],
        assigned_ids: &mut HashSet<u32>,
        split_data: &[SplitClusterData],
        old_head_embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        // Reconcile staged data with reader data
        let head_id_u32 = head_id as u32;
        self.reconcile_posting_list(head_id_u32).await?;

        // Clone for iteration (needed because of await in loop)
        let Some((ids, versions, embeddings)) = self.heads.get(&head_id_u32).map(|h| {
            (
                h.posting_list.ids.clone(),
                h.posting_list.versions.clone(),
                h.posting_list.embeddings.clone(),
            )
        }) else {
            return Ok(());
        };

        let distance_function: DistanceFunction = self.params.space.clone().into();

        for (index, doc_offset_id) in ids.iter().enumerate() {
            if assigned_ids.contains(doc_offset_id)
                || self.is_outdated(*doc_offset_id, versions[index])?
            {
                continue;
            }
            let doc_embedding = &embeddings[index];
            let distance_from_curr_center =
                distance_function.distance(doc_embedding, head_embedding);
            let distance_from_split_center1 =
                distance_function.distance(doc_embedding, &split_data[0].centroid);
            let distance_from_split_center2 =
                distance_function.distance(doc_embedding, &split_data[1].centroid);
            if distance_from_curr_center <= distance_from_split_center1
                && distance_from_curr_center <= distance_from_split_center2
            {
                continue;
            }
            let distance_from_old_head =
                distance_function.distance(doc_embedding, old_head_embedding);
            if distance_from_old_head <= distance_from_split_center1
                && distance_from_old_head <= distance_from_split_center2
            {
                continue;
            }
            // Candidate for reassignment.
            assigned_ids.insert(*doc_offset_id);
            self.reassign(
                *doc_offset_id,
                versions[index],
                doc_embedding.clone(),
                head_id_u32,
                ReassignReason::Nearby,
            )
            .await?;
        }

        // Delete head if all points were moved out.
        self.try_delete_posting_list(head_id_u32).await?;
        Ok(())
    }

    async fn collect_and_reassign(
        &self,
        split_data: &[SplitClusterData],
        old_head_embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        let mut assigned_ids = self
            .collect_and_reassign_split_points(split_data, old_head_embedding)
            .await?;
        // Reassign neighbors of this center if applicable.
        if self.params.reassign_neighbor_count > 0 {
            let nearby_heads = self.get_nearby_heads(
                old_head_embedding,
                self.params.reassign_neighbor_count as usize,
            )?;
            for (head_idx, head_id) in nearby_heads.ids.iter().enumerate() {
                // Skip the current split heads.
                if split_data
                    .iter()
                    .any(|cluster| cluster.head_id == *head_id as u32)
                {
                    continue;
                }
                self.collect_and_reassign_nearby_points(
                    *head_id,
                    &nearby_heads.embeddings[head_idx],
                    &mut assigned_ids,
                    split_data,
                    old_head_embedding,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Appends a point to a posting list, triggers scrub if over threshold.
    async fn append(
        &self,
        head_id: u32,
        id: u32,
        version: u32,
        embedding: Arc<[f32]>,
        head_embedding: Arc<[f32]>,
    ) -> Result<(), SpannIndexWriterError> {
        let Some(current_length) = self.insert_to_posting_list(head_id, id, version, embedding)
        else {
            // TODO(Sanket): Should ideally reassign here.
            return Ok(());
        };

        if current_length <= self.params.split_threshold {
            return Ok(());
        }

        // Over threshold - need to scrub and potentially split
        self.scrub_posting_list(head_id, head_embedding).await
    }

    /// Adds a point to the posting list, returns the new length.
    /// Returns None if head not found.
    fn insert_to_posting_list(
        &self,
        head_id: u32,
        id: u32,
        version: u32,
        embedding: Arc<[f32]>,
    ) -> Option<u32> {
        let mut head_data = self.heads.get_mut(&head_id)?;
        head_data.length += 1;
        head_data.posting_list.ids.push(id);
        head_data.posting_list.versions.push(version);
        head_data.posting_list.embeddings.push(embedding);
        Some(head_data.length)
    }

    /// Tries to merge a small posting list into a nearby head.
    /// Returns true if merge succeeded, false if no suitable target found.
    async fn try_merge_posting_list(
        &self,
        source_head_id: u32,
        source_centroid: Arc<[f32]>,
    ) -> Result<bool, SpannIndexWriterError> {
        // Atomically claim source by removing it from DashMap
        let Some((_, source_data)) = self.heads.remove(&source_head_id) else {
            return Ok(false); // Already gone
        };

        // Find nearby heads as merge candidates
        let nearby = self.get_nearby_heads(
            &source_centroid,
            self.params.num_centers_to_merge_to as usize,
        )?;

        let distance_function: DistanceFunction = self.params.space.clone().into();

        for (idx, &target_id) in nearby.ids.iter().enumerate() {
            let target_id_u32 = target_id as u32;

            // Can't merge into self
            if target_id_u32 == source_head_id {
                continue;
            }

            // Reconcile target to ensure we have all its data from blockfile
            self.reconcile_posting_list(target_id_u32).await?;

            // Try to get mutable access to target
            let Some(mut target_data) = self.heads.get_mut(&target_id_u32) else {
                continue; // Target disappeared
            };

            // Check if combined size fits within split threshold
            if target_data.length + source_data.length > self.params.split_threshold {
                continue; // Too big
            }

            let target_centroid = Arc::clone(&nearby.embeddings[idx]);

            // Merge source into target
            target_data
                .posting_list
                .ids
                .extend(source_data.posting_list.ids.iter().cloned());
            target_data
                .posting_list
                .versions
                .extend(source_data.posting_list.versions.iter().cloned());
            target_data
                .posting_list
                .embeddings
                .extend(source_data.posting_list.embeddings.iter().cloned());
            target_data.length += source_data.length;
            drop(target_data); // Release lock before HNSW and reassignment

            // Delete source from HNSW
            {
                let hnsw_write_guard = self.hnsw_index.inner.write();
                hnsw_write_guard
                    .hnsw_index
                    .delete(source_head_id as usize)
                    .map_err(|e| {
                        tracing::error!(
                            "Error deleting head {} from hnsw index during merge: {}",
                            source_head_id,
                            e
                        );
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
            }
            self.deleted_heads.insert(source_head_id, ());

            // Reassign merged points that now violate NPA
            for (i, doc_id) in source_data.posting_list.ids.iter().enumerate() {
                let version = source_data.posting_list.versions[i];
                if self.is_outdated(*doc_id, version)? {
                    continue;
                }

                let embedding = &source_data.posting_list.embeddings[i];
                let dist_to_target = distance_function.distance(embedding, &target_centroid);
                let dist_to_old_source = distance_function.distance(embedding, &source_centroid);

                // NPA check: if point was closer to old source than to new target, reassign
                if dist_to_target > dist_to_old_source {
                    Box::pin(self.reassign(
                        *doc_id,
                        version,
                        embedding.clone(),
                        target_id_u32,
                        ReassignReason::Merge,
                    ))
                    .await?;
                }
            }

            self.stats
                .num_merges
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return Ok(true);
        }

        // No suitable target found - restore source
        self.heads.insert(source_head_id, source_data);
        Ok(false)
    }

    /// Reconciles with blockfile, cleans up outdated entries, triggers split/merge if needed.
    async fn scrub_posting_list(
        &self,
        head_id: u32,
        head_embedding: Arc<[f32]>,
    ) -> Result<(), SpannIndexWriterError> {
        // Reconcile with blockfile.
        self.reconcile_posting_list(head_id).await?;

        // Re-acquire guards for cleanup
        let final_length = {
            let Some(mut head_data) = self.heads.get_mut(&head_id) else {
                return Ok(());
            };

            // Early return if within normal range (no cleanup needed)
            let current_len = head_data.length as usize;
            if current_len <= self.params.split_threshold as usize {
                return Ok(());
            }

            // Cleanup outdated entries
            let mut local_indices: Vec<usize> = vec![0; head_data.posting_list.ids.len()];
            let mut up_to_date_index = 0;
            for (index, (ver, doc_id)) in head_data
                .posting_list
                .versions
                .iter()
                .zip(head_data.posting_list.ids.iter())
                .enumerate()
            {
                let current_version = self
                    .versions
                    .get(doc_id)
                    .ok_or(SpannIndexWriterError::VersionNotFound)?;
                // Disregard if either deleted or on an older version
                if *current_version == 0 || *ver < *current_version {
                    continue;
                }
                local_indices[up_to_date_index] = index;
                up_to_date_index += 1;
            }

            // Compact the posting list
            for (idx, &src_idx) in local_indices.iter().enumerate().take(up_to_date_index) {
                if src_idx != idx {
                    head_data.posting_list.ids[idx] = head_data.posting_list.ids[src_idx];
                    head_data.posting_list.versions[idx] = head_data.posting_list.versions[src_idx];
                    head_data.posting_list.embeddings[idx] =
                        head_data.posting_list.embeddings[src_idx].clone();
                }
            }
            head_data.posting_list.ids.truncate(up_to_date_index);
            head_data.posting_list.versions.truncate(up_to_date_index);
            head_data.posting_list.embeddings.truncate(up_to_date_index);
            head_data.length = up_to_date_index as u32;
            up_to_date_index
        }; // guards dropped here

        // Determine action based on final length
        if final_length > self.params.split_threshold as usize {
            self.split_posting_list(head_id, head_embedding).await?;
        } else if final_length > 0 && final_length < self.params.merge_threshold as usize {
            self.try_merge_posting_list(head_id, head_embedding).await?;
        }

        Ok(())
    }

    /// Splits a posting list using KMeans
    async fn split_posting_list(
        &self,
        head_id: u32,
        head_embedding: Arc<[f32]>,
    ) -> Result<(), SpannIndexWriterError> {
        // Remove from heads DashMap to take ownership.
        // This prevents any other threads from accessing this head while we're splitting it.
        let Some((_, head_data)) = self.heads.remove(&head_id) else {
            return Ok(());
        };
        let HeadData {
            centroid: _,
            posting_list,
            length,
        } = head_data;

        self.stats
            .num_splits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut local_indices: Vec<usize> = Vec::with_capacity(posting_list.ids.len());
        for (index, (ver, doc_id)) in posting_list
            .versions
            .iter()
            .zip(posting_list.ids.iter())
            .enumerate()
        {
            let current_version = self
                .versions
                .get(doc_id)
                .ok_or(SpannIndexWriterError::VersionNotFound)?;
            // Skip if deleted or on an older version
            if *current_version == 0 || *ver < *current_version {
                continue;
            }
            local_indices.push(index);
        }
        local_indices.shuffle(&mut rand::thread_rng());
        let last = local_indices.len();

        // Run KMeans
        let mut kmeans_input = KMeansAlgorithmInput::new(
            local_indices,
            &posting_list.embeddings,
            self.dimensionality,
            /* k */ 2,
            /* first */ 0,
            last,
            self.params.num_samples_kmeans,
            self.params.space.clone().into(),
            self.params.initial_lambda,
        );
        let clustering_output = match cluster(&mut kmeans_input) {
            Ok(output) => output,
            Err(e) => {
                tracing::error!("Error clustering posting list for head {}: {}", head_id, e);
                self.heads.insert(
                    head_id,
                    HeadData {
                        centroid: head_embedding.clone(),
                        posting_list,
                        length,
                    },
                );
                return Err(SpannIndexWriterError::KMeansClusteringError(e));
            }
        };

        if clustering_output.num_clusters <= 1 {
            tracing::warn!("Clustering split the posting list into only 1 cluster");
            if let Some((index, _)) = clustering_output.cluster_labels.iter().next() {
                let single_pl = SpannPostingListOwned {
                    ids: vec![posting_list.ids[*index]],
                    versions: vec![posting_list.versions[*index]],
                    embeddings: vec![Arc::clone(&posting_list.embeddings[*index])],
                };
                self.heads.insert(
                    head_id,
                    HeadData {
                        centroid: head_embedding.clone(),
                        posting_list: single_pl,
                        length: 1,
                    },
                );
                self.stats
                    .num_pl_modified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            return Ok(());
        }

        // Validate cluster counts
        if clustering_output.cluster_counts.contains(&0) {
            tracing::error!("Zero points in a cluster after clustering");
            self.heads.insert(
                head_id,
                HeadData {
                    centroid: head_embedding.clone(),
                    posting_list,
                    length,
                },
            );
            return Err(SpannIndexWriterError::KMeansClusteringError(
                KMeansError::ZeroPointsInCluster,
            ));
        }

        // Extract data for each cluster
        let mut new_posting_lists: Vec<Vec<Arc<[f32]>>> = vec![
            Vec::with_capacity(clustering_output.cluster_counts[0]),
            Vec::with_capacity(clustering_output.cluster_counts[1]),
        ];
        let mut new_doc_offset_ids: Vec<Vec<u32>> = vec![
            Vec::with_capacity(clustering_output.cluster_counts[0]),
            Vec::with_capacity(clustering_output.cluster_counts[1]),
        ];
        let mut new_doc_versions: Vec<Vec<u32>> = vec![
            Vec::with_capacity(clustering_output.cluster_counts[0]),
            Vec::with_capacity(clustering_output.cluster_counts[1]),
        ];
        for (index, cluster) in &clustering_output.cluster_labels {
            let c = *cluster as usize;
            new_doc_offset_ids[c].push(posting_list.ids[*index]);
            new_doc_versions[c].push(posting_list.versions[*index]);
            new_posting_lists[c].push(Arc::clone(&posting_list.embeddings[*index]));
        }

        // Build split_data for reassignment
        let mut split_data: Vec<SplitClusterData> = Vec::with_capacity(2);

        // Same-head optimization: check if one cluster centroid is close to old head.
        let distance_function: DistanceFunction = self.params.space.clone().into();
        let dist0 =
            distance_function.distance(&head_embedding, &clustering_output.cluster_centers[0]);
        let dist1 =
            distance_function.distance(&head_embedding, &clustering_output.cluster_centers[1]);

        // If centroid is very close to old head, reuse head_id
        const SAME_HEAD_THRESHOLD: f32 = 1e-6;
        let same_head_cluster: Option<usize> = if dist0 < SAME_HEAD_THRESHOLD && dist0 <= dist1 {
            Some(0)
        } else if dist1 < SAME_HEAD_THRESHOLD {
            Some(1)
        } else {
            None
        };

        // Process each cluster
        for k in 0..2 {
            // Reuse old head_id and centroid if same_head, create new ones otherwise
            let (cluster_head_id, centroid) = if same_head_cluster == Some(k) {
                (head_id, Arc::clone(&head_embedding))
            } else {
                let new_id = self
                    .next_head_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (new_id, Arc::clone(&clustering_output.cluster_centers[k]))
            };

            // Build split_data entry
            split_data.push(SplitClusterData {
                head_id: cluster_head_id,
                doc_offset_ids: new_doc_offset_ids[k].clone(),
                doc_versions: new_doc_versions[k].clone(),
                embeddings: new_posting_lists[k].clone(),
                centroid: Arc::clone(&centroid),
            });

            // Insert into heads DashMap
            let new_pl = SpannPostingListOwned {
                ids: std::mem::take(&mut new_doc_offset_ids[k]),
                versions: std::mem::take(&mut new_doc_versions[k]),
                embeddings: std::mem::take(&mut new_posting_lists[k]),
            };
            self.heads.insert(
                cluster_head_id,
                HeadData {
                    centroid: Arc::clone(&centroid),
                    posting_list: new_pl,
                    length: clustering_output.cluster_counts[k] as u32,
                },
            );

            // Only add to HNSW if this is a NEW head
            if same_head_cluster != Some(k) {
                let mut hnsw_write_guard = self.hnsw_index.inner.write();
                let hnsw_len = hnsw_write_guard.hnsw_index.len_with_deleted();
                let hnsw_capacity = hnsw_write_guard.hnsw_index.capacity();
                if hnsw_len + 1 > hnsw_capacity {
                    hnsw_write_guard
                        .hnsw_index
                        .resize(hnsw_capacity * 2)
                        .map_err(|e| {
                            tracing::error!(
                                "Error resizing hnsw index during split to {}: {}",
                                hnsw_capacity * 2,
                                e
                            );
                            SpannIndexWriterError::HnswIndexResizeError(e)
                        })?;
                }
                hnsw_write_guard
                    .hnsw_index
                    .add(cluster_head_id as usize, &centroid)
                    .map_err(|e| {
                        tracing::error!(
                            "Error adding head {} to hnsw index: {}",
                            cluster_head_id,
                            e
                        );
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
                self.stats
                    .num_heads_created
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            self.stats
                .num_pl_modified
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Only delete old head from HNSW if we didn't reuse it
        if same_head_cluster.is_none() {
            let hnsw_write_guard = self.hnsw_index.inner.write();
            let _ = hnsw_write_guard.hnsw_index.delete(head_id as usize);
            self.stats
                .num_heads_deleted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.deleted_heads.insert(head_id, ());
        }

        // Reassign points to nearby heads
        Box::pin(self.collect_and_reassign(&split_data, &head_embedding)).await?;
        Ok(())
    }

    async fn add_to_postings_list(
        &self,
        id: u32,
        version: u32,
        embeddings: Arc<[f32]>,
    ) -> Result<(), SpannIndexWriterError> {
        let (ids, _, head_embeddings) = self.rng_query(&embeddings).await?;
        // The only cases when this can happen is initially when no data exists in the
        // index or if all the data that was added to the index was deleted later.
        // In both the cases, in the worst case, it can happen that ids is empty
        // for the first few points getting inserted concurrently by different threads.
        // It's fine to create new centers for each of them since the number of such points
        // will be very small and we can also run GC to merge them later if needed.
        if ids.is_empty() {
            let next_id = self
                .next_head_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Insert into heads DashMap before HNSW (ensures head is set up before becoming discoverable)
            let new_pl = SpannPostingListOwned {
                ids: vec![id],
                versions: vec![version],
                embeddings: vec![embeddings.clone()],
            };
            self.heads.insert(
                next_id,
                HeadData {
                    centroid: embeddings.clone(),
                    posting_list: new_pl,
                    length: 1,
                },
            );
            self.stats
                .num_pl_modified
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Next add to hnsw.
            {
                let mut write_guard = self.hnsw_index.inner.write();
                let hnsw_len = write_guard.hnsw_index.len_with_deleted();
                let hnsw_capacity = write_guard.hnsw_index.capacity();
                if hnsw_len + 1 > hnsw_capacity {
                    write_guard
                        .hnsw_index
                        .resize(hnsw_capacity * 2)
                        .map_err(|e| {
                            tracing::error!(
                                "Error resizing hnsw index during append to {}: {}",
                                hnsw_capacity * 2,
                                e
                            );
                            SpannIndexWriterError::HnswIndexResizeError(e)
                        })?;
                }
                write_guard
                    .hnsw_index
                    .add(next_id as usize, &embeddings)
                    .map_err(|e| {
                        tracing::error!("Error adding new head {} to hnsw index: {}", next_id, e);
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
                self.stats
                    .num_heads_created
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            return Ok(());
        }
        // Otherwise add to the posting list of these arrays.
        for (head_id, head_embedding) in ids.into_iter().zip(head_embeddings.into_iter()) {
            Box::pin(self.append(
                head_id as u32,
                id,
                version,
                embeddings.clone(),
                head_embedding,
            ))
            .await?;
        }

        Ok(())
    }

    pub async fn add(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        let version = self.add_versions_map(id).await;
        // Normalize the embedding in case of cosine.
        let distance_function: DistanceFunction = self.params.space.clone().into();
        let owned_embedding;
        let normalized_embedding: Arc<[f32]> = Arc::from(match distance_function {
            DistanceFunction::Cosine => {
                owned_embedding = normalize(embedding);
                &owned_embedding
            }
            _ => embedding,
        });
        self.embeddings.insert(id, normalized_embedding.clone());
        // Add to the posting list.
        self.add_to_postings_list(id, version, normalized_embedding)
            .await
    }

    pub async fn update(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        let inc_version = {
            let mut version_guard = self.versions.get_mut(&id).ok_or_else(|| {
                tracing::error!("Point {} not found in version map", id);
                SpannIndexWriterError::VersionNotFound
            })?;
            let curr_version = *version_guard;
            if curr_version == 0 {
                tracing::error!("Trying to update a deleted point {}", id);
                return Err(SpannIndexWriterError::VersionNotFound);
            }
            let next_ver = curr_version + 1;
            *version_guard = next_ver;
            next_ver
        }; // guard dropped here
           // Normalize the embedding in case of cosine.
        let distance_function: DistanceFunction = self.params.space.clone().into();
        let owned_embedding;
        let normalized_embedding: Arc<[f32]> = Arc::from(match distance_function {
            DistanceFunction::Cosine => {
                owned_embedding = normalize(embedding);
                &owned_embedding
            }
            _ => embedding,
        });
        self.embeddings.insert(id, normalized_embedding.clone());
        // Add to the posting list.
        self.add_to_postings_list(id, inc_version, normalized_embedding)
            .await
    }

    pub async fn delete(&self, id: u32) -> Result<(), SpannIndexWriterError> {
        self.versions.insert(id, 0);
        self.embeddings.remove(&id);
        Ok(())
    }

    async fn is_head_deleted(&self, head_id: usize) -> Result<bool, SpannIndexWriterError> {
        let hnsw_read_guard = self.hnsw_index.inner.read();
        let hnsw_emb = hnsw_read_guard.hnsw_index.get(head_id);
        // TODO(Sanket): Check for exact error.
        // TODO(Sanket): We should get this information from hnswlib and not rely on error.
        if hnsw_emb.is_err() || hnsw_emb.unwrap().is_none() {
            return Ok(true);
        }
        Ok(false)
    }

    // GC method - scrubs the posting list to remove outdated entries and optionally merges
    async fn garbage_collect_head(
        &self,
        head_id: usize,
        head_embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        // Scrub will remove outdated entries and potentially trigger merge if below threshold
        self.scrub_posting_list(head_id as u32, Arc::from(head_embedding))
            .await
    }

    pub fn eligible_to_gc(&mut self, threshold: f32) -> bool {
        let (len_with_deleted, len_without_deleted) = {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            (
                hnsw_read_guard.hnsw_index.len_with_deleted(),
                hnsw_read_guard.hnsw_index.len(),
            )
        };
        if (len_with_deleted as f32) < ((1.0 + (threshold / 100.0)) * (len_without_deleted as f32))
        {
            tracing::info!(
                "No need to garbage collect heads since delete count is within threshold"
            );
            return false;
        }
        true
    }

    pub async fn garbage_collect_heads(&mut self) -> Result<(), SpannIndexWriterError> {
        tracing::info!("Garbage collecting all the heads");
        let (prefix_path, non_deleted_len) = {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            (
                hnsw_read_guard.prefix_path.clone(),
                hnsw_read_guard.hnsw_index.len(),
            )
        };
        if non_deleted_len == 0 {
            return Ok(());
        }
        // Create a new hnsw index and add elements to it.
        let clean_hnsw = self
            .hnsw_provider
            .create(
                &self.collection_id,
                self.params.max_neighbors,
                self.params.ef_construction,
                self.params.ef_search,
                self.dimensionality as i32,
                self.params.space.clone().into(),
                &prefix_path,
            )
            .await
            .map_err(|e| {
                tracing::error!("Error creating hnsw index during gc");
                SpannIndexWriterError::HnswIndexCreateError(*e)
            })?;
        {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            let mut clean_hnsw_write_guard = clean_hnsw.inner.write();
            let (non_deleted_heads, _) = hnsw_read_guard.hnsw_index.get_all_ids().map_err(|e| {
                tracing::error!("Error getting all ids from hnsw index during gc: {}", e);
                SpannIndexWriterError::HnswIndexSearchError(e)
            })?;
            clean_hnsw_write_guard
                .hnsw_index
                .resize(non_deleted_heads.len())
                .map_err(|e| {
                    tracing::error!(
                        "Error resizing hnsw index during gc to {}: {}",
                        non_deleted_heads.len(),
                        e
                    );
                    SpannIndexWriterError::HnswIndexResizeError(e)
                })?;
            for head in non_deleted_heads {
                let head_embedding = hnsw_read_guard
                    .hnsw_index
                    .get(head)
                    .map_err(|e| {
                        tracing::error!(
                            "Error getting head {} from hnsw index during gc: {}",
                            head,
                            e
                        );
                        SpannIndexWriterError::HnswIndexSearchError(e)
                    })?
                    .ok_or(SpannIndexWriterError::HeadNotFound)?;
                let hnsw_len = clean_hnsw_write_guard.hnsw_index.len_with_deleted();
                let hnsw_capacity = clean_hnsw_write_guard.hnsw_index.capacity();
                if hnsw_len + 1 > hnsw_capacity {
                    clean_hnsw_write_guard
                        .hnsw_index
                        .resize(hnsw_capacity * 2)
                        .map_err(|e| {
                            tracing::error!(
                                "Error resizing hnsw index during gc to {}: {}",
                                hnsw_capacity * 2,
                                e
                            );
                            SpannIndexWriterError::HnswIndexResizeError(e)
                        })?;
                }
                clean_hnsw_write_guard
                    .hnsw_index
                    .add(head, &head_embedding)
                    .map_err(|e| {
                        tracing::error!("Error adding head {} to clean hnsw index: {}", head, e);
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
            }
        }
        // Swap the hnsw index.
        self.cleaned_up_hnsw_index.replace(clean_hnsw);
        Ok(())
    }

    pub async fn pl_garbage_collect_random_sample(
        &self,
        sample_size: f32,
    ) -> Result<(), SpannIndexWriterError> {
        tracing::info!(
            "Garbage collecting {} random samples of posting list",
            sample_size
        );
        // Get all the heads.
        let non_deleted_heads;
        {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            (non_deleted_heads, _) = hnsw_read_guard.hnsw_index.get_all_ids().map_err(|e| {
                tracing::error!("Error getting all ids from hnsw index during gc: {}", e);
                SpannIndexWriterError::HnswIndexSearchError(e)
            })?;
        }
        // Randomly sample x% of heads for gc.
        let sampled_heads = non_deleted_heads.choose_multiple(
            &mut rand::thread_rng(),
            (non_deleted_heads.len() as f32 * sample_size).floor() as usize,
        );
        // Iterate over all the heads and gc heads.
        for head_id in sampled_heads.into_iter() {
            if self.is_head_deleted(*head_id).await? {
                continue;
            }
            let head_embedding = self
                .hnsw_index
                .inner
                .read()
                .hnsw_index
                .get(*head_id)
                .map_err(|e| {
                    tracing::error!(
                        "Error getting head {} from hnsw index during gc: {}",
                        head_id,
                        e
                    );
                    SpannIndexWriterError::HnswIndexSearchError(e)
                })?
                .ok_or(SpannIndexWriterError::HeadNotFound)?;
            self.garbage_collect_head(*head_id, &head_embedding).await?;
        }
        Ok(())
    }

    // Note(Sanket): This has not been tested for running concurrently with
    // other add/update/delete operations.
    pub async fn garbage_collect(&mut self) -> Result<(), SpannIndexWriterError> {
        let gc_latency_metric = self.metrics.gc_latency.clone();
        let stopwatch = Stopwatch::new(
            &gc_latency_metric,
            &[],
            chroma_tracing::util::StopWatchUnit::Seconds,
        );
        if self.gc_context.pl_context().enabled {
            match &self.gc_context.pl_context().policy {
                PlGarbageCollectionPolicy::RandomSample(random_sample) => {
                    self.pl_garbage_collect_random_sample(random_sample.sample_size)
                        .await?;
                }
            }
        }
        if self.gc_context.hnsw_context().enabled {
            match &self.gc_context.hnsw_context().policy {
                HnswGarbageCollectionPolicy::FullRebuild => {
                    self.garbage_collect_heads().await?;
                }
                HnswGarbageCollectionPolicy::DeletePercentage(policy) => {
                    if self.eligible_to_gc(policy.threshold) {
                        self.garbage_collect_heads().await?;
                    }
                }
            }
        }
        tracing::info!(
            "Garbage collected in {} ms",
            stopwatch.elapsed_micros() / 1000
        );
        Ok(())
    }

    fn emit_counters(&self) {
        tracing::info!(
            "Total number of centers fetched from rng in this compaction run: {}",
            self.stats
                .num_centers_fetched_rng
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        // Emit metrics.
        self.metrics.num_centers_fetched_rng.add(
            self.stats
                .num_centers_fetched_rng
                .load(std::sync::atomic::Ordering::Relaxed),
            &[],
        );
        tracing::info!(
            "Total number of rng calls in this compaction run: {}",
            self.stats
                .num_rng_calls
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_rng_calls.add(
            self.stats
                .num_rng_calls
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of heads created in this compaction run: {}",
            self.stats
                .num_heads_created
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_heads_created.add(
            self.stats
                .num_heads_created
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of heads deleted in this compaction run: {}",
            self.stats
                .num_heads_deleted
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_heads_deleted.add(
            self.stats
                .num_heads_deleted
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of posting lists modified in this compaction run: {}",
            self.stats
                .num_pl_modified
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_pl_modified.add(
            self.stats
                .num_pl_modified
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of reassigns in this compaction run: {}",
            self.stats
                .num_reassigns
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_reassigns.add(
            self.stats
                .num_reassigns
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of reassigns due to center merges in this compaction run: {}",
            self.stats
                .num_reassigns_merged_point
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_reassigns_merged_point.add(
            self.stats
                .num_reassigns_merged_point
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of reassigns of neighbors of split cluster in this compaction run: {}",
            self.stats
                .num_reassigns_nbrs
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_reassigns_nbrs.add(
            self.stats
                .num_reassigns_nbrs
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of reassigns of points in split cluster in this compaction run: {}",
            self.stats
                .num_reassigns_split_point
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_reassigns_split_point.add(
            self.stats
                .num_reassigns_split_point
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
        tracing::info!(
            "Total number of splits in this compaction run: {}",
            self.stats
                .num_splits
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        self.metrics.num_splits.add(
            self.stats
                .num_splits
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            &[],
        );
    }

    pub async fn commit(self) -> Result<SpannIndexFlusher, SpannIndexWriterError> {
        self.emit_counters();
        // Posting list: write staged data and delete removed heads.
        let pl_flusher = {
            let stopwatch = Stopwatch::new(
                &self.metrics.pl_commit_latency,
                &[],
                chroma_tracing::util::StopWatchUnit::Millis,
            );

            // Write all staged posting lists from heads
            for (head_id, head_data) in Arc::try_unwrap(self.heads.clone())
                .unwrap_or_else(|arc| (*arc).clone())
                .into_iter()
            {
                // Skip empty posting lists
                if head_data.posting_list.ids.is_empty() {
                    continue;
                }

                // Flatten embeddings for SpannPostingList format
                let flattened_embeddings: Vec<f32> = head_data
                    .posting_list
                    .embeddings
                    .iter()
                    .flat_map(|e| e.iter().cloned())
                    .collect();

                let pl = SpannPostingList {
                    doc_offset_ids: &head_data.posting_list.ids,
                    doc_versions: &head_data.posting_list.versions,
                    doc_embeddings: &flattened_embeddings,
                };

                self.posting_list_writer
                    .set("", head_id, &pl)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error setting posting list for head {}: {}", head_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
            }

            // Delete removed heads
            for (head_id, _) in Arc::try_unwrap(self.deleted_heads)
                .unwrap_or_else(|arc| (*arc).clone())
                .into_iter()
            {
                self.posting_list_writer
                    .delete::<u32, &SpannPostingList<'_>>("", head_id)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error deleting posting list for head {}: {}", head_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
            }

            // Commit
            let pl_flusher = self
                .posting_list_writer
                .clone()
                .commit::<u32, &SpannPostingList<'_>>()
                .await
                .map_err(|e| {
                    tracing::error!("Error committing posting list: {}", e);
                    SpannIndexWriterError::PostingListCommitError(e)
                })?;
            tracing::info!(
                "Committed posting list in {} ms",
                stopwatch.elapsed_micros() / 1000
            );
            pl_flusher
        };
        let versions_map_flusher = {
            let stopwatch = Stopwatch::new(
                &self.metrics.versions_map_commit_latency,
                &[],
                chroma_tracing::util::StopWatchUnit::Millis,
            );
            // Versions map. Create a writer, write all the data and commit.
            let mut bf_options = BlockfileWriterOptions::new(self.prefix_path.clone());
            bf_options = bf_options.unordered_mutations();
            if let Some(cmek) = &self.cmek {
                bf_options = bf_options.with_cmek(cmek.clone());
            }
            let versions_map_bf_writer = self
                .blockfile_provider
                .write::<u32, u32>(bf_options)
                .await
                .map_err(|e| {
                    tracing::error!("Error creating versions map writer: {}", e);
                    SpannIndexWriterError::VersionsMapWriterCreateError(*e)
                })?;

            // Write versions (doc_id -> version)
            for (doc_offset_id, doc_version) in Arc::try_unwrap(self.versions)
                .unwrap_or_else(|arc| (*arc).clone())
                .into_iter()
            {
                versions_map_bf_writer
                    .set("", doc_offset_id, doc_version)
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "Error setting version in versions map for {}, version {}: {}",
                            doc_offset_id,
                            doc_version,
                            e
                        );
                        SpannIndexWriterError::VersionsMapSetError(e)
                    })?;
            }

            // Write lengths (head_id -> length) with "head" prefix from heads
            for (head_id, head_data) in Arc::try_unwrap(self.heads.clone())
                .unwrap_or_else(|arc| (*arc).clone())
                .into_iter()
            {
                versions_map_bf_writer
                    .set("head", head_id, head_data.length)
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "Error setting length in versions map for head {}, length {}: {}",
                            head_id,
                            head_data.length,
                            e
                        );
                        SpannIndexWriterError::VersionsMapSetError(e)
                    })?;
            }

            let versions_map_flusher =
                versions_map_bf_writer
                    .commit::<u32, u32>()
                    .await
                    .map_err(|e| {
                        tracing::error!("Error committing versions map: {}", e);
                        SpannIndexWriterError::VersionsMapCommitError(e)
                    })?;
            tracing::info!(
                "Committed versions map in {} ms",
                stopwatch.elapsed_micros() / 1000
            );
            versions_map_flusher
        };
        // Next head.
        let mut bf_options = BlockfileWriterOptions::new(self.prefix_path.clone());
        bf_options = bf_options.unordered_mutations();
        if let Some(cmek) = &self.cmek {
            bf_options = bf_options.with_cmek(cmek.clone());
        }
        let max_head_id_bf = self
            .blockfile_provider
            .write::<&str, u32>(bf_options)
            .await
            .map_err(|e| {
                tracing::error!("Error creating max head id writer: {}", e);
                SpannIndexWriterError::MaxHeadIdWriterCreateError(*e)
            })?;
        let max_head_oid = self.next_head_id.load(std::sync::atomic::Ordering::Relaxed);
        max_head_id_bf
            .set("", MAX_HEAD_OFFSET_ID, max_head_oid)
            .await
            .map_err(|e| {
                tracing::error!("Error setting max head id: {}", e);
                SpannIndexWriterError::MaxHeadIdSetError(e)
            })?;
        let max_head_id_flusher = max_head_id_bf.commit::<&str, u32>().await.map_err(|e| {
            tracing::error!("Error committing max head id: {}", e);
            SpannIndexWriterError::MaxHeadIdCommitError(e)
        })?;
        tracing::info!("Committed max head id");

        // Hnsw.
        let (hnsw_id, prefix_path, hnsw_index) = {
            let stopwatch = Stopwatch::new(
                &self.metrics.hnsw_commit_latency,
                &[],
                chroma_tracing::util::StopWatchUnit::Millis,
            );
            let (hnsw_id, prefix_path, hnsw_index) = match self.cleaned_up_hnsw_index {
                Some(index) => {
                    tracing::info!("Committing cleaned up hnsw index");
                    let (id, prefix_path) = {
                        let index_guard = index.inner.read();
                        (index_guard.hnsw_index.id, index_guard.prefix_path.clone())
                    };
                    (id, prefix_path, index)
                }
                None => {
                    let (id, prefix_path) = {
                        let index_guard = self.hnsw_index.inner.read();
                        (index_guard.hnsw_index.id, index_guard.prefix_path.clone())
                    };
                    (id, prefix_path, self.hnsw_index.clone())
                }
            };
            self.hnsw_provider.commit(hnsw_index.clone()).map_err(|e| {
                tracing::error!("Error committing hnsw index: {}", e);
                SpannIndexWriterError::HnswIndexCommitError(e)
            })?;
            tracing::info!(
                "Committed hnsw index in {} ms",
                stopwatch.elapsed_micros() / 1000
            );
            (hnsw_id, prefix_path, hnsw_index)
        };

        Ok(SpannIndexFlusher {
            pl_flusher,
            versions_map_flusher,
            max_head_id_flusher,
            hnsw_flusher: HnswIndexFlusher {
                provider: self.hnsw_provider,
                prefix_path,
                index_id: hnsw_id,
                hnsw_index,
                cmek: self.cmek,
            },
            metrics: SpannIndexFlusherMetrics {
                pl_flush_latency: self.metrics.pl_flush_latency.clone(),
                versions_map_flush_latency: self.metrics.versions_map_flush_latency.clone(),
                hnsw_flush_latency: self.metrics.hnsw_flush_latency.clone(),
                num_pl_entries_flushed: self.metrics.num_pl_entries_flushed.clone(),
                num_versions_map_entries_flushed: self
                    .metrics
                    .num_versions_map_entries_flushed
                    .clone(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{f32::consts::PI, path::PathBuf};

    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
        },
        provider::BlockfileProvider,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_config::{registry::Registry, Configurable};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, InternalSpannConfiguration};
    use rand::Rng;

    use crate::{
        config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
        hnsw_provider::HnswIndexProvider,
        spann::types::GarbageCollectionContext,
        Index,
    };

    use super::{FastSpannIndexWriter, SpannMetrics};

    #[tokio::test]
    async fn test_split() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            false,
        );
        let collection_id = CollectionUuid::new();
        let dimensionality = 2;
        let params = InternalSpannConfiguration {
            split_threshold: 100,
            reassign_neighbor_count: 8,
            merge_threshold: 50,
            max_neighbors: 16,
            ..Default::default()
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
        let prefix_path = "";
        let pl_block_size = 5 * 1024 * 1024;
        let writer = FastSpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            prefix_path,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            pl_block_size,
            SpannMetrics::default(),
            None,
        )
        .await
        .expect("Error creating spann index writer");
        // Insert origin.
        writer
            .add(1, &[0.0, 0.0])
            .await
            .expect("Error adding to spann index writer");
        println!("Inserted {:?}", &[0.0, 0.0]);
        // Insert 100 points. There should be no splitting yet.
        // Generate these points within a radius of 1 from origin.
        let mut rng = rand::thread_rng();
        for i in 2..=100 {
            // Generate random radius between 0 and 1
            let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

            // Generate random angle between 0 and 2π
            let theta = rng.gen::<f32>() * 2.0 * PI;

            // Convert to Cartesian coordinates
            let x = r * theta.cos();
            let y = r * theta.sin();

            let embedding = vec![x, y];
            println!("Inserting {:?}", embedding);
            writer
                .add(i, &embedding)
                .await
                .expect("Error adding to spann index writer");
        }
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.hnsw_index.len(), 1);
            let emb = hnsw_read_guard
                .hnsw_index
                .get(1)
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
        }
        {
            // Posting list should have 100 points.
            let head_data = writer.heads.get(&1).expect("Head 1 not found");
            assert_eq!(head_data.posting_list.ids.len(), 100);
            assert_eq!(head_data.posting_list.versions.len(), 100);
            assert_eq!(head_data.posting_list.embeddings.len(), 100);
        }
        // Insert a point in another region. (10000.0, 10000.0)
        writer
            .add(101, &[10000.0, 10000.0])
            .await
            .expect("Error adding to spann index writer");
        // There should be a split and we should have 2 centers.
        let mut emb_1_id;
        let mut emb_2_id;
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.hnsw_index.len(), 2);
            emb_2_id = 2;
            // Head could be 2 and 3 or 1 and 2.
            if hnsw_read_guard.hnsw_index.get(1).is_err() {
                emb_1_id = 3;
            } else {
                emb_1_id = 1;
            }
        }
        {
            // Posting list should have 100 points.
            let head1 = writer.heads.get(&emb_1_id).expect("Head 1 not found");
            let head2 = writer.heads.get(&emb_2_id).expect("Head 2 not found");
            // Only two combinations possible.
            if head1.posting_list.ids.len() == 100 {
                assert_eq!(head1.posting_list.versions.len(), 100);
                assert_eq!(head1.posting_list.embeddings.len(), 100);
                assert_eq!(head2.posting_list.ids.len(), 1);
                assert_eq!(head2.posting_list.versions.len(), 1);
                assert_eq!(head2.posting_list.embeddings.len(), 1);
            } else if head2.posting_list.ids.len() == 100 {
                assert_eq!(head2.posting_list.versions.len(), 100);
                assert_eq!(head2.posting_list.embeddings.len(), 100);
                assert_eq!(head1.posting_list.ids.len(), 1);
                assert_eq!(head1.posting_list.versions.len(), 1);
                assert_eq!(head1.posting_list.embeddings.len(), 1);
            } else {
                panic!("Invalid posting list lengths");
            }
        }
        // Next insert 99 points in the region of (1000.0, 1000.0)
        for i in 102..=200 {
            // Generate random radius between 0 and 1
            let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

            // Generate random angle between 0 and 2π
            let theta = rng.gen::<f32>() * 2.0 * PI;

            // Convert to Cartesian coordinates
            let x = r * theta.cos() + 10000.0;
            let y = r * theta.sin() + 10000.0;

            let embedding = vec![x, y];
            println!("Inserting {:?}", embedding);
            writer
                .add(i, &embedding)
                .await
                .expect("Error adding to spann index writer");
        }
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.hnsw_index.len(), 2);
            emb_2_id = 2;
            // Head could be 2 and 3 or 1 and 2.
            if hnsw_read_guard.hnsw_index.get(1).is_err() {
                emb_1_id = 3;
            } else {
                emb_1_id = 1;
            }
        }
        {
            // Posting list should have 100 points.
            let head1 = writer.heads.get(&emb_1_id).expect("Head 1 not found");
            assert_eq!(head1.posting_list.ids.len(), 100);
            assert_eq!(head1.posting_list.versions.len(), 100);
            assert_eq!(head1.posting_list.embeddings.len(), 100);
            let head2 = writer.heads.get(&emb_2_id).expect("Head 2 not found");
            assert_eq!(head2.posting_list.ids.len(), 100);
            assert_eq!(head2.posting_list.versions.len(), 100);
            assert_eq!(head2.posting_list.embeddings.len(), 100);
        }
    }
}
