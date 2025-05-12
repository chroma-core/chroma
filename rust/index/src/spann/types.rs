use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc,
    },
};

use chroma_blockstore::{
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileFlusher, BlockfileReader, BlockfileWriter, BlockfileWriterOptions,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_tracing::util::Stopwatch;
use chroma_types::SpannPostingList;
use chroma_types::{CollectionUuid, InternalSpannConfiguration};
use opentelemetry::{global, KeyValue};
use rand::seq::SliceRandom;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    config::{
        HnswGarbageCollectionConfig, HnswGarbageCollectionPolicyConfig, PlGarbageCollectionConfig,
        PlGarbageCollectionPolicyConfig, RandomSamplePolicyConfig,
    },
    hnsw_provider::{
        HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderFlushError,
        HnswIndexProviderForkError, HnswIndexProviderOpenError, HnswIndexRef,
    },
    spann::utils::cluster,
    Index, IndexUuid,
};

use super::utils::{rng_query, KMeansAlgorithmInput, KMeansError, RngQueryError};

#[derive(Clone, Debug)]
pub struct VersionsMapInner {
    pub versions_map: HashMap<u32, u32>,
}

#[derive(Clone, Debug)]
pub struct GarbageCollectionContext {
    pl_context: PlGarbageCollectionContext,
    hnsw_context: HnswGarbageCollectionContext,
}

#[async_trait::async_trait]
impl Configurable<(PlGarbageCollectionConfig, HnswGarbageCollectionConfig)>
    for GarbageCollectionContext
{
    async fn try_from_config(
        config: &(PlGarbageCollectionConfig, HnswGarbageCollectionConfig),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let pl_context = PlGarbageCollectionContext::try_from_config(&config.0, registry).await?;
        let hnsw_context =
            HnswGarbageCollectionContext::try_from_config(&config.1, registry).await?;
        Ok(GarbageCollectionContext {
            pl_context,
            hnsw_context,
        })
    }
}

#[derive(Clone, Debug)]
pub struct HnswGarbageCollectionContext {
    pub enabled: bool,
    pub policy: HnswGarbageCollectionPolicy,
}

#[async_trait::async_trait]
impl Configurable<HnswGarbageCollectionConfig> for HnswGarbageCollectionContext {
    async fn try_from_config(
        config: &HnswGarbageCollectionConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let policy = HnswGarbageCollectionPolicy::try_from_config(&config.policy, registry).await?;
        Ok(HnswGarbageCollectionContext {
            enabled: config.enabled,
            policy,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PlGarbageCollectionContext {
    pub enabled: bool,
    pub policy: PlGarbageCollectionPolicy,
}

#[async_trait::async_trait]
impl Configurable<PlGarbageCollectionConfig> for PlGarbageCollectionContext {
    async fn try_from_config(
        config: &PlGarbageCollectionConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let policy = PlGarbageCollectionPolicy::try_from_config(&config.policy, registry).await?;
        Ok(PlGarbageCollectionContext {
            enabled: config.enabled,
            policy,
        })
    }
}

#[derive(Clone, Debug)]
pub enum PlGarbageCollectionPolicy {
    RandomSample(RandomSamplePolicy),
}

#[async_trait::async_trait]
impl Configurable<PlGarbageCollectionPolicyConfig> for PlGarbageCollectionPolicy {
    async fn try_from_config(
        config: &PlGarbageCollectionPolicyConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            PlGarbageCollectionPolicyConfig::RandomSample(policy) => {
                let policy = RandomSamplePolicy::try_from_config(policy, registry).await?;
                Ok(PlGarbageCollectionPolicy::RandomSample(policy))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeletePercentageThresholdPolicy {
    pub threshold: f32,
}

#[derive(Clone, Debug)]
pub enum HnswGarbageCollectionPolicy {
    FullRebuild,
    DeletePercentage(DeletePercentageThresholdPolicy),
}

#[async_trait::async_trait]
impl Configurable<HnswGarbageCollectionPolicyConfig> for HnswGarbageCollectionPolicy {
    async fn try_from_config(
        config: &HnswGarbageCollectionPolicyConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            HnswGarbageCollectionPolicyConfig::FullRebuild => {
                Ok(HnswGarbageCollectionPolicy::FullRebuild)
            }
            HnswGarbageCollectionPolicyConfig::DeletePercentage(policy_config) => Ok(
                HnswGarbageCollectionPolicy::DeletePercentage(DeletePercentageThresholdPolicy {
                    threshold: policy_config.threshold,
                }),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RandomSamplePolicy {
    pub sample_size: f32,
}

#[async_trait::async_trait]
impl Configurable<RandomSamplePolicyConfig> for RandomSamplePolicy {
    async fn try_from_config(
        config: &RandomSamplePolicyConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(RandomSamplePolicy {
            sample_size: config.sample_size,
        })
    }
}

#[derive(Clone, Debug)]
struct WriteStats {
    num_pl_modified: Arc<AtomicU32>,
    num_heads_created: Arc<AtomicU32>,
    num_heads_deleted: Arc<AtomicU32>,
    num_reassigns: Arc<AtomicU32>,
    num_splits: Arc<AtomicU32>,
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

#[derive(Clone)]
// Note: Fields of this struct are public for testing.
pub struct SpannIndexWriter {
    // HNSW index and its provider for centroid search.
    pub hnsw_index: HnswIndexRef,
    pub cleaned_up_hnsw_index: Option<HnswIndexRef>,
    hnsw_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    // Posting list of the centroids.
    // TODO(Sanket): For now the lock is very coarse grained. But this should
    // be changed in future if perf is not satisfactory.
    pub posting_list_writer: Arc<tokio::sync::Mutex<BlockfileWriter>>,
    pub next_head_id: Arc<AtomicU32>,
    // Version number of each point.
    // TODO(Sanket): Finer grained locking for this map in future if perf is not satisfactory.
    pub versions_map: Arc<tokio::sync::RwLock<VersionsMapInner>>,
    pub dimensionality: usize,
    pub params: InternalSpannConfiguration,
    pub gc_context: GarbageCollectionContext,
    pub collection_id: CollectionUuid,
    metrics: SpannMetrics,
    stats: WriteStats,
}

#[derive(Error, Debug)]
pub enum SpannIndexWriterError {
    #[error("Error forking hnsw index {0}")]
    HnswIndexForkError(#[source] HnswIndexProviderForkError),
    #[error("Error creating hnsw index {0}")]
    HnswIndexCreateError(#[source] HnswIndexProviderCreateError),
    #[error("Error creating reader for versions map blockfile {0}")]
    VersionsMapReaderCreateError(#[source] OpenError),
    #[error("Error creating/forking postings list writer {0}")]
    PostingsListWriterCreateError(#[source] CreateError),
    #[error("Error loading data from versions map blockfile {0}")]
    VersionsMapDataLoadError(#[source] Box<dyn ChromaError>),
    #[error("Error reading max offset id for heads {0}")]
    MaxHeadIdBlockfileGetError(#[source] Box<dyn ChromaError>),
    #[error("Max Head Id not found")]
    MaxHeadIdNotFound,
    #[error("Error resizing hnsw index {0}")]
    HnswIndexResizeError(#[source] Box<dyn ChromaError>),
    #[error("Error adding to hnsw index {0}")]
    HnswIndexMutateError(#[source] Box<dyn ChromaError>),
    #[error("Error searching hnsw {0}")]
    HnswIndexSearchError(#[source] Box<dyn ChromaError>),
    #[error("Error rng querying hnsw {0}")]
    RngQueryError(#[source] RngQueryError),
    #[error("Head not found in Hnsw index")]
    HeadNotFound,
    #[error("Error adding posting list for a head {0}")]
    PostingListSetError(#[source] Box<dyn ChromaError>),
    #[error("Error getting the posting list for a head")]
    PostingListGetError(#[source] Box<dyn ChromaError>),
    #[error("Posting list not found for head")]
    PostingListNotFound,
    #[error("Did not find the version for head id")]
    VersionNotFound,
    #[error("Error committing postings list blockfile {0}")]
    PostingListCommitError(#[source] Box<dyn ChromaError>),
    #[error("Error creating blockfile writer for versions map {0}")]
    VersionsMapWriterCreateError(#[source] CreateError),
    #[error("Error writing data to versions map blockfile {0}")]
    VersionsMapSetError(#[source] Box<dyn ChromaError>),
    #[error("Error committing versions map blockfile {0}")]
    VersionsMapCommitError(#[source] Box<dyn ChromaError>),
    #[error("Error creating blockfile writer for max head id {0}")]
    MaxHeadIdWriterCreateError(#[source] CreateError),
    #[error("Error writing data to max head id blockfile {0}")]
    MaxHeadIdSetError(#[source] Box<dyn ChromaError>),
    #[error("Error committing max head id blockfile {0}")]
    MaxHeadIdCommitError(#[source] Box<dyn ChromaError>),
    #[error("Error committing hnsw index {0}")]
    HnswIndexCommitError(#[source] Box<dyn ChromaError>),
    #[error("Error flushing postings list blockfile {0}")]
    PostingListFlushError(#[source] Box<dyn ChromaError>),
    #[error("Error flushing versions map blockfile {0}")]
    VersionsMapFlushError(#[source] Box<dyn ChromaError>),
    #[error("Error flushing max head id blockfile {0}")]
    MaxHeadIdFlushError(#[source] Box<dyn ChromaError>),
    #[error("Error flushing hnsw index {0}")]
    HnswIndexFlushError(#[source] HnswIndexProviderFlushError),
    #[error("Error kmeans clustering {0}")]
    KMeansClusteringError(#[from] KMeansError),
}

impl ChromaError for SpannIndexWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexForkError(e) => e.code(),
            Self::HnswIndexCreateError(e) => e.code(),
            Self::VersionsMapReaderCreateError(e) => e.code(),
            Self::PostingsListWriterCreateError(e) => e.code(),
            Self::VersionsMapDataLoadError(e) => e.code(),
            Self::MaxHeadIdBlockfileGetError(e) => e.code(),
            Self::MaxHeadIdNotFound => ErrorCodes::Internal,
            Self::HnswIndexResizeError(e) => e.code(),
            Self::HnswIndexMutateError(e) => e.code(),
            Self::PostingListSetError(e) => e.code(),
            Self::HnswIndexSearchError(e) => e.code(),
            Self::RngQueryError(e) => e.code(),
            Self::HeadNotFound => ErrorCodes::Internal,
            Self::PostingListGetError(e) => e.code(),
            Self::PostingListNotFound => ErrorCodes::Internal,
            Self::VersionNotFound => ErrorCodes::Internal,
            Self::PostingListCommitError(e) => e.code(),
            Self::VersionsMapSetError(e) => e.code(),
            Self::VersionsMapCommitError(e) => e.code(),
            Self::MaxHeadIdSetError(e) => e.code(),
            Self::MaxHeadIdCommitError(e) => e.code(),
            Self::HnswIndexCommitError(e) => e.code(),
            Self::PostingListFlushError(e) => e.code(),
            Self::VersionsMapFlushError(e) => e.code(),
            Self::MaxHeadIdFlushError(e) => e.code(),
            Self::HnswIndexFlushError(e) => e.code(),
            Self::VersionsMapWriterCreateError(e) => e.code(),
            Self::MaxHeadIdWriterCreateError(e) => e.code(),
            Self::KMeansClusteringError(e) => e.code(),
        }
    }
}

const MAX_HEAD_OFFSET_ID: &str = "max_head_offset_id";

#[derive(Clone, Debug)]
enum ReassignReason {
    Split,
    Nearby,
    Merge,
}

impl SpannIndexWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hnsw_index: HnswIndexRef,
        hnsw_provider: HnswIndexProvider,
        blockfile_provider: BlockfileProvider,
        posting_list_writer: BlockfileWriter,
        next_head_id: u32,
        versions_map: VersionsMapInner,
        dimensionality: usize,
        params: InternalSpannConfiguration,
        gc_context: GarbageCollectionContext,
        collection_id: CollectionUuid,
        metrics: SpannMetrics,
    ) -> Self {
        SpannIndexWriter {
            hnsw_index,
            cleaned_up_hnsw_index: None,
            hnsw_provider,
            blockfile_provider,
            posting_list_writer: Arc::new(tokio::sync::Mutex::new(posting_list_writer)),
            next_head_id: Arc::new(AtomicU32::new(next_head_id)),
            versions_map: Arc::new(tokio::sync::RwLock::new(versions_map)),
            dimensionality,
            params,
            gc_context,
            collection_id,
            metrics,
            stats: WriteStats::default(),
        }
    }

    async fn hnsw_index_from_id(
        hnsw_provider: &HnswIndexProvider,
        id: &IndexUuid,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        ef_search: usize,
    ) -> Result<HnswIndexRef, SpannIndexWriterError> {
        match hnsw_provider
            .fork(
                id,
                collection_id,
                dimensionality as i32,
                distance_function,
                ef_search,
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

    async fn create_hnsw_index(
        hnsw_provider: &HnswIndexProvider,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<HnswIndexRef, SpannIndexWriterError> {
        match hnsw_provider
            .create(
                collection_id,
                m,
                ef_construction,
                ef_search,
                dimensionality as i32,
                distance_function,
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

    async fn load_versions_map(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<VersionsMapInner, SpannIndexWriterError> {
        // Create a reader for the blockfile. Load all the data into the versions map.
        let mut versions_map = HashMap::new();
        let reader = match blockfile_provider.read::<u32, u32>(blockfile_id).await {
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
        // Load data using the reader.
        let versions_data = reader.get_range(.., ..).await.map_err(|e| {
            tracing::error!(
                "Error performing get_range for versions map blockfile {:?}: {:?}",
                blockfile_id,
                e
            );
            SpannIndexWriterError::VersionsMapDataLoadError(e)
        })?;
        versions_data.iter().for_each(|(_, key, value)| {
            versions_map.insert(*key, *value);
        });
        Ok(VersionsMapInner { versions_map })
    }

    async fn fork_postings_list(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterError> {
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
        bf_options = bf_options.fork(*blockfile_id);
        match blockfile_provider
            .write::<u32, &SpannPostingList<'_>>(bf_options)
            .await
        {
            Ok(writer) => Ok(writer),
            Err(e) => {
                tracing::error!(
                    "Error forking postings list writer from blockfile {:?}: {:?}",
                    blockfile_id,
                    e
                );
                Err(SpannIndexWriterError::PostingsListWriterCreateError(*e))
            }
        }
    }

    async fn create_posting_list(
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterError> {
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
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
        dimensionality: usize,
        blockfile_provider: &BlockfileProvider,
        params: InternalSpannConfiguration,
        gc_context: GarbageCollectionContext,
        metrics: SpannMetrics,
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
                )
                .await?
            }
        };
        // Load the versions map.
        let versions_map = match versions_map_id {
            Some(versions_map_id) => {
                Self::load_versions_map(versions_map_id, blockfile_provider).await?
            }
            None => VersionsMapInner {
                versions_map: HashMap::new(),
            },
        };
        // Fork the posting list writer.
        let posting_list_writer = match posting_list_id {
            Some(posting_list_id) => {
                Self::fork_postings_list(posting_list_id, blockfile_provider).await?
            }
            None => Self::create_posting_list(blockfile_provider).await?,
        };

        let max_head_id = match max_head_id_bf_id {
            Some(max_head_id_bf_id) => {
                let reader = blockfile_provider
                    .read::<&str, u32>(max_head_id_bf_id)
                    .await;
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
            versions_map,
            dimensionality,
            params,
            gc_context,
            *collection_id,
            metrics,
        ))
    }

    async fn add_versions_map(&self, id: u32) -> u32 {
        // 0 means deleted. Version counting starts from 1.
        let mut write_lock = self.versions_map.write().await;
        write_lock.versions_map.insert(id, 1);
        *write_lock.versions_map.get(&id).unwrap()
    }

    async fn rng_query(
        &self,
        query: &[f32],
    ) -> Result<(Vec<usize>, Vec<f32>, Vec<Vec<f32>>), SpannIndexWriterError> {
        let res = rng_query(
            query,
            self.hnsw_index.clone(),
            self.params.write_nprobe as usize,
            self.params.write_rng_epsilon,
            self.params.write_rng_factor,
            self.params.space.clone().into(),
            true,
        )
        .await
        .map_err(|e| {
            tracing::error!("Error rng querying hnsw for {:?}: {:?}", query, e);
            SpannIndexWriterError::RngQueryError(e)
        });
        self.stats
            .num_rng_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats.num_centers_fetched_rng.fetch_add(
            res.as_ref().map(|r| r.0.len() as u64).unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );
        res
    }

    async fn is_outdated(
        &self,
        doc_offset_id: u32,
        version: u32,
    ) -> Result<bool, SpannIndexWriterError> {
        let version_map_guard = self.versions_map.read().await;
        let current_version = version_map_guard
            .versions_map
            .get(&doc_offset_id)
            .ok_or(SpannIndexWriterError::VersionNotFound)?;
        if *current_version == 0 || version < *current_version {
            return Ok(true);
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    async fn collect_and_reassign_split_points(
        &self,
        new_head_ids: &[i32],
        new_head_embeddings: &[Option<&Vec<f32>>],
        old_head_embedding: &[f32],
        split_doc_offset_ids: &[Vec<u32>],
        split_doc_versions: &[Vec<u32>],
        split_doc_embeddings: &[Vec<f32>],
    ) -> Result<HashSet<u32>, SpannIndexWriterError> {
        let mut assigned_ids = HashSet::new();
        for (k, ((doc_offset_ids, doc_versions), doc_embeddings)) in split_doc_offset_ids
            .iter()
            .zip(split_doc_versions.iter())
            .zip(split_doc_embeddings.iter())
            .enumerate()
        {
            for (index, doc_offset_id) in doc_offset_ids.iter().enumerate() {
                if assigned_ids.contains(doc_offset_id)
                    || self
                        .is_outdated(*doc_offset_id, doc_versions[index])
                        .await?
                {
                    continue;
                }
                let distance_function: DistanceFunction = self.params.space.clone().into();
                let old_dist = distance_function.distance(
                    old_head_embedding,
                    &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                );
                let new_dist = distance_function.distance(
                    new_head_embeddings[k].unwrap(),
                    &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                );
                // NPA check.
                if new_dist > old_dist {
                    assigned_ids.insert(*doc_offset_id);
                    self.reassign(
                        *doc_offset_id,
                        doc_versions[index],
                        &doc_embeddings
                            [index * self.dimensionality..(index + 1) * self.dimensionality],
                        new_head_ids[k] as u32,
                        ReassignReason::Split,
                    )
                    .await?;
                }
            }
        }
        Ok(assigned_ids)
    }

    async fn get_nearby_heads(
        &self,
        head_embedding: &[f32],
        k: usize,
    ) -> Result<(Vec<usize>, Vec<f32>, Vec<Vec<f32>>), SpannIndexWriterError> {
        let mut nearest_embeddings: Vec<Vec<f32>> = vec![];
        let read_guard = self.hnsw_index.inner.read();
        let allowed_ids = vec![];
        let disallowed_ids = vec![];
        let (nearest_ids, nearest_distances) = read_guard
            .query(head_embedding, k, &allowed_ids, &disallowed_ids)
            .map_err(|e| {
                tracing::error!("Error querying hnsw for {:?}: {:?}", head_embedding, e);
                SpannIndexWriterError::HnswIndexSearchError(e)
            })?;
        // Get the embeddings also for distance computation.
        // TODO(Sanket): Don't consider heads that are farther away than the closest.
        for id in nearest_ids.iter() {
            let emb = read_guard
                .get(*id)
                .map_err(|e| {
                    tracing::error!(
                        "Error getting embedding from hnsw index for id {}: {}",
                        id,
                        e
                    );
                    SpannIndexWriterError::HnswIndexSearchError(e)
                })?
                .ok_or(SpannIndexWriterError::HeadNotFound)?;
            nearest_embeddings.push(emb);
        }
        Ok((nearest_ids, nearest_distances, nearest_embeddings))
    }

    async fn reassign(
        &self,
        doc_offset_id: u32,
        doc_version: u32,
        doc_embedding: &[f32],
        prev_head_id: u32,
        reason: ReassignReason,
    ) -> Result<(), SpannIndexWriterError> {
        // Don't reassign if outdated by now.
        if self.is_outdated(doc_offset_id, doc_version).await? {
            tracing::debug!(
                "Outdated point {} for reassignment version {} current head id {}",
                doc_offset_id,
                doc_version,
                prev_head_id
            );
            return Ok(());
        }
        // RNG query to find the nearest heads.
        let (nearest_head_ids, _, nearest_head_embeddings) = self.rng_query(doc_embedding).await?;
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
        let next_version;
        {
            let mut version_map_guard = self.versions_map.write().await;
            let current_version = version_map_guard
                .versions_map
                .get(&doc_offset_id)
                .ok_or(SpannIndexWriterError::VersionNotFound)?;
            if doc_version < *current_version {
                tracing::debug!(
                    "Outdated point {} for reassignment version {} current head id {}",
                    doc_offset_id,
                    doc_version,
                    prev_head_id
                );
                return Ok(());
            }
            next_version = *current_version + 1;
            version_map_guard
                .versions_map
                .insert(doc_offset_id, next_version);
        }
        // Append to the posting list.
        for (nearest_head_id, nearest_head_embedding) in nearest_head_ids
            .into_iter()
            .zip(nearest_head_embeddings.into_iter())
        {
            if self.is_outdated(doc_offset_id, next_version).await? {
                tracing::debug!(
                    "Outdated point {} for reassignment version {} current head id {}",
                    doc_offset_id,
                    doc_version,
                    prev_head_id
                );
                return Ok(());
            }
            tracing::debug!(
                "Reassigning {} to head {} incremented version {} current head id {}",
                doc_offset_id,
                nearest_head_id,
                next_version,
                prev_head_id
            );
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
                doc_embedding,
                nearest_head_embedding,
            )
            .await?;
        }
        Ok(())
    }

    async fn collect_and_reassign_nearby_points(
        &self,
        head_id: usize,
        head_embedding: &[f32],
        assigned_ids: &mut HashSet<u32>,
        new_head_embeddings: &[Option<&Vec<f32>>],
        old_head_embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        // Get posting list of each neighbour and check for reassignment criteria.
        let doc_offset_ids;
        let doc_versions;
        let doc_embeddings;
        {
            let write_guard = self.posting_list_writer.lock().await;
            // TODO(Sanket): Check if head is deleted, can happen if another concurrent thread
            // deletes it.
            (doc_offset_ids, doc_versions, doc_embeddings) = write_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", head_id as u32)
                .await
                .map_err(|e| {
                    tracing::error!("Error getting posting list for head {}: {}", head_id, e);
                    SpannIndexWriterError::PostingListGetError(e)
                })?
                .ok_or(SpannIndexWriterError::PostingListNotFound)?;
        }
        for (index, doc_offset_id) in doc_offset_ids.iter().enumerate() {
            if assigned_ids.contains(doc_offset_id)
                || self
                    .is_outdated(*doc_offset_id, doc_versions[index])
                    .await?
            {
                continue;
            }
            let distance_function: DistanceFunction = self.params.space.clone().into();
            let distance_from_curr_center = distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                head_embedding,
            );
            let distance_from_split_center1 = distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                new_head_embeddings[0].unwrap(),
            );
            let distance_from_split_center2 = distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                new_head_embeddings[1].unwrap(),
            );
            if distance_from_curr_center <= distance_from_split_center1
                && distance_from_curr_center <= distance_from_split_center2
            {
                continue;
            }
            let distance_from_old_head = distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                old_head_embedding,
            );
            if distance_from_old_head <= distance_from_split_center1
                && distance_from_old_head <= distance_from_split_center2
            {
                continue;
            }
            // Candidate for reassignment.
            assigned_ids.insert(*doc_offset_id);
            self.reassign(
                *doc_offset_id,
                doc_versions[index],
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                head_id as u32,
                ReassignReason::Nearby,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn collect_and_reassign(
        &self,
        new_head_ids: &[i32],
        new_head_embeddings: &[Option<&Vec<f32>>],
        old_head_embedding: &[f32],
        split_doc_offset_ids: &[Vec<u32>],
        split_doc_versions: &[Vec<u32>],
        split_doc_embeddings: &[Vec<f32>],
    ) -> Result<(), SpannIndexWriterError> {
        let mut assigned_ids = self
            .collect_and_reassign_split_points(
                new_head_ids,
                new_head_embeddings,
                old_head_embedding,
                split_doc_offset_ids,
                split_doc_versions,
                split_doc_embeddings,
            )
            .await?;
        // Reassign neighbors of this center if applicable.
        if self.params.reassign_neighbor_count > 0 {
            let (nearby_head_ids, _, nearby_head_embeddings) = self
                .get_nearby_heads(
                    old_head_embedding,
                    self.params.reassign_neighbor_count as usize,
                )
                .await?;
            for (head_idx, head_id) in nearby_head_ids.iter().enumerate() {
                // Skip the current split heads.
                if new_head_ids.contains(&(*head_id as i32)) {
                    continue;
                }
                self.collect_and_reassign_nearby_points(
                    *head_id,
                    &nearby_head_embeddings[head_idx],
                    &mut assigned_ids,
                    new_head_embeddings,
                    old_head_embedding,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append(
        &self,
        head_id: u32,
        id: u32,
        version: u32,
        embedding: &[f32],
        head_embedding: Vec<f32>,
    ) -> Result<(), SpannIndexWriterError> {
        let mut new_posting_lists: Vec<Vec<f32>> = Vec::with_capacity(2);
        let mut new_doc_offset_ids: Vec<Vec<u32>> = Vec::with_capacity(2);
        let mut new_doc_versions: Vec<Vec<u32>> = Vec::with_capacity(2);
        let mut new_head_ids = vec![-1; 2];
        let mut new_head_embeddings = vec![None; 2];
        let clustering_output;
        {
            let write_guard = self.posting_list_writer.lock().await;
            if self.is_head_deleted(head_id as usize).await? {
                tracing::info!(
                    "Head {} got concurrently deleted for adding point {} at version {}. Reassigning now",
                    head_id,
                    id,
                    version
                );
                if self.is_outdated(id, version).await? {
                    return Ok(());
                }
                // Try again.
                drop(write_guard);
                return Box::pin(self.reassign(
                    id,
                    version,
                    embedding,
                    head_id,
                    ReassignReason::Split,
                ))
                .await;
            }
            let (mut doc_offset_ids, mut doc_versions, mut doc_embeddings) = write_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", head_id)
                .await
                .map_err(|e| {
                    tracing::error!("Error getting posting list for head {}: {}", head_id, e);
                    SpannIndexWriterError::PostingListGetError(e)
                })?
                .ok_or(SpannIndexWriterError::PostingListNotFound)?;
            // Append the new point to the posting list.
            doc_offset_ids.reserve_exact(1);
            doc_versions.reserve_exact(1);
            doc_embeddings.reserve_exact(embedding.len());
            doc_offset_ids.push(id);
            doc_versions.push(version);
            doc_embeddings.extend_from_slice(embedding);
            // Cleanup this posting list.
            // Note: There is an order in which we are acquiring locks here to prevent deadlocks.
            // Note: This way of cleaning up takes less memory since we don't allocate
            // memory for embeddings that are not outdated.
            let mut local_indices = vec![0; doc_offset_ids.len()];
            let mut up_to_date_index = 0;
            {
                let version_map_guard = self.versions_map.read().await;
                for (index, doc_version) in doc_versions.iter().enumerate() {
                    let current_version = version_map_guard
                        .versions_map
                        .get(&doc_offset_ids[index])
                        .ok_or(SpannIndexWriterError::VersionNotFound)?;
                    // disregard if either deleted or on an older version.
                    if *current_version == 0 || doc_version < current_version {
                        continue;
                    }
                    local_indices[up_to_date_index] = index;
                    up_to_date_index += 1;
                }
            }
            // If size is within threshold, write the new posting back and return.
            if up_to_date_index <= self.params.split_threshold as usize {
                for idx in 0..up_to_date_index {
                    if local_indices[idx] == idx {
                        continue;
                    }
                    doc_offset_ids[idx] = doc_offset_ids[local_indices[idx]];
                    doc_versions[idx] = doc_versions[local_indices[idx]];
                    doc_embeddings.copy_within(
                        local_indices[idx] * self.dimensionality
                            ..(local_indices[idx] + 1) * self.dimensionality,
                        idx * self.dimensionality,
                    );
                }
                doc_offset_ids.truncate(up_to_date_index);
                doc_versions.truncate(up_to_date_index);
                doc_embeddings.truncate(up_to_date_index * self.dimensionality);
                let posting_list = SpannPostingList {
                    doc_offset_ids: &doc_offset_ids,
                    doc_versions: &doc_versions,
                    doc_embeddings: &doc_embeddings,
                };
                // The only case when this can happen is if the point that is being appended
                // was concurrently reassigned and its version incremented. In this case,
                // we can safely ignore the append since the reassign will take care of
                // adding the point to the correct heads.
                if doc_offset_ids.is_empty() {
                    tracing::info!(
                        "Point {} at version {} was concurrently updated. Empty posting list after appending",
                        id, version
                    );
                    return Ok(());
                }
                write_guard
                    .set("", head_id, &posting_list)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error setting posting list for head {}: {}", head_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
                self.stats
                    .num_pl_modified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                return Ok(());
            }
            self.stats
                .num_splits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                "Splitting posting list of head {} since it exceeds threshold in lieu of appending point {} at version {}",
                head_id, id, version
            );
            // Otherwise split the posting list.
            local_indices.truncate(up_to_date_index);
            // Shuffle local_indices.
            local_indices.shuffle(&mut rand::thread_rng());
            let last = local_indices.len();
            // Prepare KMeans.
            let mut kmeans_input = KMeansAlgorithmInput::new(
                local_indices,
                &doc_embeddings,
                self.dimensionality,
                /* k */ 2,
                /* first */ 0,
                last,
                self.params.num_samples_kmeans,
                self.params.space.clone().into(),
                self.params.initial_lambda,
            );
            clustering_output = cluster(&mut kmeans_input).map_err(|e| {
                tracing::error!("Error clustering posting list for head {}: {}", head_id, e);
                SpannIndexWriterError::KMeansClusteringError(e)
            })?;
            // TODO(Sanket): Not sure how this can happen. The reference implementation
            // just includes one point from the entire list in this case.
            // My guess is that this can happen only when the total number of points
            // that need to be split is 1.
            if clustering_output.num_clusters <= 1 {
                tracing::warn!("Clustering split the posting list into only 1 cluster");
                let mut single_doc_offset_ids = Vec::with_capacity(1);
                let mut single_doc_versions = Vec::with_capacity(1);
                let mut single_doc_embeddings = Vec::with_capacity(self.dimensionality);
                let label = clustering_output.cluster_labels.iter().nth(0);
                match label {
                    Some((index, _)) => {
                        single_doc_offset_ids.push(doc_offset_ids[*index]);
                        single_doc_versions.push(doc_versions[*index]);
                        single_doc_embeddings.extend_from_slice(
                            &doc_embeddings
                                [*index * self.dimensionality..(*index + 1) * self.dimensionality],
                        );
                    }
                    None => {
                        tracing::warn!("No points in the posting list");
                        return Ok(());
                    }
                }
                let single_posting_list = SpannPostingList {
                    doc_offset_ids: &single_doc_offset_ids,
                    doc_versions: &single_doc_versions,
                    doc_embeddings: &single_doc_embeddings,
                };
                write_guard
                    .set("", head_id, &single_posting_list)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error setting posting list for head {}: {}", head_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
                self.stats
                    .num_pl_modified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            } else {
                // None of the cluster_counts should be 0. Points to some error if it is.
                if clustering_output.cluster_counts.iter().any(|&x| x == 0) {
                    tracing::error!("Zero points in a cluster after clustering");
                    return Err(SpannIndexWriterError::KMeansClusteringError(
                        KMeansError::ZeroPointsInCluster,
                    ));
                }
                new_posting_lists.push(Vec::with_capacity(
                    clustering_output.cluster_counts[0] * self.dimensionality,
                ));
                new_posting_lists.push(Vec::with_capacity(
                    clustering_output.cluster_counts[1] * self.dimensionality,
                ));
                new_doc_offset_ids.push(Vec::with_capacity(clustering_output.cluster_counts[0]));
                new_doc_offset_ids.push(Vec::with_capacity(clustering_output.cluster_counts[1]));
                new_doc_versions.push(Vec::with_capacity(clustering_output.cluster_counts[0]));
                new_doc_versions.push(Vec::with_capacity(clustering_output.cluster_counts[1]));
                for (index, cluster) in clustering_output.cluster_labels {
                    new_doc_offset_ids[cluster as usize].push(doc_offset_ids[index]);
                    new_doc_versions[cluster as usize].push(doc_versions[index]);
                    new_posting_lists[cluster as usize].extend_from_slice(
                        &doc_embeddings
                            [index * self.dimensionality..(index + 1) * self.dimensionality],
                    );
                }
                let mut same_head = false;
                let distance_function: DistanceFunction = self.params.space.clone().into();
                for k in 0..2 {
                    // Update the existing head.
                    if !same_head
                        && distance_function
                            .distance(&clustering_output.cluster_centers[k], &head_embedding)
                            < 1e-6
                    {
                        tracing::debug!(
                            "One of the heads remains the same id {} after splitting in lieu of adding point {} at version {}",
                            head_id, id, version
                        );
                        same_head = true;
                        let posting_list = SpannPostingList {
                            doc_offset_ids: &new_doc_offset_ids[k],
                            doc_versions: &new_doc_versions[k],
                            doc_embeddings: &new_posting_lists[k],
                        };
                        write_guard
                            .set("", head_id, &posting_list)
                            .await
                            .map_err(|e| {
                                tracing::error!(
                                    "Error setting posting list for head {}: {}",
                                    head_id,
                                    e
                                );
                                SpannIndexWriterError::PostingListSetError(e)
                            })?;
                        self.stats
                            .num_pl_modified
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        new_head_ids[k] = head_id as i32;
                        new_head_embeddings[k] = Some(&head_embedding);
                    } else {
                        // Create new head.
                        let next_id = self
                            .next_head_id
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!(
                            "Creating new head {}, old head {} in lieu of adding point {} at version {}",
                            next_id, head_id, id, version
                        );
                        let posting_list = SpannPostingList {
                            doc_offset_ids: &new_doc_offset_ids[k],
                            doc_versions: &new_doc_versions[k],
                            doc_embeddings: &new_posting_lists[k],
                        };
                        // Insert to postings list.
                        write_guard
                            .set("", next_id, &posting_list)
                            .await
                            .map_err(|e| {
                                tracing::error!(
                                    "Error setting posting list for head {}: {}",
                                    head_id,
                                    e
                                );
                                SpannIndexWriterError::PostingListSetError(e)
                            })?;
                        self.stats
                            .num_pl_modified
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        new_head_ids[k] = next_id as i32;
                        new_head_embeddings[k] = Some(&clustering_output.cluster_centers[k]);
                        // Insert to hnsw now.
                        let mut hnsw_write_guard = self.hnsw_index.inner.write();
                        let hnsw_len = hnsw_write_guard.len_with_deleted();
                        let hnsw_capacity = hnsw_write_guard.capacity();
                        if hnsw_len + 1 > hnsw_capacity {
                            tracing::info!("Resizing hnsw index to {}", hnsw_capacity * 2);
                            hnsw_write_guard.resize(hnsw_capacity * 2).map_err(|e| {
                                tracing::error!(
                                    "Error resizing hnsw index during append to {}: {}",
                                    hnsw_capacity * 2,
                                    e
                                );
                                SpannIndexWriterError::HnswIndexResizeError(e)
                            })?;
                        }
                        hnsw_write_guard
                            .add(next_id as usize, &clustering_output.cluster_centers[k])
                            .map_err(|e| {
                                tracing::error!(
                                    "Error adding new head {} to hnsw index: {}",
                                    next_id,
                                    e
                                );
                                SpannIndexWriterError::HnswIndexMutateError(e)
                            })?;
                        self.stats
                            .num_heads_created
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                if !same_head {
                    tracing::debug!(
                        "Deleting head {} after splitting in lieu of adding point {} at version {}",
                        head_id,
                        id,
                        version
                    );
                    // Delete the old head
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard.delete(head_id as usize).map_err(|e| {
                        tracing::error!("Error deleting head {} from hnsw index: {}", head_id, e);
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
                    self.stats
                        .num_heads_deleted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        // Reassign code.
        // The Box::pin is to make compiler happy since this code is
        // async recursive.
        Box::pin(self.collect_and_reassign(
            &new_head_ids,
            &new_head_embeddings,
            &head_embedding,
            &new_doc_offset_ids,
            &new_doc_versions,
            &new_posting_lists,
        ))
        .await
    }

    async fn add_to_postings_list(
        &self,
        id: u32,
        version: u32,
        embeddings: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        let (ids, _, head_embeddings) = self.rng_query(embeddings).await?;
        // The only cases when this can happen is initially when no data exists in the
        // index or if all the data that was added to the index was deleted later.
        // In both the cases, in the worst case, it can happen that ids is empty
        // for the first few points getting inserted concurrently by different threads.
        // It's fine to create new centers for each of them since the number of such points
        // will be very small and we can also run GC to merge them later if needed.
        if ids.is_empty() {
            tracing::info!(
                "No nearby heads found for adding {} at version {}. Creating a new head",
                id,
                version
            );
            let next_id = self
                .next_head_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                "Created new head {} in lieu of adding point {} at version {}",
                next_id,
                id,
                version
            );
            // First add to postings list then to hnsw. This order is important
            // to ensure that if and when the center is discoverable, it also exists
            // in the postings list. Otherwise, it will be a dangling center.
            {
                let posting_list = SpannPostingList {
                    doc_offset_ids: &[id],
                    doc_versions: &[version],
                    doc_embeddings: embeddings,
                };
                let write_guard = self.posting_list_writer.lock().await;
                write_guard
                    .set("", next_id, &posting_list)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error setting posting list for head {}: {}", next_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
                self.stats
                    .num_pl_modified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            // Next add to hnsw.
            {
                let mut write_guard = self.hnsw_index.inner.write();
                let hnsw_len = write_guard.len_with_deleted();
                let hnsw_capacity = write_guard.capacity();
                if hnsw_len + 1 > hnsw_capacity {
                    tracing::info!("Resizing hnsw index to {}", hnsw_capacity * 2);
                    write_guard.resize(hnsw_capacity * 2).map_err(|e| {
                        tracing::error!(
                            "Error resizing hnsw index during append to {}: {}",
                            hnsw_capacity * 2,
                            e
                        );
                        SpannIndexWriterError::HnswIndexResizeError(e)
                    })?;
                }
                write_guard.add(next_id as usize, embeddings).map_err(|e| {
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
        for (head_id, head_embedding) in ids.iter().zip(head_embeddings) {
            Box::pin(self.append(*head_id as u32, id, version, embeddings, head_embedding)).await?;
        }

        Ok(())
    }

    pub async fn add(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        let version = self.add_versions_map(id).await;
        // Normalize the embedding in case of cosine.
        let mut normalized_embedding = embedding.to_vec();
        let distance_function: DistanceFunction = self.params.space.clone().into();
        if distance_function == DistanceFunction::Cosine {
            normalized_embedding = normalize(embedding);
        }
        // Add to the posting list.
        self.add_to_postings_list(id, version, &normalized_embedding)
            .await
    }

    pub async fn update(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        let inc_version;
        {
            // Increment version.
            let mut version_map_guard = self.versions_map.write().await;
            let curr_version = match version_map_guard.versions_map.get(&id) {
                Some(version) => *version,
                None => {
                    tracing::error!("Point {} not found in version map", id);
                    return Err(SpannIndexWriterError::VersionNotFound);
                }
            };
            if curr_version == 0 {
                tracing::error!("Trying to update a deleted point {}", id);
                return Err(SpannIndexWriterError::VersionNotFound);
            }
            inc_version = curr_version + 1;
            version_map_guard.versions_map.insert(id, inc_version);
        }
        // Normalize the embedding in case of cosine.
        let mut normalized_embedding = embedding.to_vec();
        let distance_function: DistanceFunction = self.params.space.clone().into();
        if distance_function == DistanceFunction::Cosine {
            normalized_embedding = normalize(embedding);
        }
        // Add to the posting list.
        self.add_to_postings_list(id, inc_version, &normalized_embedding)
            .await
    }

    pub async fn delete(&self, id: u32) -> Result<(), SpannIndexWriterError> {
        let mut version_map_guard = self.versions_map.write().await;
        version_map_guard.versions_map.insert(id, 0);
        Ok(())
    }

    async fn get_up_to_date_count(
        &self,
        doc_offset_ids: &[u32],
        doc_versions: &[u32],
    ) -> Result<usize, SpannIndexWriterError> {
        let mut up_to_date_index = 0;
        let version_map_guard = self.versions_map.read().await;
        for (index, doc_version) in doc_versions.iter().enumerate() {
            let current_version = version_map_guard
                .versions_map
                .get(&doc_offset_ids[index])
                .ok_or(SpannIndexWriterError::VersionNotFound)?;
            // disregard if either deleted or on an older version.
            if *current_version == 0 || doc_version < current_version {
                continue;
            }
            up_to_date_index += 1;
        }
        Ok(up_to_date_index)
    }

    async fn is_head_deleted(&self, head_id: usize) -> Result<bool, SpannIndexWriterError> {
        let hnsw_read_guard = self.hnsw_index.inner.read();
        let hnsw_emb = hnsw_read_guard.get(head_id);
        // TODO(Sanket): Check for exact error.
        // TODO(Sanket): We should get this information from hnswlib and not rely on error.
        if hnsw_emb.is_err() || hnsw_emb.unwrap().is_none() {
            return Ok(true);
        }
        Ok(false)
    }

    async fn remove_outdated_entries(
        &self,
        mut doc_offset_ids: Vec<u32>,
        mut doc_versions: Vec<u32>,
        mut doc_embeddings: Vec<f32>,
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<f32>), SpannIndexWriterError> {
        let mut cluster_len = 0;
        let mut local_indices = vec![0; doc_offset_ids.len()];
        {
            let version_map_guard = self.versions_map.read().await;
            for (index, doc_version) in doc_versions.iter().enumerate() {
                let current_version = version_map_guard
                    .versions_map
                    .get(&doc_offset_ids[index])
                    .ok_or(SpannIndexWriterError::VersionNotFound)?;
                // disregard if either deleted or on an older version.
                if *current_version == 0 || doc_version < current_version {
                    continue;
                }
                local_indices[cluster_len] = index;
                cluster_len += 1;
            }
        }
        for idx in 0..cluster_len {
            if local_indices[idx] == idx {
                continue;
            }
            doc_offset_ids[idx] = doc_offset_ids[local_indices[idx]];
            doc_versions[idx] = doc_versions[local_indices[idx]];
            doc_embeddings.copy_within(
                local_indices[idx] * self.dimensionality
                    ..(local_indices[idx] + 1) * self.dimensionality,
                idx * self.dimensionality,
            );
        }
        doc_offset_ids.truncate(cluster_len);
        doc_versions.truncate(cluster_len);
        doc_embeddings.truncate(cluster_len * self.dimensionality);
        Ok((doc_offset_ids, doc_versions, doc_embeddings))
    }

    #[allow(clippy::too_many_arguments)]
    async fn merge_posting_lists(
        &self,
        mut source_doc_offset_ids: Vec<u32>,
        mut source_doc_versions: Vec<u32>,
        mut source_doc_embeddings: Vec<f32>,
        target_doc_offset_ids: Vec<u32>,
        target_doc_versions: Vec<u32>,
        target_doc_embeddings: Vec<f32>,
        target_cluster_len: usize,
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<f32>), SpannIndexWriterError> {
        source_doc_embeddings.reserve_exact(target_cluster_len);
        source_doc_versions.reserve_exact(target_cluster_len);
        source_doc_embeddings.reserve_exact(target_cluster_len * self.dimensionality);
        for (index, target_doc_offset_id) in target_doc_offset_ids.into_iter().enumerate() {
            if self
                .is_outdated(target_doc_offset_id, target_doc_versions[index])
                .await?
            {
                continue;
            }
            source_doc_offset_ids.push(target_doc_offset_id);
            source_doc_versions.push(target_doc_versions[index]);
            source_doc_embeddings.extend_from_slice(
                &target_doc_embeddings
                    [index * self.dimensionality..(index + 1) * self.dimensionality],
            );
        }
        Ok((
            source_doc_offset_ids,
            source_doc_versions,
            source_doc_embeddings,
        ))
    }

    // Note(Sanket): This has not been tested for running concurrently with
    // other add/update/delete operations.
    async fn garbage_collect_head(
        &self,
        head_id: usize,
        head_embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        // Get heads.
        let mut merged_with_a_nbr = false;
        let source_cluster_len;
        let mut target_cluster_len = 0;
        let mut doc_offset_ids;
        let mut doc_versions;
        let mut doc_embeddings;
        let mut target_embedding = vec![];
        let mut target_head = 0;
        {
            let pl_guard = self.posting_list_writer.lock().await;
            // If head is concurrently deleted then skip.
            if self.is_head_deleted(head_id).await? {
                return Ok(());
            }
            (doc_offset_ids, doc_versions, doc_embeddings) = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", head_id as u32)
                .await
                .map_err(|e| {
                    tracing::error!("Error getting posting list for head {}: {}", head_id, e);
                    SpannIndexWriterError::PostingListGetError(e)
                })?
                .ok_or(SpannIndexWriterError::PostingListNotFound)?;
            (doc_offset_ids, doc_versions, doc_embeddings) = self
                .remove_outdated_entries(doc_offset_ids, doc_versions, doc_embeddings)
                .await?;
            source_cluster_len = doc_offset_ids.len();
            // Write the PL back and return if within the merge threshold.
            if source_cluster_len > self.params.merge_threshold as usize {
                let posting_list = SpannPostingList {
                    doc_offset_ids: &doc_offset_ids,
                    doc_versions: &doc_versions,
                    doc_embeddings: &doc_embeddings,
                };
                pl_guard
                    .set("", head_id as u32, &posting_list)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error setting posting list for head {}: {}", head_id, e);
                        SpannIndexWriterError::PostingListSetError(e)
                    })?;
                self.stats
                    .num_pl_modified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                return Ok(());
            }
            if source_cluster_len == 0 {
                tracing::info!("Posting list of {} is empty. Deleting from hnsw", head_id);
                // Delete from hnsw.
                let hnsw_write_guard = self.hnsw_index.inner.write();
                hnsw_write_guard.delete(head_id).map_err(|e| {
                    tracing::error!("Error deleting head {} from hnsw index: {}", head_id, e);
                    SpannIndexWriterError::HnswIndexMutateError(e)
                })?;
                self.stats
                    .num_heads_deleted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
            // Find candidates for merge.
            let (nearest_head_ids, _, nearest_head_embeddings) = self
                .get_nearby_heads(head_embedding, self.params.num_centers_to_merge_to as usize)
                .await?;
            for (nearest_head_id, nearest_head_embedding) in nearest_head_ids
                .into_iter()
                .zip(nearest_head_embeddings.into_iter())
            {
                // Skip if it is the current head. Can't a merge a head into itself.
                if nearest_head_id == head_id {
                    continue;
                }
                // TODO(Sanket): If and when the lock is more fine grained, then
                // need to acquire a lock on the nearest_head_id here.
                // TODO(Sanket): Also need to check if the head is deleted concurrently then.
                let (
                    nearest_head_doc_offset_ids,
                    nearest_head_doc_versions,
                    nearest_head_doc_embeddings,
                ) = pl_guard
                    .get_owned::<u32, &SpannPostingList<'_>>("", nearest_head_id as u32)
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "Error getting posting list for head {}: {}",
                            nearest_head_id,
                            e
                        );
                        SpannIndexWriterError::PostingListGetError(e)
                    })?
                    .ok_or(SpannIndexWriterError::PostingListNotFound)?;
                target_cluster_len = self
                    .get_up_to_date_count(&nearest_head_doc_offset_ids, &nearest_head_doc_versions)
                    .await?;
                // If the total count exceeds the max posting list size then skip.
                if target_cluster_len + source_cluster_len >= self.params.split_threshold as usize {
                    continue;
                }
                // Merge the two PLs.
                (doc_offset_ids, doc_versions, doc_embeddings) = self
                    .merge_posting_lists(
                        doc_offset_ids,
                        doc_versions,
                        doc_embeddings,
                        nearest_head_doc_offset_ids,
                        nearest_head_doc_versions,
                        nearest_head_doc_embeddings,
                        target_cluster_len,
                    )
                    .await?;
                // Write the merged PL back.
                // Merge into the larger of the two clusters.
                let merged_posting_list = SpannPostingList {
                    doc_offset_ids: &doc_offset_ids,
                    doc_versions: &doc_versions,
                    doc_embeddings: &doc_embeddings,
                };
                if target_cluster_len > source_cluster_len {
                    pl_guard
                        .set("", nearest_head_id as u32, &merged_posting_list)
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                "Error setting posting list for head {}: {}",
                                head_id,
                                e
                            );
                            SpannIndexWriterError::PostingListSetError(e)
                        })?;
                    self.stats
                        .num_pl_modified
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Delete from hnsw.
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard.delete(head_id).map_err(|e| {
                        tracing::error!("Error deleting head {} from hnsw index: {}", head_id, e);
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
                    self.stats
                        .num_heads_deleted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    pl_guard
                        .set("", head_id as u32, &merged_posting_list)
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                "Error setting posting list for head {}: {}",
                                head_id,
                                e
                            );
                            SpannIndexWriterError::PostingListSetError(e)
                        })?;
                    self.stats
                        .num_pl_modified
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Delete from hnsw.
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard.delete(nearest_head_id).map_err(|e| {
                        tracing::error!(
                            "Error deleting head {} from hnsw index: {}",
                            nearest_head_id,
                            e
                        );
                        SpannIndexWriterError::HnswIndexMutateError(e)
                    })?;
                    self.stats
                        .num_heads_deleted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                // This center is now merged with a neighbor.
                target_head = nearest_head_id;
                target_embedding = nearest_head_embedding;
                merged_with_a_nbr = true;
                break;
            }
        }
        if !merged_with_a_nbr {
            return Ok(());
        }
        // Reassign points that were merged to neighbouring heads.
        if source_cluster_len > target_cluster_len {
            // target_cluster points were merged to source_cluster
            // so they are candidates for reassignment.
            let distance_function: DistanceFunction = self.params.space.clone().into();
            for idx in source_cluster_len..(source_cluster_len + target_cluster_len) {
                let origin_dist = distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    &target_embedding,
                );
                let new_dist = distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    head_embedding,
                );
                if new_dist > origin_dist {
                    self.reassign(
                        doc_offset_ids[idx],
                        doc_versions[idx],
                        &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                        head_id as u32,
                        ReassignReason::Merge,
                    )
                    .await?;
                }
            }
        } else {
            // source_cluster points were merged to target_cluster
            // so they are candidates for reassignment.
            let distance_function: DistanceFunction = self.params.space.clone().into();
            for idx in 0..source_cluster_len {
                let origin_dist = distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    head_embedding,
                );
                let new_dist = distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    &target_embedding,
                );
                if new_dist > origin_dist {
                    self.reassign(
                        doc_offset_ids[idx],
                        doc_versions[idx],
                        &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                        target_head as u32,
                        ReassignReason::Merge,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub fn eligible_to_gc(&mut self, threshold: f32) -> bool {
        let (len_with_deleted, len_without_deleted) = {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            (hnsw_read_guard.len_with_deleted(), hnsw_read_guard.len())
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
            )
            .await
            .map_err(|e| {
                tracing::error!("Error creating hnsw index during gc");
                SpannIndexWriterError::HnswIndexCreateError(*e)
            })?;
        {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            let mut clean_hnsw_write_guard = clean_hnsw.inner.write();
            let (non_deleted_heads, _) = hnsw_read_guard.get_all_ids().map_err(|e| {
                tracing::error!("Error getting all ids from hnsw index during gc: {}", e);
                SpannIndexWriterError::HnswIndexSearchError(e)
            })?;
            clean_hnsw_write_guard
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
                let hnsw_len = clean_hnsw_write_guard.len_with_deleted();
                let hnsw_capacity = clean_hnsw_write_guard.capacity();
                if hnsw_len + 1 > hnsw_capacity {
                    tracing::info!("Resizing hnsw index to {}", hnsw_capacity * 2);
                    clean_hnsw_write_guard
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
            (non_deleted_heads, _) = hnsw_read_guard.get_all_ids().map_err(|e| {
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
                return Ok(());
            }
            let head_embedding = self
                .hnsw_index
                .inner
                .read()
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
            tracing::debug!("Garbage collecting head {}", head_id);
            self.garbage_collect_head(*head_id, &head_embedding).await?;
        }
        Ok(())
    }

    // Note(Sanket): This has not been tested for running concurrently with
    // other add/update/delete operations.
    pub async fn garbage_collect(&mut self) -> Result<(), SpannIndexWriterError> {
        let attributes = &[KeyValue::new(
            "collection_id",
            self.collection_id.to_string(),
        )];
        let gc_latency_metric = self.metrics.gc_latency.clone();
        let stopwatch = Stopwatch::new(&gc_latency_metric, attributes);
        if self.gc_context.pl_context.enabled {
            match &self.gc_context.pl_context.policy {
                PlGarbageCollectionPolicy::RandomSample(random_sample) => {
                    self.pl_garbage_collect_random_sample(random_sample.sample_size)
                        .await?;
                }
            }
        }
        if self.gc_context.hnsw_context.enabled {
            match &self.gc_context.hnsw_context.policy {
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
        let attribute = &[KeyValue::new(
            "collection_id",
            self.collection_id.to_string(),
        )];
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
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
            attribute,
        );
    }

    pub async fn commit(self) -> Result<SpannIndexFlusher, SpannIndexWriterError> {
        self.emit_counters();
        // NOTE(Sanket): This is not the best way to drain the writer but the orchestrator keeps a
        // reference to the writer so cannot do an Arc::try_unwrap() here.
        // Pl list.
        let attribute = &[KeyValue::new(
            "collection_id",
            self.collection_id.to_string(),
        )];
        let pl_flusher = {
            let stopwatch = Stopwatch::new(&self.metrics.pl_commit_latency, attribute);
            let pl_writer_clone = self.posting_list_writer.lock().await.clone();
            let pl_flusher = pl_writer_clone
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
            let stopwatch = Stopwatch::new(&self.metrics.versions_map_commit_latency, attribute);
            // Versions map. Create a writer, write all the data and commit.
            let mut bf_options = BlockfileWriterOptions::new();
            bf_options = bf_options.unordered_mutations();
            let versions_map_bf_writer = self
                .blockfile_provider
                .write::<u32, u32>(bf_options)
                .await
                .map_err(|e| {
                    tracing::error!("Error creating versions map writer: {}", e);
                    SpannIndexWriterError::VersionsMapWriterCreateError(*e)
                })?;
            {
                let mut version_map_guard = self.versions_map.write().await;
                for (doc_offset_id, doc_version) in version_map_guard.versions_map.drain() {
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
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
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
        let hnsw_id = {
            let stopwatch = Stopwatch::new(&self.metrics.hnsw_commit_latency, attribute);
            let (hnsw_id, hnsw_index) = match self.cleaned_up_hnsw_index {
                Some(index) => {
                    tracing::info!("Committing cleaned up hnsw index");
                    let index_id = index.inner.read().id;
                    (index_id, index)
                }
                None => {
                    let index_id = self.hnsw_index.inner.read().id;
                    (index_id, self.hnsw_index)
                }
            };
            self.hnsw_provider.commit(hnsw_index).map_err(|e| {
                tracing::error!("Error committing hnsw index: {}", e);
                SpannIndexWriterError::HnswIndexCommitError(e)
            })?;
            tracing::info!(
                "Committed hnsw index in {} ms",
                stopwatch.elapsed_micros() / 1000
            );
            hnsw_id
        };

        Ok(SpannIndexFlusher {
            pl_flusher,
            versions_map_flusher,
            max_head_id_flusher,
            hnsw_id,
            hnsw_flusher: self.hnsw_provider,
            collection_id: self.collection_id,
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

struct SpannIndexFlusherMetrics {
    pl_flush_latency: opentelemetry::metrics::Histogram<u64>,
    versions_map_flush_latency: opentelemetry::metrics::Histogram<u64>,
    hnsw_flush_latency: opentelemetry::metrics::Histogram<u64>,
    num_pl_entries_flushed: opentelemetry::metrics::Counter<u64>,
    num_versions_map_entries_flushed: opentelemetry::metrics::Counter<u64>,
}

pub struct SpannIndexFlusher {
    pl_flusher: BlockfileFlusher,
    versions_map_flusher: BlockfileFlusher,
    max_head_id_flusher: BlockfileFlusher,
    hnsw_id: IndexUuid,
    hnsw_flusher: HnswIndexProvider,
    collection_id: CollectionUuid,
    metrics: SpannIndexFlusherMetrics,
}

#[derive(Debug)]
pub struct SpannIndexIds {
    pub pl_id: Uuid,
    pub versions_map_id: Uuid,
    pub max_head_id_id: Uuid,
    pub hnsw_id: IndexUuid,
}

impl SpannIndexFlusher {
    pub async fn flush(self) -> Result<SpannIndexIds, SpannIndexWriterError> {
        let res = SpannIndexIds {
            pl_id: self.pl_flusher.id(),
            versions_map_id: self.versions_map_flusher.id(),
            max_head_id_id: self.max_head_id_flusher.id(),
            hnsw_id: self.hnsw_id,
        };
        let attribute = &[KeyValue::new(
            "collection_id",
            self.collection_id.to_string(),
        )];
        {
            let stopwatch = Stopwatch::new(&self.metrics.pl_flush_latency, attribute);
            let num_pl_entries_flushed = self.pl_flusher.num_entries();
            self.pl_flusher
                .flush::<u32, &SpannPostingList<'_>>()
                .await
                .map_err(|e| {
                    tracing::error!("Error flushing posting list {}: {}", res.pl_id, e);
                    SpannIndexWriterError::PostingListFlushError(e)
                })?;
            self.metrics
                .num_pl_entries_flushed
                .add(num_pl_entries_flushed as u64, attribute);
            tracing::info!(
                "Flushed {} entries from posting list in {} ms",
                num_pl_entries_flushed,
                stopwatch.elapsed_micros() / 1000
            );
        }
        {
            let stopwatch = Stopwatch::new(&self.metrics.versions_map_flush_latency, attribute);
            let num_versions_map_entries_flushed = self.versions_map_flusher.num_entries();
            self.versions_map_flusher
                .flush::<u32, u32>()
                .await
                .map_err(|e| {
                    tracing::error!("Error flushing versions map {}: {}", res.versions_map_id, e);
                    SpannIndexWriterError::VersionsMapFlushError(e)
                })?;
            self.metrics
                .num_versions_map_entries_flushed
                .add(num_versions_map_entries_flushed as u64, attribute);
            tracing::info!(
                "Flushed {} entries from versions map in {} ms",
                num_versions_map_entries_flushed,
                stopwatch.elapsed_micros() / 1000
            );
        }
        self.max_head_id_flusher
            .flush::<&str, u32>()
            .await
            .map_err(|e| {
                tracing::error!("Error flushing max head id {}: {}", res.max_head_id_id, e);
                SpannIndexWriterError::MaxHeadIdFlushError(e)
            })?;
        {
            let stopwatch = Stopwatch::new(&self.metrics.hnsw_flush_latency, attribute);
            self.hnsw_flusher.flush(&self.hnsw_id).await.map_err(|e| {
                tracing::error!("Error flushing hnsw index {}: {}", res.hnsw_id, e);
                SpannIndexWriterError::HnswIndexFlushError(*e)
            })?;
            tracing::info!(
                "Flushed hnsw index {} in {} ms",
                res.hnsw_id,
                stopwatch.elapsed_micros() / 1000
            );
        }
        Ok(res)
    }
}

#[derive(Error, Debug)]
pub enum SpannIndexReaderError {
    #[error("Error creating/opening hnsw index {0}")]
    HnswIndexConstructionError(#[source] HnswIndexProviderOpenError),
    #[error("Error creating/opening postings list reader {0}")]
    PostingListReaderConstructionError(#[source] OpenError),
    #[error("Error creating/opening versions map reader")]
    VersionsMapReaderConstructionError(#[source] OpenError),
    #[error("Spann index uninitialized")]
    UninitializedIndex,
    #[error("Error reading posting list {0}")]
    PostingListReadError(#[source] Box<dyn ChromaError>),
    #[error("Posting list not found")]
    PostingListNotFound,
    #[error("Error reading versions map {0}")]
    VersionsMapReadError(#[source] Box<dyn ChromaError>),
    #[error("Versions map not found")]
    VersionsMapNotFound,
    #[error("Error scanning hnsw index {0}")]
    ScanHnswError(#[source] Box<dyn ChromaError>),
    #[error("Data inconsistency error")]
    DataInconsistencyError,
}

impl ChromaError for SpannIndexReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexConstructionError(e) => e.code(),
            Self::PostingListReaderConstructionError(e) => e.code(),
            Self::VersionsMapReaderConstructionError(e) => e.code(),
            Self::UninitializedIndex => ErrorCodes::Internal,
            Self::PostingListReadError(e) => e.code(),
            Self::PostingListNotFound => ErrorCodes::NotFound,
            Self::VersionsMapReadError(e) => e.code(),
            Self::VersionsMapNotFound => ErrorCodes::NotFound,
            Self::ScanHnswError(e) => e.code(),
            Self::DataInconsistencyError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug)]
pub struct SpannPosting {
    pub doc_offset_id: u32,
    pub doc_embedding: Vec<f32>,
}

#[derive(Clone, Debug)]
pub struct SpannIndexReader<'me> {
    pub posting_lists: BlockfileReader<'me, u32, SpannPostingList<'me>>,
    pub hnsw_index: HnswIndexRef,
    pub versions_map: BlockfileReader<'me, u32, u32>,
    pub dimensionality: usize,
}

impl<'me> SpannIndexReader<'me> {
    async fn hnsw_index_from_id(
        hnsw_provider: &HnswIndexProvider,
        id: &IndexUuid,
        cache_key: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        ef_search: usize,
    ) -> Result<HnswIndexRef, SpannIndexReaderError> {
        match hnsw_provider
            .open(
                id,
                cache_key,
                dimensionality as i32,
                distance_function,
                ef_search,
            )
            .await
        {
            Ok(index) => Ok(index),
            Err(e) => {
                tracing::error!("Error opening hnsw index{}: {}", id, e);
                Err(SpannIndexReaderError::HnswIndexConstructionError(*e))
            }
        }
    }

    async fn posting_list_reader_from_id(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileReader<'me, u32, SpannPostingList<'me>>, SpannIndexReaderError> {
        match blockfile_provider
            .read::<u32, SpannPostingList<'me>>(blockfile_id)
            .await
        {
            Ok(reader) => Ok(reader),
            Err(e) => {
                tracing::error!("Error opening posting list reader {}: {}", blockfile_id, e);
                Err(SpannIndexReaderError::PostingListReaderConstructionError(
                    *e,
                ))
            }
        }
    }

    async fn versions_map_reader_from_id(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileReader<'me, u32, u32>, SpannIndexReaderError> {
        match blockfile_provider.read::<u32, u32>(blockfile_id).await {
            Ok(reader) => Ok(reader),
            Err(e) => {
                tracing::error!("Error opening versions map reader {}: {}", blockfile_id, e);
                Err(SpannIndexReaderError::VersionsMapReaderConstructionError(
                    *e,
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_id(
        hnsw_id: Option<&IndexUuid>,
        hnsw_provider: &HnswIndexProvider,
        hnsw_cache_key: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        ef_search: usize,
        pl_blockfile_id: Option<&Uuid>,
        versions_map_blockfile_id: Option<&Uuid>,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<SpannIndexReader<'me>, SpannIndexReaderError> {
        let hnsw_reader = match hnsw_id {
            Some(hnsw_id) => {
                Self::hnsw_index_from_id(
                    hnsw_provider,
                    hnsw_id,
                    hnsw_cache_key,
                    distance_function,
                    dimensionality,
                    ef_search,
                )
                .await?
            }
            None => {
                return Err(SpannIndexReaderError::UninitializedIndex);
            }
        };
        let postings_list_reader = match pl_blockfile_id {
            Some(pl_id) => Self::posting_list_reader_from_id(pl_id, blockfile_provider).await?,
            None => return Err(SpannIndexReaderError::UninitializedIndex),
        };

        let versions_map_reader = match versions_map_blockfile_id {
            Some(versions_id) => {
                Self::versions_map_reader_from_id(versions_id, blockfile_provider).await?
            }
            None => return Err(SpannIndexReaderError::UninitializedIndex),
        };

        Ok(Self {
            posting_lists: postings_list_reader,
            hnsw_index: hnsw_reader,
            versions_map: versions_map_reader,
            dimensionality,
        })
    }

    async fn is_outdated(
        &self,
        doc_offset_id: u32,
        doc_version: u32,
    ) -> Result<bool, SpannIndexReaderError> {
        let actual_version = self
            .versions_map
            .get("", doc_offset_id)
            .await
            .map_err(|e| {
                tracing::error!(
                    "Error getting version for doc offset id {}: {}",
                    doc_offset_id,
                    e
                );
                SpannIndexReaderError::VersionsMapReadError(e)
            })?
            .ok_or(SpannIndexReaderError::VersionsMapNotFound)?;
        Ok(actual_version == 0 || doc_version < actual_version)
    }

    pub async fn fetch_posting_list(
        &self,
        head_id: u32,
    ) -> Result<Vec<SpannPosting>, SpannIndexReaderError> {
        let res = self
            .posting_lists
            .get("", head_id)
            .await
            .map_err(|e| {
                tracing::error!("Error getting posting list for head {}: {}", head_id, e);
                SpannIndexReaderError::PostingListReadError(e)
            })?
            .ok_or(SpannIndexReaderError::PostingListNotFound)?;

        let mut posting_lists = Vec::with_capacity(res.doc_offset_ids.len());
        let mut unique_ids = HashSet::new();
        for (index, doc_offset_id) in res.doc_offset_ids.iter().enumerate() {
            if self
                .is_outdated(*doc_offset_id, res.doc_versions[index])
                .await?
            {
                continue;
            }
            if unique_ids.contains(doc_offset_id) {
                continue;
            }
            unique_ids.insert(*doc_offset_id);
            posting_lists.push(SpannPosting {
                doc_offset_id: *doc_offset_id,
                doc_embedding: res.doc_embeddings
                    [index * self.dimensionality..(index + 1) * self.dimensionality]
                    .to_vec(),
            });
        }
        Ok(posting_lists)
    }

    // Only for testing purposes as of 5 March 2024.
    // Returns all the ids with embeddings.
    // Intentionally dumb and not paginated.
    pub async fn scan(&self) -> Result<Vec<SpannPosting>, SpannIndexReaderError> {
        // Get all the heads.
        let (non_deleted_heads, _) = self.hnsw_index.inner.read().get_all_ids().map_err(|e| {
            tracing::error!("Error getting all ids from hnsw index during scan: {}", e);
            SpannIndexReaderError::ScanHnswError(e)
        })?;
        let mut postings_map: HashMap<u32, Vec<f32>> = HashMap::new();
        for head in non_deleted_heads {
            let res = self
                .posting_lists
                .get("", head as u32)
                .await
                .map_err(|e| {
                    tracing::error!("Error getting posting list for head {}: {}", head, e);
                    SpannIndexReaderError::PostingListReadError(e)
                })?
                .ok_or(SpannIndexReaderError::PostingListNotFound)?;
            for (index, doc_offset_id) in res.doc_offset_ids.iter().enumerate() {
                if self
                    .is_outdated(*doc_offset_id, res.doc_versions[index])
                    .await?
                {
                    continue;
                }
                // Deduplicate.
                if let Some(posting) = postings_map.get(doc_offset_id) {
                    // values should be same.
                    if posting
                        != &res.doc_embeddings
                            [index * self.dimensionality..(index + 1) * self.dimensionality]
                    {
                        let actual_version = self
                            .versions_map
                            .get("", *doc_offset_id)
                            .await
                            .map_err(|e| {
                                tracing::error!(
                                    "Error getting version for doc offset id {}: {}",
                                    doc_offset_id,
                                    e
                                );
                                SpannIndexReaderError::VersionsMapReadError(e)
                            })?
                            .ok_or(SpannIndexReaderError::VersionsMapNotFound)?;
                        tracing::error!("Duplicate doc offset id {} at latest version {} with different embeddings", doc_offset_id, actual_version);
                        return Err(SpannIndexReaderError::DataInconsistencyError);
                    }
                    continue;
                }
                postings_map.insert(
                    *doc_offset_id,
                    res.doc_embeddings
                        [index * self.dimensionality..(index + 1) * self.dimensionality]
                        .to_vec(),
                );
            }
        }
        let mut postings = Vec::with_capacity(postings_map.len());
        for (doc_offset_id, embedding) in postings_map {
            postings.push(SpannPosting {
                doc_offset_id,
                doc_embedding: embedding,
            });
        }
        Ok(postings)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, f32::consts::PI, path::PathBuf, sync::Arc};

    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_config::{registry::Registry, Configurable};
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, InternalSpannConfiguration, SpannPostingList};
    use rand::Rng;
    use tempfile::TempDir;

    use crate::{
        config::{
            HnswGarbageCollectionConfig, HnswGarbageCollectionPolicyConfig,
            PlGarbageCollectionConfig, PlGarbageCollectionPolicyConfig, RandomSamplePolicyConfig,
        },
        hnsw_provider::HnswIndexProvider,
        spann::types::{
            GarbageCollectionContext, SpannIndexReader, SpannIndexWriter, SpannIndexWriterError,
            SpannMetrics,
        },
        Index,
    };

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
        let writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            SpannMetrics::default(),
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
            assert_eq!(hnsw_read_guard.len(), 1);
            let emb = hnsw_read_guard
                .get(1)
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
        }
        {
            // Posting list should have 100 points.
            let pl_read_guard = writer.posting_list_writer.lock().await;
            let pl = pl_read_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
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
            assert_eq!(hnsw_read_guard.len(), 2);
            emb_2_id = 2;
            // Head could be 2 and 3 or 1 and 2.
            if hnsw_read_guard.get(1).is_err() {
                emb_1_id = 3;
            } else {
                emb_1_id = 1;
            }
        }
        {
            // Posting list should have 100 points.
            let pl_read_guard = writer.posting_list_writer.lock().await;
            let pl1 = pl_read_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", emb_1_id)
                .await
                .expect("Error getting posting list")
                .unwrap();
            let pl2 = pl_read_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", emb_2_id)
                .await
                .expect("Error getting posting list")
                .unwrap();
            // Only two combinations possible.
            if pl1.0.len() == 100 {
                assert_eq!(pl1.1.len(), 100);
                assert_eq!(pl1.2.len(), 200);
                assert_eq!(pl2.0.len(), 1);
                assert_eq!(pl2.1.len(), 1);
                assert_eq!(pl2.2.len(), 2);
            } else if pl2.0.len() == 100 {
                assert_eq!(pl2.1.len(), 100);
                assert_eq!(pl2.2.len(), 200);
                assert_eq!(pl1.0.len(), 1);
                assert_eq!(pl1.1.len(), 1);
                assert_eq!(pl1.2.len(), 2);
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
            assert_eq!(hnsw_read_guard.len(), 2);
            emb_2_id = 2;
            // Head could be 2 and 3 or 1 and 2.
            if hnsw_read_guard.get(1).is_err() {
                emb_1_id = 3;
            } else {
                emb_1_id = 1;
            }
        }
        {
            // Posting list should have 100 points.
            let pl_read_guard = writer.posting_list_writer.lock().await;
            let pl = pl_read_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", emb_1_id)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
            let pl = pl_read_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", emb_2_id)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
        }
    }

    #[tokio::test]
    async fn test_gc_deletes() {
        // Insert a few entries in a couple of centers. Delete a few
        // still keeping within the merge threshold.
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
        let dimensionality = 2;
        let params = InternalSpannConfiguration {
            split_threshold: 100,
            reassign_neighbor_count: 8,
            merge_threshold: 50,
            max_neighbors: 16,
            ..Default::default()
        };
        let pl_gc_policy = PlGarbageCollectionConfig {
            enabled: true,
            policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig {
                sample_size: 1.0,
            }),
        };
        let hnsw_gc_policy = HnswGarbageCollectionConfig {
            enabled: true,
            policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(pl_gc_policy, hnsw_gc_policy),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let mut writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann index writer");
        // Insert a couple of centers.
        {
            let hnsw_guard = writer.hnsw_index.inner.write();
            hnsw_guard
                .add(1, &[0.0, 0.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(2, &[1000.0, 1000.0])
                .expect("Error adding to hnsw index");
        }
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            let mut doc_offset_ids = vec![0u32; 100];
            let mut doc_versions = vec![0; 100];
            let mut doc_embeddings = vec![0.0; 200];
            // Insert 100 points in each of the centers.
            for point in 1..=100 {
                doc_offset_ids[point - 1] = point as u32;
                doc_versions[point - 1] = 1;
                doc_embeddings[(point - 1) * 2] = point as f32;
                doc_embeddings[(point - 1) * 2 + 1] = point as f32;
            }
            let pl = SpannPostingList {
                doc_offset_ids: &doc_offset_ids,
                doc_versions: &doc_versions,
                doc_embeddings: &doc_embeddings,
            };
            pl_guard
                .set("", 1, &pl)
                .await
                .expect("Error writing to posting list");
            for point in 1..=100 {
                doc_offset_ids[point - 1] = 100 + point as u32;
                doc_versions[point - 1] = 1;
                doc_embeddings[(point - 1) * 2] = 1000.0 + point as f32;
                doc_embeddings[(point - 1) * 2 + 1] = 1000.0 + point as f32;
            }
            let pl = SpannPostingList {
                doc_offset_ids: &doc_offset_ids,
                doc_versions: &doc_versions,
                doc_embeddings: &doc_embeddings,
            };
            pl_guard
                .set("", 2, &pl)
                .await
                .expect("Error writing to posting list");
        }
        // Insert the points in the version map as well.
        {
            let mut version_map_guard = writer.versions_map.write().await;
            for point in 1..=100 {
                version_map_guard.versions_map.insert(point as u32, 1);
                version_map_guard.versions_map.insert(100 + point as u32, 1);
            }
        }
        // Delete 40 points each from the centers.
        for point in 1..=40 {
            writer
                .delete(point)
                .await
                .expect("Error deleting from spann index writer");
            writer
                .delete(100 + point)
                .await
                .expect("Error deleting from spann index writer");
        }
        // Expect the version map to be properly updated.
        {
            let version_map_guard = writer.versions_map.read().await;
            for point in 1..=40 {
                assert_eq!(version_map_guard.versions_map.get(&point), Some(&0));
                assert_eq!(version_map_guard.versions_map.get(&(100 + point)), Some(&0));
            }
            // For the other 60 points, the version should be 1.
            for point in 41..=100 {
                assert_eq!(version_map_guard.versions_map.get(&point), Some(&1));
                assert_eq!(version_map_guard.versions_map.get(&(100 + point)), Some(&1));
            }
        }
        {
            // The posting lists should not be changed at all.
            let pl_guard = writer.posting_list_writer.lock().await;
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 2)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
        }
        // Now garbage collect.
        writer
            .garbage_collect()
            .await
            .expect("Error garbage collecting");
        // Expect the posting lists to be 60. Also validate the ids, versions and embeddings
        // individually.
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 60);
            assert_eq!(pl.1.len(), 60);
            assert_eq!(pl.2.len(), 120);
            for point in 41..=100 {
                assert_eq!(pl.0[point - 41], point as u32);
                assert_eq!(pl.1[point - 41], 1);
                assert_eq!(pl.2[(point - 41) * 2], point as f32);
                assert_eq!(pl.2[(point - 41) * 2 + 1], point as f32);
            }
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 2)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 60);
            assert_eq!(pl.1.len(), 60);
            assert_eq!(pl.2.len(), 120);
            for point in 41..=100 {
                assert_eq!(pl.0[point - 41], 100 + point as u32);
                assert_eq!(pl.1[point - 41], 1);
                assert_eq!(pl.2[(point - 41) * 2], 1000.0 + point as f32);
                assert_eq!(pl.2[(point - 41) * 2 + 1], 1000.0 + point as f32);
            }
        }
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.len(), 2);
            let (mut non_deleted_ids, deleted_ids) = hnsw_read_guard
                .get_all_ids()
                .expect("Error getting all ids");
            assert_eq!(non_deleted_ids.len(), 2);
            assert_eq!(deleted_ids.len(), 0);
            non_deleted_ids.sort();
            assert_eq!(non_deleted_ids[0], 1);
            assert_eq!(non_deleted_ids[1], 2);
            let emb = hnsw_read_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
            let emb = hnsw_read_guard
                .get(non_deleted_ids[1])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[1000.0, 1000.0]);
            assert!(writer.cleaned_up_hnsw_index.is_some());
            let cleaned_hnsw = writer
                .cleaned_up_hnsw_index
                .expect("Expected cleaned up hnsw index to be set");
            let cleaned_guard = cleaned_hnsw.inner.read();
            assert_eq!(cleaned_guard.len(), 2);
            let (mut non_deleted_ids, deleted_ids) =
                cleaned_guard.get_all_ids().expect("Error getting all ids");
            assert_eq!(non_deleted_ids.len(), 2);
            assert_eq!(deleted_ids.len(), 0);
            non_deleted_ids.sort();
            assert_eq!(non_deleted_ids[0], 1);
            assert_eq!(non_deleted_ids[1], 2);
            let emb = cleaned_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
            let emb = cleaned_guard
                .get(non_deleted_ids[1])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[1000.0, 1000.0]);
        }
    }

    #[tokio::test]
    async fn test_merge() {
        // Insert a few entries in a couple of centers. Delete a few
        // still keeping within the merge threshold.
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
        let dimensionality = 2;
        let params = InternalSpannConfiguration {
            split_threshold: 100,
            reassign_neighbor_count: 8,
            merge_threshold: 50,
            max_neighbors: 16,
            ..Default::default()
        };
        let pl_gc_policy = PlGarbageCollectionConfig {
            enabled: true,
            policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig {
                sample_size: 1.0,
            }),
        };
        let hnsw_gc_policy = HnswGarbageCollectionConfig {
            enabled: true,
            policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(pl_gc_policy, hnsw_gc_policy),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let mut writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann index writer");
        // Insert a couple of centers.
        {
            let hnsw_guard = writer.hnsw_index.inner.write();
            hnsw_guard
                .add(1, &[0.0, 0.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(2, &[1000.0, 1000.0])
                .expect("Error adding to hnsw index");
        }
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            let mut doc_offset_ids = vec![0u32; 100];
            let mut doc_versions = vec![0; 100];
            let mut doc_embeddings = vec![0.0; 200];
            // Insert 100 points in each of the centers.
            for point in 1..=100 {
                doc_offset_ids[point - 1] = point as u32;
                doc_versions[point - 1] = 1;
                doc_embeddings[(point - 1) * 2] = point as f32;
                doc_embeddings[(point - 1) * 2 + 1] = point as f32;
            }
            let pl = SpannPostingList {
                doc_offset_ids: &doc_offset_ids,
                doc_versions: &doc_versions,
                doc_embeddings: &doc_embeddings,
            };
            pl_guard
                .set("", 1, &pl)
                .await
                .expect("Error writing to posting list");
            for point in 1..=100 {
                doc_offset_ids[point - 1] = 100 + point as u32;
                doc_versions[point - 1] = 1;
                doc_embeddings[(point - 1) * 2] = 1000.0 + point as f32;
                doc_embeddings[(point - 1) * 2 + 1] = 1000.0 + point as f32;
            }
            let pl = SpannPostingList {
                doc_offset_ids: &doc_offset_ids,
                doc_versions: &doc_versions,
                doc_embeddings: &doc_embeddings,
            };
            pl_guard
                .set("", 2, &pl)
                .await
                .expect("Error writing to posting list");
        }
        // Insert the points in the version map as well.
        {
            let mut version_map_guard = writer.versions_map.write().await;
            for point in 1..=100 {
                version_map_guard.versions_map.insert(point as u32, 1);
                version_map_guard.versions_map.insert(100 + point as u32, 1);
            }
        }
        // Delete 60 points each from the centers. Since merge_threshold is 50, this should
        // trigger a merge between the two centers.
        for point in 1..=60 {
            writer
                .delete(point)
                .await
                .expect("Error deleting from spann index writer");
            writer
                .delete(100 + point)
                .await
                .expect("Error deleting from spann index writer");
        }
        // Just one more point from the latter center.
        writer
            .delete(100 + 61)
            .await
            .expect("Error deleting from spann index writer");
        // Expect the version map to be properly updated.
        {
            let version_map_guard = writer.versions_map.read().await;
            for point in 1..=60 {
                assert_eq!(version_map_guard.versions_map.get(&point), Some(&0));
                assert_eq!(version_map_guard.versions_map.get(&(100 + point)), Some(&0));
            }
            // For the other 60 points, the version should be 1.
            for point in 61..=100 {
                assert_eq!(version_map_guard.versions_map.get(&point), Some(&1));
                if point == 61 {
                    assert_eq!(version_map_guard.versions_map.get(&(100 + point)), Some(&0));
                } else {
                    assert_eq!(version_map_guard.versions_map.get(&(100 + point)), Some(&1));
                }
            }
        }
        {
            // The posting lists should not be changed at all.
            let pl_guard = writer.posting_list_writer.lock().await;
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 2)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
        }
        // Now garbage collect.
        writer
            .garbage_collect()
            .await
            .expect("Error garbage collecting");
        // Expect only one center now. [0.0, 0.0]
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.len(), 1);
            let (non_deleted_ids, deleted_ids) = hnsw_read_guard
                .get_all_ids()
                .expect("Error getting all ids");
            assert_eq!(non_deleted_ids.len(), 1);
            assert_eq!(deleted_ids.len(), 1);
            assert_eq!(non_deleted_ids[0], 1);
            assert_eq!(deleted_ids[0], 2);
            let emb = hnsw_read_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
            assert!(writer.cleaned_up_hnsw_index.is_some());
            let cleaned_hnsw = writer
                .cleaned_up_hnsw_index
                .expect("Expected cleaned up hnsw index to be set");
            let cleaned_guard = cleaned_hnsw.inner.read();
            assert_eq!(cleaned_guard.len(), 1);
            let (non_deleted_ids, deleted_ids) =
                cleaned_guard.get_all_ids().expect("Error getting all ids");
            assert_eq!(non_deleted_ids.len(), 1);
            assert_eq!(deleted_ids.len(), 0);
            assert_eq!(non_deleted_ids[0], 1);
            let emb = cleaned_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
        }
        // Expect the posting lists with id 1 to be 79.
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 79);
            assert_eq!(pl.1.len(), 79);
            assert_eq!(pl.2.len(), 158);
        }
    }

    #[tokio::test]
    async fn test_reassign() {
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
        let writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann index writer");
        // Create three centers with ill placed points.
        {
            let hnsw_guard = writer.hnsw_index.inner.write();
            hnsw_guard
                .add(1, &[0.0, 0.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(2, &[1000.0, 1000.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(3, &[10000.0, 10000.0])
                .expect("Error adding to hnsw index");
        }
        // Insert 50 points within a radius of 1 to center 1.
        let mut split_doc_offset_ids1 = vec![0u32; 50];
        let mut split_doc_versions1 = vec![0u32; 50];
        let mut split_doc_embeddings1 = vec![0.0; 100];
        let mut split_doc_offset_ids2 = vec![0u32; 50];
        let mut split_doc_versions2 = vec![0u32; 50];
        let mut split_doc_embeddings2 = vec![0.0; 100];
        let mut split_doc_offset_ids3 = vec![0u32; 50];
        let mut split_doc_versions3 = vec![0u32; 50];
        let mut split_doc_embeddings3 = vec![0.0; 100];
        {
            let mut rng = rand::thread_rng();
            let pl_guard = writer.posting_list_writer.lock().await;
            for i in 1..=50 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos();
                let y = r * theta.sin();

                split_doc_offset_ids1[i - 1] = i as u32;
                split_doc_versions1[i - 1] = 1;
                split_doc_embeddings1[(i - 1) * 2] = x;
                split_doc_embeddings1[(i - 1) * 2 + 1] = y;
            }
            let posting_list = SpannPostingList {
                doc_offset_ids: &split_doc_offset_ids1,
                doc_versions: &split_doc_versions1,
                doc_embeddings: &split_doc_embeddings1,
            };
            pl_guard
                .set("", 1, &posting_list)
                .await
                .expect("Error writing to posting list");
            // Insert 50 points within a radius of 1 to center 3 to center 2 and vice versa.
            // This ensures that we test reassignment and that it shuffles the two fully.
            for i in 1..=50 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos() + 1000.0;
                let y = r * theta.sin() + 1000.0;

                split_doc_offset_ids3[i - 1] = 50 + i as u32;
                split_doc_versions3[i - 1] = 1;
                split_doc_embeddings3[(i - 1) * 2] = x;
                split_doc_embeddings3[(i - 1) * 2 + 1] = y;
            }
            let posting_list = SpannPostingList {
                doc_offset_ids: &split_doc_offset_ids3,
                doc_versions: &split_doc_versions3,
                doc_embeddings: &split_doc_embeddings3,
            };
            pl_guard
                .set("", 3, &posting_list)
                .await
                .expect("Error writing to posting list");
            // Do the same for 10000.
            for i in 1..=50 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos() + 10000.0;
                let y = r * theta.sin() + 10000.0;

                split_doc_offset_ids2[i - 1] = 100 + i as u32;
                split_doc_versions2[i - 1] = 1;
                split_doc_embeddings2[(i - 1) * 2] = x;
                split_doc_embeddings2[(i - 1) * 2 + 1] = y;
            }
            let posting_list = SpannPostingList {
                doc_offset_ids: &split_doc_offset_ids2,
                doc_versions: &split_doc_versions2,
                doc_embeddings: &split_doc_embeddings2,
            };
            pl_guard
                .set("", 2, &posting_list)
                .await
                .expect("Error writing to posting list");
        }
        // Insert these 150 points to version map.
        {
            let mut version_map_guard = writer.versions_map.write().await;
            for i in 1..=150 {
                version_map_guard.versions_map.insert(i as u32, 1);
            }
        }
        // Trigger reassign and see the results.
        // Carefully construct the old head embedding so that NPA
        // is violated for the second center.
        writer
            .collect_and_reassign(
                &[1, 2],
                &[Some(&vec![0.0, 0.0]), Some(&vec![1000.0, 1000.0])],
                &[5000.0, 5000.0],
                &[split_doc_offset_ids1.clone(), split_doc_offset_ids2.clone()],
                &[split_doc_versions1.clone(), split_doc_versions2.clone()],
                &[split_doc_embeddings1.clone(), split_doc_embeddings2.clone()],
            )
            .await
            .expect("Expected reassign to succeed");
        // See the reassigned points.
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            // Center 1 should remain unchanged.
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 50);
            assert_eq!(pl.1.len(), 50);
            assert_eq!(pl.2.len(), 100);
            for i in 1..=50 {
                assert_eq!(pl.0[i - 1], i as u32);
                assert_eq!(pl.1[i - 1], 1);
                assert_eq!(pl.2[(i - 1) * 2], split_doc_embeddings1[(i - 1) * 2]);
                assert_eq!(
                    pl.2[(i - 1) * 2 + 1],
                    split_doc_embeddings1[(i - 1) * 2 + 1]
                );
            }
            // Center 2 should get 50 points, all with version 2 migrating from center 3.
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 2)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 50);
            assert_eq!(pl.1.len(), 50);
            assert_eq!(pl.2.len(), 100);
            for i in 1..=50 {
                assert_eq!(pl.0[i - 1], 50 + i as u32);
                assert_eq!(pl.1[i - 1], 2);
                assert_eq!(pl.2[(i - 1) * 2], split_doc_embeddings3[(i - 1) * 2]);
                assert_eq!(
                    pl.2[(i - 1) * 2 + 1],
                    split_doc_embeddings3[(i - 1) * 2 + 1]
                );
            }
            // Center 3 should get 100 points. 50 points with version 1 which weere
            // originally in center 3 and 50 points with version 2 which were originally
            // in center 2.
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 3)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 100);
            assert_eq!(pl.1.len(), 100);
            assert_eq!(pl.2.len(), 200);
            for i in 1..=100 {
                assert_eq!(pl.0[i - 1], 50 + i as u32);
                if i <= 50 {
                    assert_eq!(pl.1[i - 1], 1);
                    assert_eq!(pl.2[(i - 1) * 2], split_doc_embeddings3[(i - 1) * 2]);
                    assert_eq!(
                        pl.2[(i - 1) * 2 + 1],
                        split_doc_embeddings3[(i - 1) * 2 + 1]
                    );
                } else {
                    assert_eq!(pl.1[i - 1], 2);
                    assert_eq!(pl.2[(i - 1) * 2], split_doc_embeddings2[(i - 51) * 2]);
                    assert_eq!(
                        pl.2[(i - 1) * 2 + 1],
                        split_doc_embeddings2[(i - 51) * 2 + 1]
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_reassign_merge() {
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
        let dimensionality = 2;
        let params = InternalSpannConfiguration {
            split_threshold: 100,
            reassign_neighbor_count: 8,
            merge_threshold: 50,
            max_neighbors: 16,
            ..Default::default()
        };
        let pl_gc_policy = PlGarbageCollectionConfig {
            enabled: true,
            policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig {
                sample_size: 1.0,
            }),
        };
        let hnsw_gc_policy = HnswGarbageCollectionConfig {
            enabled: true,
            policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(pl_gc_policy, hnsw_gc_policy),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let mut writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            &collection_id,
            dimensionality,
            &blockfile_provider,
            params,
            gc_context,
            SpannMetrics::default(),
        )
        .await
        .expect("Error creating spann index writer");
        // Create three centers. 2 of these are accurate wrt their centers and third
        // is ill placed.
        {
            let hnsw_guard = writer.hnsw_index.inner.write();
            hnsw_guard
                .add(1, &[0.0, 0.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(2, &[1000.0, 1000.0])
                .expect("Error adding to hnsw index");
            hnsw_guard
                .add(3, &[10000.0, 10000.0])
                .expect("Error adding to hnsw index");
        }
        let mut doc_offset_ids1 = vec![0u32; 70];
        let mut doc_versions1 = vec![0u32; 70];
        let mut doc_embeddings1 = vec![0.0; 140];
        let mut doc_offset_ids2 = vec![0u32; 20];
        let mut doc_versions2 = vec![0u32; 20];
        let mut doc_embeddings2 = vec![0.0; 40];
        let mut doc_offset_ids3 = vec![0u32; 70];
        let mut doc_versions3 = vec![0u32; 70];
        let mut doc_embeddings3 = vec![0.0; 140];
        {
            let mut rng = rand::thread_rng();
            let pl_guard = writer.posting_list_writer.lock().await;
            // Insert 70 points within a radius of 1 to center 1.
            for i in 1..=70 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos();
                let y = r * theta.sin();

                doc_offset_ids1[i - 1] = i as u32;
                doc_versions1[i - 1] = 1;
                doc_embeddings1[(i - 1) * 2] = x;
                doc_embeddings1[(i - 1) * 2 + 1] = y;
            }
            // Insert 20 points within a radius of 1 to center 2.
            for i in 71..=90 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos() + 10000.0;
                let y = r * theta.sin() + 10000.0;

                doc_offset_ids2[i - 71] = i as u32;
                doc_versions2[i - 71] = 1;
                doc_embeddings2[(i - 71) * 2] = x;
                doc_embeddings2[(i - 71) * 2 + 1] = y;
            }
            // Insert 70 points within a radius of 1 to center 3.
            for i in 91..=160 {
                // Generate random radius between 0 and 1
                let r = rng.gen::<f32>().sqrt(); // sqrt for uniform distribution

                // Generate random angle between 0 and 2π
                let theta = rng.gen::<f32>() * 2.0 * PI;

                // Convert to Cartesian coordinates
                let x = r * theta.cos() + 10000.0;
                let y = r * theta.sin() + 10000.0;

                doc_offset_ids3[i - 91] = i as u32;
                doc_versions3[i - 91] = 1;
                doc_embeddings3[(i - 91) * 2] = x;
                doc_embeddings3[(i - 91) * 2 + 1] = y;
            }
            let spann_posting_list = SpannPostingList {
                doc_offset_ids: &doc_offset_ids1,
                doc_versions: &doc_versions1,
                doc_embeddings: &doc_embeddings1,
            };
            pl_guard
                .set("", 1, &spann_posting_list)
                .await
                .expect("Error writing to posting list");
            let spann_posting_list = SpannPostingList {
                doc_offset_ids: &doc_offset_ids2,
                doc_versions: &doc_versions2,
                doc_embeddings: &doc_embeddings2,
            };
            pl_guard
                .set("", 2, &spann_posting_list)
                .await
                .expect("Error writing to posting list");
            let spann_posting_list = SpannPostingList {
                doc_offset_ids: &doc_offset_ids3,
                doc_versions: &doc_versions3,
                doc_embeddings: &doc_embeddings3,
            };
            pl_guard
                .set("", 3, &spann_posting_list)
                .await
                .expect("Error writing to posting list");
        }
        // Initialize the versions map appropriately.
        {
            let mut version_map_guard = writer.versions_map.write().await;
            for i in 1..=160 {
                version_map_guard.versions_map.insert(i as u32, 1);
            }
        }
        // Run a GC now.
        writer
            .garbage_collect()
            .await
            .expect("Error garbage collecting");
        // Run GC again to clean up the outdated points.
        writer
            .garbage_collect()
            .await
            .expect("Error garbage collecting");
        // check the posting lists.
        {
            let pl_guard = writer.posting_list_writer.lock().await;
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 1)
                .await
                .expect("Error getting posting list")
                .unwrap();
            assert_eq!(pl.0.len(), 70);
            assert_eq!(pl.1.len(), 70);
            assert_eq!(pl.2.len(), 140);
            for point in 1..=70 {
                assert_eq!(pl.0[point - 1], point as u32);
                assert_eq!(pl.1[point - 1], 1);
                assert_eq!(pl.2[(point - 1) * 2], doc_embeddings1[(point - 1) * 2]);
                assert_eq!(
                    pl.2[(point - 1) * 2 + 1],
                    doc_embeddings1[(point - 1) * 2 + 1]
                );
            }
            let pl = pl_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", 3)
                .await
                .expect("Error getting posting list")
                .unwrap();
            // PL3 should be 90.
            assert_eq!(pl.0.len(), 90);
            assert_eq!(pl.1.len(), 90);
            assert_eq!(pl.2.len(), 180);
            for point in 1..=70 {
                assert_eq!(pl.0[point - 1], 90 + point as u32);
                assert_eq!(pl.1[point - 1], 1);
                assert_eq!(pl.2[(point - 1) * 2], doc_embeddings3[(point - 1) * 2]);
                assert_eq!(
                    pl.2[(point - 1) * 2 + 1],
                    doc_embeddings3[(point - 1) * 2 + 1]
                );
            }
            for point in 71..=90 {
                assert_eq!(pl.0[point - 1], point as u32);
                assert_eq!(pl.1[point - 1], 2);
                assert_eq!(pl.2[(point - 1) * 2], doc_embeddings2[(point - 71) * 2]);
                assert_eq!(
                    pl.2[(point - 1) * 2 + 1],
                    doc_embeddings2[(point - 71) * 2 + 1]
                );
            }
        }
        // There should only be two heads.
        {
            let hnsw_read_guard = writer.hnsw_index.inner.read();
            assert_eq!(hnsw_read_guard.len(), 2);
            let (mut non_deleted_ids, deleted_ids) = hnsw_read_guard
                .get_all_ids()
                .expect("Error getting all ids");
            non_deleted_ids.sort();
            assert_eq!(non_deleted_ids.len(), 2);
            assert_eq!(deleted_ids.len(), 1);
            assert_eq!(non_deleted_ids[0], 1);
            assert_eq!(non_deleted_ids[1], 3);
            assert_eq!(deleted_ids[0], 2);
            let emb = hnsw_read_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
            let emb = hnsw_read_guard
                .get(non_deleted_ids[1])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[10000.0, 10000.0]);
            let cleaned_hnsw = writer
                .cleaned_up_hnsw_index
                .expect("Expected cleaned up hnsw index to be set");
            let cleaned_guard = cleaned_hnsw.inner.read();
            assert_eq!(cleaned_guard.len(), 2);
            let (mut non_deleted_ids, deleted_ids) =
                cleaned_guard.get_all_ids().expect("Error getting all ids");
            non_deleted_ids.sort();
            assert_eq!(non_deleted_ids.len(), 2);
            assert_eq!(deleted_ids.len(), 0);
            assert_eq!(non_deleted_ids[0], 1);
            assert_eq!(non_deleted_ids[1], 3);
            let emb = cleaned_guard
                .get(non_deleted_ids[0])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[0.0, 0.0]);
            let emb = cleaned_guard
                .get(non_deleted_ids[1])
                .expect("Error getting hnsw index")
                .unwrap();
            assert_eq!(emb, &[10000.0, 10000.0]);
        }
    }

    fn new_blockfile_provider_for_tests(
        max_block_size_bytes: usize,
        storage: Storage,
    ) -> BlockfileProvider {
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        );
        BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
    }

    fn new_hnsw_provider_for_tests(storage: Storage, temp_dir: &TempDir) -> HnswIndexProvider {
        let hnsw_cache = new_non_persistent_cache_for_test();
        HnswIndexProvider::new(
            storage,
            PathBuf::from(temp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        )
    }

    #[test]
    fn test_long_running_data_integrity() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Expected runtime to build");
        runtime.block_on(async {
            // Inserts 10k randomly generated embeddings each of 1000 dimensions.
            // Commits and flushes the data to disk. Then reads the data back using scan api
            // and verifies that all the data is present and correct.
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let max_block_size_bytes = 8 * 1024 * 1024;

            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let collection_id = CollectionUuid::new();
            let params = InternalSpannConfiguration {
                split_threshold: 100,
                reassign_neighbor_count: 8,
                merge_threshold: 50,
                max_neighbors: 16,
                ..Default::default()
            };
            let distance_function = params.space.clone().into();
            let ef_search = params.ef_search;
            let dimensionality = 1000;
            let gc_context = GarbageCollectionContext::try_from_config(
                &(
                    PlGarbageCollectionConfig::default(),
                    HnswGarbageCollectionConfig::default(),
                ),
                &Registry::default(),
            )
            .await
            .expect("Error converting config to gc context");
            let writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                None,
                None,
                None,
                None,
                &collection_id,
                dimensionality,
                &blockfile_provider,
                params,
                gc_context,
                SpannMetrics::default(),
            )
            .await
            .expect("Error creating spann index writer");
            let mut rng = rand::thread_rng();
            let mut doc_offset_ids = vec![0u32; 10000];
            let mut doc_embeddings: Vec<Vec<f32>> = Vec::new();
            for i in 1..=10000 {
                // Generate 1000 randomly generated f32.
                let embedding = (0..1000).map(|_| rng.gen::<f32>()).collect::<Vec<f32>>();
                writer
                    .add(i as u32, &embedding)
                    .await
                    .expect("Error adding to spann index writer");
                doc_offset_ids[i - 1] = i as u32;
                doc_embeddings.push(embedding);
            }
            let flusher = writer
                .commit()
                .await
                .expect("Error committing spann index writer");
            let paths = flusher
                .flush()
                .await
                .expect("Error flushing spann index writer");
            println!("Wrote 10k records of 1000 dimensions each");
            // Construct a reader.
            // Clear the cache.
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage);
            let reader = SpannIndexReader::from_id(
                Some(&paths.hnsw_id),
                &hnsw_provider,
                &collection_id,
                distance_function,
                dimensionality,
                ef_search,
                Some(&paths.pl_id),
                Some(&paths.versions_map_id),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            // Scan the reader and verify the data.
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            assert_eq!(results.len(), 10000);
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));

            for i in 0..10000 {
                assert_eq!(results[i].doc_offset_id, doc_offset_ids[i]);
                assert_eq!(results[i].doc_embedding, doc_embeddings[i].as_slice());
            }
        });
    }

    // NOTE(Sanket): It is non-trivial to use shuttle for this test since it requires
    // a tokio runtime for creating the hnsw provider - the cache requires a mpsc channel,
    // the construction of hnsw provider calls async tokio filesystem apis that also need
    // a runtime which is not supported by shuttle.
    #[test]
    fn test_long_running_data_integrity_parallel() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Expected runtime to build");
        runtime.block_on(async {
            // Inserts 10k randomly generated embeddings each of 1000 dimensions using 500 parallel tasks.
            // Commits and flushes the data to disk. Then reads the data back using scan api
            // and verifies that all the data is present and correct.
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let max_block_size_bytes = 8 * 1024 * 1024;

            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let collection_id = CollectionUuid::new();
            let params = InternalSpannConfiguration {
                split_threshold: 100,
                reassign_neighbor_count: 8,
                merge_threshold: 50,
                max_neighbors: 16,
                ..Default::default()
            };
            let distance_function = params.space.clone().into();
            let dimensionality = 1000;
            let ef_search = params.ef_search;
            let gc_context = GarbageCollectionContext::try_from_config(
                &(
                    PlGarbageCollectionConfig::default(),
                    HnswGarbageCollectionConfig::default(),
                ),
                &Registry::default(),
            )
            .await
            .expect("Error converting config to gc context");
            let writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                None,
                None,
                None,
                None,
                &collection_id,
                dimensionality,
                &blockfile_provider,
                params,
                gc_context,
                SpannMetrics::default(),
            )
            .await
            .expect("Error creating spann index writer");
            let mut rng = rand::thread_rng();
            let mut doc_offset_ids = vec![0u32; 10000];
            let mut doc_embeddings: Vec<Vec<f32>> = Vec::new();
            for i in 1..=10000 {
                // Generate 1000 randomly generated f32.
                let embedding = (0..1000).map(|_| rng.gen::<f32>()).collect::<Vec<f32>>();
                doc_offset_ids[i - 1] = i as u32;
                doc_embeddings.push(embedding);
            }
            let doc_offset_ids_arc = Arc::new(doc_offset_ids);
            let doc_embeddings_arc = Arc::new(doc_embeddings);

            // 500 tokio tasks each adding 20 embeddings.
            let mut tasks = Vec::new();
            for i in 0..500 {
                let writer_clone = writer.clone();
                let doc_offset_ids_clone = doc_offset_ids_arc.clone();
                let doc_embeddings_clone = doc_embeddings_arc.clone();
                let join_handle = tokio::task::spawn(async move {
                    for j in 1..=20 {
                        let id = i * 20 + j;
                        writer_clone
                            .add(doc_offset_ids_clone[id - 1], &doc_embeddings_clone[id - 1])
                            .await
                            .expect("Error adding to spann index writer");
                    }
                });
                tasks.push(join_handle);
            }
            futures::future::join_all(tasks)
                .await
                .into_iter()
                .for_each(|result| {
                    result.expect("Error in tokio task");
                });
            let flusher = writer
                .commit()
                .await
                .expect("Error committing spann index writer");
            let paths = flusher
                .flush()
                .await
                .expect("Error flushing spann index writer");
            println!("Wrote 10k records of 1000 dimensions each");
            // Construct a reader.
            // Clear the cache.
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage);
            let reader = SpannIndexReader::from_id(
                Some(&paths.hnsw_id),
                &hnsw_provider,
                &collection_id,
                distance_function,
                dimensionality,
                ef_search,
                Some(&paths.pl_id),
                Some(&paths.versions_map_id),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            // Scan the reader and verify the data.
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            assert_eq!(results.len(), 10000);
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));

            for i in 0..10000 {
                assert_eq!(results[i].doc_offset_id, doc_offset_ids_arc[i]);
                assert_eq!(results[i].doc_embedding, doc_embeddings_arc[i].as_slice());
            }
        });
    }

    #[test]
    fn test_long_running_integrity_multiple_runs() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Expected runtime to build");
        runtime.block_on(async {
            // Inserts 10k randomly generated embeddings each of 1000 dimensions in batches of 1k.
            // After each batch of 1k, it commits and flushes to disk. Then reads the data back using scan api
            // and verifies that all the data is present and correct.
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let max_block_size_bytes = 8 * 1024 * 1024;
            let collection_id = CollectionUuid::new();
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
            let distance_function = params.space.clone().into();
            let dimensionality = 1000;
            let ef_search = params.ef_search;
            let mut hnsw_path = None;
            let mut versions_map_path = None;
            let mut pl_path = None;
            let mut max_bf_id_path = None;
            let mut doc_offset_ids = vec![0u32; 10000];
            let mut doc_embeddings: Vec<Vec<f32>> = Vec::new();
            for k in 0..10 {
                let blockfile_provider =
                    new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
                let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
                let writer = SpannIndexWriter::from_id(
                    &hnsw_provider,
                    hnsw_path.as_ref(),
                    versions_map_path.as_ref(),
                    pl_path.as_ref(),
                    max_bf_id_path.as_ref(),
                    &collection_id,
                    dimensionality,
                    &blockfile_provider,
                    params.clone(),
                    gc_context.clone(),
                    SpannMetrics::default(),
                )
                .await
                .expect("Error creating spann index writer");
                let mut rng = rand::thread_rng();
                for i in 1..=1000 {
                    let id = 1000 * k + i;
                    // Generate 1000 randomly generated f32.
                    let embedding = (0..1000).map(|_| rng.gen::<f32>()).collect::<Vec<f32>>();
                    writer
                        .add(id as u32, &embedding)
                        .await
                        .expect("Error adding to spann index writer");
                    doc_offset_ids[id - 1] = id as u32;
                    doc_embeddings.push(embedding);
                }
                let flusher = writer
                    .commit()
                    .await
                    .expect("Error committing spann index writer");
                let paths = flusher
                    .flush()
                    .await
                    .expect("Error flushing spann index writer");
                println!(
                    "Wrote 1k records of 1000 dimensions each to path {:?}",
                    paths
                );
                // Update paths for the next run.
                hnsw_path = Some(paths.hnsw_id);
                versions_map_path = Some(paths.versions_map_id);
                pl_path = Some(paths.pl_id);
                max_bf_id_path = Some(paths.max_head_id_id);
            }
            // Construct a reader.
            // Clear the cache.
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage);
            let reader = SpannIndexReader::from_id(
                hnsw_path.as_ref(),
                &hnsw_provider,
                &collection_id,
                distance_function,
                dimensionality,
                ef_search,
                pl_path.as_ref(),
                versions_map_path.as_ref(),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            // Scan the reader and verify the data.
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            assert_eq!(results.len(), 10000);
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));

            for i in 0..10000 {
                assert_eq!(results[i].doc_offset_id, doc_offset_ids[i]);
                assert_eq!(results[i].doc_embedding, doc_embeddings[i].as_slice());
            }
        });
    }

    // NOTE(Sanket): It is non-trivial to use shuttle for this test since it requires
    // a tokio runtime for creating the hnsw provider - the cache requires a mpsc channel,
    // the construction of hnsw provider calls async tokio filesystem apis that also need
    // a runtime which is not supported by shuttle.
    #[test]
    fn test_long_running_data_integrity_multiple_parallel_runs() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Expected runtime to build");
        runtime.block_on(async {
            // Inserts 10k randomly generated embeddings each of 1000 dimensions in batches of 1k.
            // Each batch of 1k records is inserted in parallel using 10 tokio tasks.
            // After each batch of 1k, it commits and flushes to disk. Then reads the data back using scan api
            // and verifies that all the data is present and correct.
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let max_block_size_bytes = 8 * 1024 * 1024;
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
            let distance_function = params.space.clone().into();
            let collection_id = CollectionUuid::new();
            let dimensionality = 1000;
            let ef_search = params.ef_search;
            let mut hnsw_path = None;
            let mut versions_map_path = None;
            let mut pl_path = None;
            let mut max_bf_id_path = None;
            let mut doc_offset_ids = Vec::new();
            let mut doc_embeddings: Vec<Vec<f32>> = Vec::new();
            // Generate 10k random embeddings.
            for i in 1..=10000 {
                let embedding = (0..1000)
                    .map(|_| rand::thread_rng().gen::<f32>())
                    .collect::<Vec<f32>>();
                doc_offset_ids.push(i as u32);
                doc_embeddings.push(embedding);
            }
            let doc_offset_ids_arc = Arc::new(doc_offset_ids);
            let doc_embeddings_arc = Arc::new(doc_embeddings);
            println!("Generated 10k random embeddings");
            for k in 0..10 {
                // Create tokio task for each batch.
                let blockfile_provider =
                    new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
                let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
                let writer = SpannIndexWriter::from_id(
                    &hnsw_provider,
                    hnsw_path.as_ref(),
                    versions_map_path.as_ref(),
                    pl_path.as_ref(),
                    max_bf_id_path.as_ref(),
                    &collection_id,
                    dimensionality,
                    &blockfile_provider,
                    params.clone(),
                    gc_context.clone(),
                    SpannMetrics::default(),
                )
                .await
                .expect("Error creating spann index writer");
                // Create tokio tasks for each batch.
                let mut join_handles = Vec::new();
                for batch in 0..10 {
                    let writer_clone = writer.clone();
                    let doc_offset_ids_clone = doc_offset_ids_arc.clone();
                    let doc_embeddings_clone = doc_embeddings_arc.clone();
                    let join_handle = tokio::task::spawn(async move {
                        for i in 1..=100 {
                            let id = 1000 * k + 100 * batch + i;
                            writer_clone
                                .add(doc_offset_ids_clone[id - 1], &doc_embeddings_clone[id - 1])
                                .await
                                .expect("Error adding to spann index writer");
                        }
                    });
                    join_handles.push(join_handle);
                }
                // wait on all the futures.
                let r = futures::future::join_all(join_handles).await;
                for res in r {
                    res.expect("Error adding to spann index writer");
                }
                let flusher = writer
                    .commit()
                    .await
                    .expect("Error committing spann index writer");
                let paths = flusher
                    .flush()
                    .await
                    .expect("Error flushing spann index writer");
                println!(
                    "Wrote 1k records of 1000 dimensions each to path {:?}",
                    paths
                );
                // Update paths for the next run.
                hnsw_path = Some(paths.hnsw_id);
                versions_map_path = Some(paths.versions_map_id);
                pl_path = Some(paths.pl_id);
                max_bf_id_path = Some(paths.max_head_id_id);
            }
            // Construct a reader.
            // Clear the cache.
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage);
            let reader = SpannIndexReader::from_id(
                hnsw_path.as_ref(),
                &hnsw_provider,
                &collection_id,
                distance_function,
                dimensionality,
                ef_search,
                pl_path.as_ref(),
                versions_map_path.as_ref(),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            // Scan the reader and verify the data.
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            assert_eq!(results.len(), 10000);
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));

            for i in 0..10000 {
                assert_eq!(results[i].doc_offset_id, doc_offset_ids_arc[i]);
                assert_eq!(results[i].doc_embedding, doc_embeddings_arc[i].as_slice());
            }
        });
    }

    // NOTE(Sanket): It is non-trivial to use shuttle for this test since it requires
    // a tokio runtime for creating the hnsw provider - the cache requires a mpsc channel,
    // the construction of hnsw provider calls async tokio filesystem apis that also need
    // a runtime which is not supported by shuttle.
    #[test]
    fn test_long_running_data_integrity_multiple_parallel_runs_with_updates_deletes() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Expected runtime to build");
        runtime.block_on(async {
            // Inserts 5k randomly generated embeddings each of 1000 dimensions in batches of 1k.
            // Each batch of 1k records is inserted in parallel using 10 tokio tasks.
            // After each batch of 1k, it commits and flushes to disk.
            // Inserts another 1k adds/updates/deletes randomly chosen, commits and flushes.
            // Then reads the data back using scan api
            // and verifies that all the data is present and correct.
            // Additionally runs GC after and verifies that the data is still correct.
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let max_block_size_bytes = 8 * 1024 * 1024;
            let params = InternalSpannConfiguration {
                split_threshold: 100,
                reassign_neighbor_count: 8,
                merge_threshold: 50,
                max_neighbors: 16,
                ..Default::default()
            };
            // Create a garbage collection context.
            let pl_gc_config = PlGarbageCollectionConfig {
                enabled: true,
                policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig {
                    sample_size: 1.0,
                }),
            };
            let hnsw_gc_config = HnswGarbageCollectionConfig {
                enabled: true,
                policy: HnswGarbageCollectionPolicyConfig::FullRebuild,
            };
            let gc_context = GarbageCollectionContext::try_from_config(
                &(pl_gc_config, hnsw_gc_config),
                &Registry::default(),
            )
            .await
            .expect("Error converting config to gc context");
            let distance_function: DistanceFunction = params.space.clone().into();
            let collection_id = CollectionUuid::new();
            let dimensionality = 1000;
            let ef_search = params.ef_search;
            let mut hnsw_path = None;
            let mut versions_map_path = None;
            let mut pl_path = None;
            let mut max_bf_id_path = None;
            let mut doc_offset_ids = Vec::new();
            let mut doc_embeddings: Vec<Option<Vec<f32>>> = Vec::new();
            // Generate 10k random embeddings.
            for i in 1..=5000 {
                let embedding = (0..1000)
                    .map(|_| rand::thread_rng().gen::<f32>())
                    .collect::<Vec<f32>>();
                doc_offset_ids.push(i as u32);
                doc_embeddings.push(Some(embedding));
            }
            let doc_offset_ids_arc = Arc::new(doc_offset_ids.clone());
            let doc_embeddings_arc = Arc::new(doc_embeddings.clone());
            println!("Generated 10k random embeddings");
            for k in 0..5 {
                // Create tokio task for each batch.
                let blockfile_provider =
                    new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
                let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
                let writer = SpannIndexWriter::from_id(
                    &hnsw_provider,
                    hnsw_path.as_ref(),
                    versions_map_path.as_ref(),
                    pl_path.as_ref(),
                    max_bf_id_path.as_ref(),
                    &collection_id,
                    dimensionality,
                    &blockfile_provider,
                    params.clone(),
                    gc_context.clone(),
                    SpannMetrics::default(),
                )
                .await
                .expect("Error creating spann index writer");
                // Create tokio tasks for each batch.
                let mut join_handles = Vec::new();
                for batch in 0..10 {
                    let writer_clone = writer.clone();
                    let doc_offset_ids_clone = doc_offset_ids_arc.clone();
                    let doc_embeddings_clone = doc_embeddings_arc.clone();
                    let join_handle = tokio::task::spawn(async move {
                        for i in 1..=100 {
                            let id = 1000 * k + 100 * batch + i;
                            writer_clone
                                .add(
                                    doc_offset_ids_clone[id - 1],
                                    doc_embeddings_clone[id - 1].as_ref().unwrap(),
                                )
                                .await
                                .expect("Error adding to spann index writer");
                        }
                    });
                    join_handles.push(join_handle);
                }
                // wait on all the futures.
                let r = futures::future::join_all(join_handles).await;
                for res in r {
                    res.expect("Error adding to spann index writer");
                }
                let flusher = writer
                    .commit()
                    .await
                    .expect("Error committing spann index writer");
                let paths = flusher
                    .flush()
                    .await
                    .expect("Error flushing spann index writer");
                println!(
                    "Wrote 1k records of 1000 dimensions each to path {:?}",
                    paths
                );
                // Update paths for the next run.
                hnsw_path = Some(paths.hnsw_id);
                versions_map_path = Some(paths.versions_map_id);
                pl_path = Some(paths.pl_id);
                max_bf_id_path = Some(paths.max_head_id_id);
            }

            // 10 tokio tasks, each randomly either inserting, or updating or deleting 100 records.
            // Generate data for this.
            let mut operations: Vec<(u32, u32, Vec<f32>)> = Vec::new();
            let mut count_ops = 0;
            let mut touched_ids = HashSet::new();
            while count_ops < 1000 {
                // Generate a random integer between 0 and 2.
                let operation = rand::thread_rng().gen_range(0..3);
                match operation {
                    0 => {
                        // Insert
                        let id = rand::thread_rng().gen_range(5001..=10000);
                        if touched_ids.contains(&id) {
                            continue;
                        }
                        touched_ids.insert(id);
                        count_ops += 1;
                        let embedding = (0..1000)
                            .map(|_| rand::thread_rng().gen::<f32>())
                            .collect::<Vec<f32>>();
                        operations.push((id, 0, embedding.clone()));
                        doc_offset_ids.push(id);
                        doc_embeddings.push(Some(embedding));
                    }
                    1 => {
                        // Update
                        // Generate a random index between 0 and 5000.
                        let id = rand::thread_rng().gen_range(1..=5000);
                        if touched_ids.contains(&id) {
                            continue;
                        }
                        touched_ids.insert(id);
                        count_ops += 1;
                        let embedding = (0..1000)
                            .map(|_| rand::thread_rng().gen::<f32>())
                            .collect::<Vec<f32>>();
                        operations.push((id, 1, embedding.clone()));
                        doc_embeddings[id as usize - 1] = Some(embedding);
                    }
                    2 => {
                        // Delete
                        let id = rand::thread_rng().gen_range(1..=5000);
                        if touched_ids.contains(&id) {
                            continue;
                        }
                        touched_ids.insert(id);
                        count_ops += 1;
                        operations.push((id, 2, Vec::new()));
                        doc_embeddings[id as usize - 1] = None;
                    }
                    _ => panic!("Invalid operation"),
                }
            }
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage.clone());
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                hnsw_path.as_ref(),
                versions_map_path.as_ref(),
                pl_path.as_ref(),
                max_bf_id_path.as_ref(),
                &collection_id,
                dimensionality,
                &blockfile_provider,
                params.clone(),
                gc_context.clone(),
                SpannMetrics::default(),
            )
            .await
            .expect("Error creating spann index writer");
            let operations_arc = Arc::new(operations);
            let mut join_handles = Vec::new();
            for t in 0..100 {
                let operations_clone = operations_arc.clone();
                let writer_clone = writer.clone();
                let join_handle = tokio::task::spawn(async move {
                    for k in 1..=10 {
                        let (id, operation, embedding) = &operations_clone[t * 10 + k - 1];
                        match operation {
                            0 => {
                                writer_clone
                                    .add(*id, embedding)
                                    .await
                                    .expect("Error adding to spann index writer");
                            }
                            1 => match writer_clone.update(*id, embedding).await {
                                Ok(_) => {}
                                Err(e) => {
                                    if matches!(e, SpannIndexWriterError::VersionNotFound) {
                                        // If the id is not found, then ignore.
                                        continue;
                                    }
                                    panic!("Error updating spann index writer: {:?}", e);
                                }
                            },
                            2 => {
                                writer_clone
                                    .delete(*id)
                                    .await
                                    .expect("Error deleting from spann index writer");
                            }
                            _ => panic!("Invalid operation"),
                        }
                    }
                });
                join_handles.push(join_handle);
            }
            // wait on all the futures.
            let r = futures::future::join_all(join_handles).await;
            for res in r {
                res.expect("Error adding to spann index writer");
            }

            // Commit and flush.
            let flusher = writer
                .commit()
                .await
                .expect("Error committing spann index writer");
            let paths = flusher
                .flush()
                .await
                .expect("Error flushing spann index writer");
            hnsw_path = Some(paths.hnsw_id);
            versions_map_path = Some(paths.versions_map_id);
            pl_path = Some(paths.pl_id);
            max_bf_id_path = Some(paths.max_head_id_id);

            // Construct a reader.
            // Clear the cache.
            let hnsw_provider = new_hnsw_provider_for_tests(storage.clone(), &tmp_dir);
            let blockfile_provider =
                new_blockfile_provider_for_tests(max_block_size_bytes, storage);
            let reader = SpannIndexReader::from_id(
                hnsw_path.as_ref(),
                &hnsw_provider,
                &collection_id,
                distance_function.clone(),
                dimensionality,
                ef_search,
                pl_path.as_ref(),
                versions_map_path.as_ref(),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            // Scan the reader and verify the data.
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));

            let mut actual_pairs: Vec<(u32, Option<Vec<f32>>)> = doc_offset_ids
                .iter()
                .cloned()
                .zip(doc_embeddings.drain(..))
                .collect();

            // Sort the pairs by id
            actual_pairs.sort_by_key(|(id, _)| *id);
            let mut count = 0;
            for (id, embedding) in actual_pairs.iter() {
                if embedding.is_none() {
                    continue;
                }
                assert_eq!(results[count].doc_offset_id, *id);
                assert_eq!(
                    results[count].doc_embedding,
                    embedding.as_ref().unwrap().as_slice(),
                );
                count += 1;
            }
            assert_eq!(results.len(), count);
            // After GC, it should return the same result.
            let mut writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                hnsw_path.as_ref(),
                versions_map_path.as_ref(),
                pl_path.as_ref(),
                max_bf_id_path.as_ref(),
                &collection_id,
                dimensionality,
                &blockfile_provider,
                params,
                gc_context,
                SpannMetrics::default(),
            )
            .await
            .expect("Error creating spann index writer");
            writer
                .garbage_collect()
                .await
                .expect("Error garbage collecting");
            let flusher = writer
                .commit()
                .await
                .expect("Error committing spann index writer");
            let paths = flusher
                .flush()
                .await
                .expect("Error flushing spann index writer");
            hnsw_path = Some(paths.hnsw_id);
            versions_map_path = Some(paths.versions_map_id);
            pl_path = Some(paths.pl_id);
            let reader = SpannIndexReader::from_id(
                hnsw_path.as_ref(),
                &hnsw_provider,
                &collection_id,
                distance_function.clone(),
                dimensionality,
                ef_search,
                pl_path.as_ref(),
                versions_map_path.as_ref(),
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index reader");
            let mut results = reader
                .scan()
                .await
                .expect("Error scanning spann index reader");
            results.sort_by(|a, b| a.doc_offset_id.cmp(&b.doc_offset_id));
            let mut count = 0;
            for (id, embedding) in actual_pairs.iter() {
                if embedding.is_none() {
                    continue;
                }
                assert_eq!(results[count].doc_offset_id, *id);
                assert_eq!(
                    results[count].doc_embedding,
                    embedding.as_ref().unwrap().as_slice(),
                );
                count += 1;
            }
            assert_eq!(results.len(), count);
        });
    }
}
