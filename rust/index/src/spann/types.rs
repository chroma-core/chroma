use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

use chroma_blockstore::{
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileFlusher, BlockfileWriter, BlockfileWriterOptions,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::CollectionUuid;
use chroma_types::SpannPostingList;
use rand::seq::SliceRandom;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    hnsw_provider::{
        HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderForkError, HnswIndexRef,
    },
    Index, IndexUuid,
};

use super::utils::{cluster, KMeansAlgorithmInput, KMeansError};

pub struct VersionsMapInner {
    pub versions_map: HashMap<u32, u32>,
}

#[allow(dead_code)]
// Note: Fields of this struct are public for testing.
pub struct SpannIndexWriter {
    // HNSW index and its provider for centroid search.
    pub hnsw_index: HnswIndexRef,
    hnsw_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    // Posting list of the centroids.
    // TODO(Sanket): For now the lock is very coarse grained. But this should
    // be changed in future if perf is not satisfactory.
    pub posting_list_writer: Arc<tokio::sync::Mutex<BlockfileWriter>>,
    pub next_head_id: Arc<AtomicU32>,
    // Version number of each point.
    // TODO(Sanket): Finer grained locking for this map in future if perf is not satisfactory.
    pub versions_map: Arc<parking_lot::RwLock<VersionsMapInner>>,
    pub distance_function: DistanceFunction,
    pub dimensionality: usize,
}

// TODO(Sanket): Can compose errors whenever downstream returns Box<dyn ChromaError>.
#[derive(Error, Debug)]
pub enum SpannIndexWriterError {
    #[error("Error forking hnsw index {0}")]
    HnswIndexForkError(#[from] HnswIndexProviderForkError),
    #[error("Error creating hnsw index {0}")]
    HnswIndexCreateError(#[from] HnswIndexProviderCreateError),
    #[error("Error opening reader for versions map blockfile {0}")]
    VersionsMapOpenError(#[from] OpenError),
    #[error("Error creating/forking postings list writer {0}")]
    PostingsListCreateError(#[from] CreateError),
    #[error("Error loading version data from blockfile {0}")]
    VersionsMapDataLoadError(#[from] Box<dyn ChromaError>),
    #[error("Error reading max offset id for heads")]
    MaxHeadOffsetIdBlockfileGetError,
    #[error("Error resizing hnsw index")]
    HnswIndexResizeError,
    #[error("Error adding to hnsw index")]
    HnswIndexAddError,
    #[error("Error searching from hnsw")]
    HnswIndexSearchError,
    #[error("Error adding posting list for a head")]
    PostingListSetError,
    #[error("Error getting the posting list for a head")]
    PostingListGetError,
    #[error("Did not find the version for head id")]
    VersionNotFound,
    #[error("Error committing postings list blockfile")]
    PostingListCommitError,
    #[error("Error creating blockfile writer for versions map")]
    VersionsMapWriterCreateError,
    #[error("Error writing data to versions map blockfile")]
    VersionsMapSetError,
    #[error("Error committing versions map blockfile")]
    VersionsMapCommitError,
    #[error("Error creating blockfile writer for max head id")]
    MaxHeadIdWriterCreateError,
    #[error("Error writing data to max head id blockfile")]
    MaxHeadIdSetError,
    #[error("Error committing max head id blockfile")]
    MaxHeadIdCommitError,
    #[error("Error committing hnsw index")]
    HnswIndexCommitError,
    #[error("Error flushing postings list blockfile")]
    PostingListFlushError,
    #[error("Error flushing versions map blockfile")]
    VersionsMapFlushError,
    #[error("Error flushing max head id blockfile")]
    MaxHeadIdFlushError,
    #[error("Error flushing hnsw index")]
    HnswIndexFlushError,
    #[error("Error kmeans clustering {0}")]
    KMeansClusteringError(#[from] KMeansError),
}

impl ChromaError for SpannIndexWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexForkError(e) => e.code(),
            Self::HnswIndexCreateError(e) => e.code(),
            Self::VersionsMapOpenError(e) => e.code(),
            Self::PostingsListCreateError(e) => e.code(),
            Self::VersionsMapDataLoadError(e) => e.code(),
            Self::MaxHeadOffsetIdBlockfileGetError => ErrorCodes::Internal,
            Self::HnswIndexResizeError => ErrorCodes::Internal,
            Self::HnswIndexAddError => ErrorCodes::Internal,
            Self::PostingListSetError => ErrorCodes::Internal,
            Self::HnswIndexSearchError => ErrorCodes::Internal,
            Self::PostingListGetError => ErrorCodes::Internal,
            Self::VersionNotFound => ErrorCodes::Internal,
            Self::PostingListCommitError => ErrorCodes::Internal,
            Self::VersionsMapSetError => ErrorCodes::Internal,
            Self::VersionsMapCommitError => ErrorCodes::Internal,
            Self::MaxHeadIdSetError => ErrorCodes::Internal,
            Self::MaxHeadIdCommitError => ErrorCodes::Internal,
            Self::HnswIndexCommitError => ErrorCodes::Internal,
            Self::PostingListFlushError => ErrorCodes::Internal,
            Self::VersionsMapFlushError => ErrorCodes::Internal,
            Self::MaxHeadIdFlushError => ErrorCodes::Internal,
            Self::HnswIndexFlushError => ErrorCodes::Internal,
            Self::VersionsMapWriterCreateError => ErrorCodes::Internal,
            Self::MaxHeadIdWriterCreateError => ErrorCodes::Internal,
            Self::KMeansClusteringError(e) => e.code(),
        }
    }
}

const MAX_HEAD_OFFSET_ID: &str = "max_head_offset_id";

// TODO(Sanket): Make these configurable.
#[allow(dead_code)]
const NUM_CENTROIDS_TO_SEARCH: u32 = 64;
#[allow(dead_code)]
const RNG_FACTOR: f32 = 1.0;
#[allow(dead_code)]
const SPLIT_THRESHOLD: usize = 100;
const NUM_SAMPLES_FOR_KMEANS: usize = 1000;
const INITIAL_LAMBDA: f32 = 100.0;
const REASSIGN_NBR_COUNT: usize = 8;
const QUERY_EPSILON: f32 = 10.0;

impl SpannIndexWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hnsw_index: HnswIndexRef,
        hnsw_provider: HnswIndexProvider,
        blockfile_provider: BlockfileProvider,
        posting_list_writer: BlockfileWriter,
        next_head_id: u32,
        versions_map: VersionsMapInner,
        distance_function: DistanceFunction,
        dimensionality: usize,
    ) -> Self {
        SpannIndexWriter {
            hnsw_index,
            hnsw_provider,
            blockfile_provider,
            posting_list_writer: Arc::new(tokio::sync::Mutex::new(posting_list_writer)),
            next_head_id: Arc::new(AtomicU32::new(next_head_id)),
            versions_map: Arc::new(parking_lot::RwLock::new(versions_map)),
            distance_function,
            dimensionality,
        }
    }

    async fn hnsw_index_from_id(
        hnsw_provider: &HnswIndexProvider,
        id: &IndexUuid,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
    ) -> Result<HnswIndexRef, SpannIndexWriterError> {
        match hnsw_provider
            .fork(id, collection_id, dimensionality as i32, distance_function)
            .await
        {
            Ok(index) => Ok(index),
            Err(e) => Err(SpannIndexWriterError::HnswIndexForkError(*e)),
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
            Err(e) => Err(SpannIndexWriterError::HnswIndexCreateError(*e)),
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
            Err(e) => return Err(SpannIndexWriterError::VersionsMapOpenError(*e)),
        };
        // Load data using the reader.
        let versions_data = reader
            .get_range(.., ..)
            .await
            .map_err(SpannIndexWriterError::VersionsMapDataLoadError)?;
        versions_data.iter().for_each(|(key, value)| {
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
            Err(e) => Err(SpannIndexWriterError::PostingsListCreateError(*e)),
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
            Err(e) => Err(SpannIndexWriterError::PostingsListCreateError(*e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_id(
        hnsw_provider: &HnswIndexProvider,
        hnsw_id: Option<&IndexUuid>,
        versions_map_id: Option<&Uuid>,
        posting_list_id: Option<&Uuid>,
        max_head_id_bf_id: Option<&Uuid>,
        m: Option<usize>,
        ef_construction: Option<usize>,
        ef_search: Option<usize>,
        collection_id: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, SpannIndexWriterError> {
        // Create the HNSW index.
        let hnsw_index = match hnsw_id {
            Some(hnsw_id) => {
                Self::hnsw_index_from_id(
                    hnsw_provider,
                    hnsw_id,
                    collection_id,
                    distance_function.clone(),
                    dimensionality,
                )
                .await?
            }
            None => {
                Self::create_hnsw_index(
                    hnsw_provider,
                    collection_id,
                    distance_function.clone(),
                    dimensionality,
                    m.unwrap(), // Safe since caller should always provide this.
                    ef_construction.unwrap(), // Safe since caller should always provide this.
                    ef_search.unwrap(), // Safe since caller should always provide this.
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
                        .map_err(|_| SpannIndexWriterError::MaxHeadOffsetIdBlockfileGetError)?
                        .unwrap(),
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
            distance_function,
            dimensionality,
        ))
    }

    fn add_versions_map(&self, id: u32) -> u32 {
        // 0 means deleted. Version counting starts from 1.
        let mut write_lock = self.versions_map.write();
        write_lock.versions_map.insert(id, 1);
        *write_lock.versions_map.get(&id).unwrap()
    }

    #[allow(dead_code)]
    async fn rng_query(
        &self,
        query: &[f32],
    ) -> Result<(Vec<usize>, Vec<f32>, Vec<Vec<f32>>), SpannIndexWriterError> {
        let mut nearby_ids: Vec<usize> = vec![];
        let mut nearby_distances: Vec<f32> = vec![];
        let mut embeddings: Vec<Vec<f32>> = vec![];
        {
            let read_guard = self.hnsw_index.inner.read();
            let allowed_ids = vec![];
            let disallowed_ids = vec![];
            let (ids, distances) = read_guard
                .query(
                    query,
                    NUM_CENTROIDS_TO_SEARCH as usize,
                    &allowed_ids,
                    &disallowed_ids,
                )
                .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?;
            // Get the embeddings also for distance computation.
            // Normalization is idempotent and since we write normalized embeddings
            // to the hnsw index, we'll get the same embeddings after denormalization.
            for (id, distance) in ids.iter().zip(distances.iter()) {
                if *distance <= (1_f32 + QUERY_EPSILON) * distances[0] {
                    nearby_ids.push(*id);
                    nearby_distances.push(*distance);
                }
            }
            // Get the embeddings also for distance computation.
            for id in nearby_ids.iter() {
                let emb = read_guard
                    .get(*id)
                    .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?
                    .ok_or(SpannIndexWriterError::HnswIndexSearchError)?;
                embeddings.push(emb);
            }
        }
        // Apply the RNG rule to prune.
        let mut res_ids = vec![];
        let mut res_distances = vec![];
        let mut res_embeddings: Vec<Vec<f32>> = vec![];
        // Embeddings that were obtained are already normalized.
        for (id, (distance, embedding)) in nearby_ids
            .iter()
            .zip(nearby_distances.iter().zip(embeddings))
        {
            let mut rng_accepted = true;
            for nbr_embedding in res_embeddings.iter() {
                // Embeddings are already normalized so no need to normalize again.
                let dist = self
                    .distance_function
                    .distance(&embedding[..], &nbr_embedding[..]);
                if RNG_FACTOR * dist <= *distance {
                    rng_accepted = false;
                    break;
                }
            }
            if !rng_accepted {
                continue;
            }
            res_ids.push(*id);
            res_distances.push(*distance);
            res_embeddings.push(embedding);
        }

        Ok((res_ids, res_distances, res_embeddings))
    }

    async fn is_outdated(
        &self,
        doc_offset_id: u32,
        version: u32,
    ) -> Result<bool, SpannIndexWriterError> {
        let version_map_guard = self.versions_map.read();
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
                let old_dist = self.distance_function.distance(
                    old_head_embedding,
                    &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                );
                let new_dist = self.distance_function.distance(
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
            .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?;
        // Get the embeddings also for distance computation.
        for id in nearest_ids.iter() {
            let emb = read_guard
                .get(*id)
                .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?
                .ok_or(SpannIndexWriterError::HnswIndexSearchError)?;
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
    ) -> Result<(), SpannIndexWriterError> {
        // Don't reassign if outdated by now.
        if self.is_outdated(doc_offset_id, doc_version).await? {
            return Ok(());
        }
        // RNG query to find the nearest heads.
        let (nearest_head_ids, _, nearest_head_embeddings) = self.rng_query(doc_embedding).await?;
        // If nearest_head_ids contain the previous_head_id then don't reassign.
        let prev_head_id = prev_head_id as usize;
        if nearest_head_ids.contains(&prev_head_id) {
            return Ok(());
        }
        // Increment version and trigger append.
        let next_version;
        {
            let mut version_map_guard = self.versions_map.write();
            let current_version = version_map_guard
                .versions_map
                .get(&doc_offset_id)
                .ok_or(SpannIndexWriterError::VersionNotFound)?;
            if doc_version < *current_version {
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
                return Ok(());
            }
            tracing::info!("Reassigning {} to {}", doc_offset_id, nearest_head_id);
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
                .map_err(|_| SpannIndexWriterError::PostingListGetError)?
                .ok_or(SpannIndexWriterError::PostingListGetError)?;
        }
        for (index, doc_offset_id) in doc_offset_ids.iter().enumerate() {
            if assigned_ids.contains(doc_offset_id)
                || self
                    .is_outdated(*doc_offset_id, doc_versions[index])
                    .await?
            {
                continue;
            }
            let distance_from_curr_center = self.distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                head_embedding,
            );
            let distance_from_split_center1 = self.distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                new_head_embeddings[0].unwrap(),
            );
            let distance_from_split_center2 = self.distance_function.distance(
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                new_head_embeddings[1].unwrap(),
            );
            if distance_from_curr_center <= distance_from_split_center1
                && distance_from_curr_center <= distance_from_split_center2
            {
                continue;
            }
            let distance_from_old_head = self.distance_function.distance(
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
            // TODO(Sanket): Implement reassign.
            self.reassign(
                *doc_offset_id,
                doc_versions[index],
                &doc_embeddings[index * self.dimensionality..(index + 1) * self.dimensionality],
                head_id as u32,
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
        if REASSIGN_NBR_COUNT > 0 {
            let (nearby_head_ids, _, nearby_head_embeddings) = self
                .get_nearby_heads(old_head_embedding, REASSIGN_NBR_COUNT)
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

    #[allow(dead_code)]
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
            // TODO(Sanket): Check if head is deleted, can happen if another concurrent thread
            // deletes it.
            let (mut doc_offset_ids, mut doc_versions, mut doc_embeddings) = write_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", head_id)
                .await
                .map_err(|_| SpannIndexWriterError::PostingListGetError)?
                .ok_or(SpannIndexWriterError::PostingListGetError)?;
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
                let version_map_guard = self.versions_map.read();
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
            if up_to_date_index <= SPLIT_THRESHOLD {
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
                write_guard
                    .set("", head_id, &posting_list)
                    .await
                    .map_err(|_| SpannIndexWriterError::PostingListSetError)?;

                return Ok(());
            }
            tracing::info!("Splitting posting list for head {}", head_id);
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
                NUM_SAMPLES_FOR_KMEANS,
                self.distance_function.clone(),
                INITIAL_LAMBDA,
            );
            clustering_output =
                cluster(&mut kmeans_input).map_err(SpannIndexWriterError::KMeansClusteringError)?;
            // TODO(Sanket): Not sure how this can happen. The reference implementation
            // just includes one point from the entire list in this case.
            if clustering_output.num_clusters <= 1 {
                tracing::warn!("Clustering split the posting list into only 1 cluster");
                panic!("Clustering split the posting list into only 1 cluster");
            } else {
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
                for k in 0..2 {
                    // Update the existing head.
                    if !same_head
                        && self
                            .distance_function
                            .distance(&clustering_output.cluster_centers[k], &head_embedding)
                            < 1e-6
                    {
                        tracing::info!("Same head after splitting");
                        same_head = true;
                        let posting_list = SpannPostingList {
                            doc_offset_ids: &new_doc_offset_ids[k],
                            doc_versions: &new_doc_versions[k],
                            doc_embeddings: &new_posting_lists[k],
                        };
                        write_guard
                            .set("", head_id, &posting_list)
                            .await
                            .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
                        new_head_ids[k] = head_id as i32;
                        new_head_embeddings[k] = Some(&head_embedding);
                    } else {
                        // Create new head.
                        let next_id = self
                            .next_head_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let posting_list = SpannPostingList {
                            doc_offset_ids: &new_doc_offset_ids[k],
                            doc_versions: &new_doc_versions[k],
                            doc_embeddings: &new_posting_lists[k],
                        };
                        // Insert to postings list.
                        write_guard
                            .set("", next_id, &posting_list)
                            .await
                            .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
                        new_head_ids[k] = next_id as i32;
                        new_head_embeddings[k] = Some(&clustering_output.cluster_centers[k]);
                        // Insert to hnsw now.
                        let mut hnsw_write_guard = self.hnsw_index.inner.write();
                        let hnsw_len = hnsw_write_guard.len();
                        let hnsw_capacity = hnsw_write_guard.capacity();
                        if hnsw_len + 1 > hnsw_capacity {
                            tracing::info!("Resizing hnsw index");
                            hnsw_write_guard
                                .resize(hnsw_capacity * 2)
                                .map_err(|_| SpannIndexWriterError::HnswIndexResizeError)?;
                        }
                        hnsw_write_guard
                            .add(next_id as usize, &clustering_output.cluster_centers[k])
                            .map_err(|_| SpannIndexWriterError::HnswIndexAddError)?;
                    }
                }
                if !same_head {
                    // Delete the old head
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard
                        .delete(head_id as usize)
                        .map_err(|_| SpannIndexWriterError::HnswIndexAddError)?;
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

    #[allow(dead_code)]
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
            let next_id = self
                .next_head_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                    .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
            }
            // Next add to hnsw.
            // This shouldn't exceed the capacity since this will happen only for the first few points
            // so no need to check and increase the capacity.
            {
                let write_guard = self.hnsw_index.inner.write();
                write_guard
                    .add(next_id as usize, embeddings)
                    .map_err(|_| SpannIndexWriterError::HnswIndexAddError)?;
            }
            return Ok(());
        }
        // Otherwise add to the posting list of these arrays.
        for (head_id, head_embedding) in ids.iter().zip(head_embeddings) {
            self.append(*head_id as u32, id, version, embeddings, head_embedding)
                .await?;
        }

        Ok(())
    }

    pub async fn add(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        let version = self.add_versions_map(id);
        // Normalize the embedding in case of cosine.
        let mut normalized_embedding = embedding.to_vec();
        if self.distance_function == DistanceFunction::Cosine {
            normalized_embedding = normalize(embedding);
        }
        // Add to the posting list.
        self.add_to_postings_list(id, version, &normalized_embedding)
            .await
    }

    pub async fn commit(self) -> Result<SpannIndexFlusher, SpannIndexWriterError> {
        // Pl list.
        let pl_flusher = match Arc::try_unwrap(self.posting_list_writer) {
            Ok(writer) => writer
                .into_inner()
                .commit::<u32, &SpannPostingList<'_>>()
                .await
                .map_err(|_| SpannIndexWriterError::PostingListCommitError)?,
            Err(_) => {
                // This should never happen.
                panic!("Failed to unwrap posting list writer");
            }
        };
        // Versions map. Create a writer, write all the data and commit.
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
        let versions_map_bf_writer = self
            .blockfile_provider
            .write::<u32, u32>(bf_options)
            .await
            .map_err(|_| SpannIndexWriterError::VersionsMapWriterCreateError)?;
        let versions_map_flusher = match Arc::try_unwrap(self.versions_map) {
            Ok(writer) => {
                let writer = writer.into_inner();
                for (doc_offset_id, doc_version) in writer.versions_map.into_iter() {
                    versions_map_bf_writer
                        .set("", doc_offset_id, doc_version)
                        .await
                        .map_err(|_| SpannIndexWriterError::VersionsMapSetError)?;
                }
                versions_map_bf_writer
                    .commit::<u32, u32>()
                    .await
                    .map_err(|_| SpannIndexWriterError::VersionsMapCommitError)?
            }
            Err(_) => {
                // This should never happen.
                panic!("Failed to unwrap posting list writer");
            }
        };
        // Next head.
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
        let max_head_id_bf = self
            .blockfile_provider
            .write::<&str, u32>(bf_options)
            .await
            .map_err(|_| SpannIndexWriterError::MaxHeadIdWriterCreateError)?;
        let max_head_id_flusher = match Arc::try_unwrap(self.next_head_id) {
            Ok(value) => {
                let value = value.into_inner();
                max_head_id_bf
                    .set("", MAX_HEAD_OFFSET_ID, value)
                    .await
                    .map_err(|_| SpannIndexWriterError::MaxHeadIdSetError)?;
                max_head_id_bf
                    .commit::<&str, u32>()
                    .await
                    .map_err(|_| SpannIndexWriterError::MaxHeadIdCommitError)?
            }
            Err(_) => {
                // This should never happen.
                panic!("Failed to unwrap next head id");
            }
        };

        let hnsw_id = self.hnsw_index.inner.read().id;

        // Hnsw.
        self.hnsw_provider
            .commit(self.hnsw_index)
            .map_err(|_| SpannIndexWriterError::HnswIndexCommitError)?;

        Ok(SpannIndexFlusher {
            pl_flusher,
            versions_map_flusher,
            max_head_id_flusher,
            hnsw_id,
            hnsw_flusher: self.hnsw_provider,
        })
    }
}

pub struct SpannIndexFlusher {
    pl_flusher: BlockfileFlusher,
    versions_map_flusher: BlockfileFlusher,
    max_head_id_flusher: BlockfileFlusher,
    hnsw_id: IndexUuid,
    hnsw_flusher: HnswIndexProvider,
}

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
        self.pl_flusher
            .flush::<u32, &SpannPostingList<'_>>()
            .await
            .map_err(|_| SpannIndexWriterError::PostingListFlushError)?;
        self.versions_map_flusher
            .flush::<u32, u32>()
            .await
            .map_err(|_| SpannIndexWriterError::VersionsMapFlushError)?;
        self.max_head_id_flusher
            .flush::<&str, u32>()
            .await
            .map_err(|_| SpannIndexWriterError::MaxHeadIdFlushError)?;
        self.hnsw_flusher
            .flush(&self.hnsw_id)
            .await
            .map_err(|_| SpannIndexWriterError::HnswIndexFlushError)?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::{f32::consts::PI, path::PathBuf};

    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, SpannPostingList};
    use rand::Rng;

    use crate::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter, Index};

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
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            rx,
        );
        let m = 16;
        let ef_construction = 200;
        let ef_search = 200;
        let collection_id = CollectionUuid::new();
        let distance_function = chroma_distance::DistanceFunction::Euclidean;
        let dimensionality = 2;
        let writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            Some(m),
            Some(ef_construction),
            Some(ef_search),
            &collection_id,
            distance_function,
            dimensionality,
            &blockfile_provider,
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
            assert_eq!(pl.0.len(), 1);
            assert_eq!(pl.1.len(), 1);
            assert_eq!(pl.2.len(), 2);
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
}
