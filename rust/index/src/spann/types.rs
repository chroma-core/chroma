use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

use chroma_blockstore::{
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileFlusher, BlockfileReader, BlockfileWriter, BlockfileWriterOptions,
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
    spann::utils::cluster,
    Index, IndexUuid,
};

use super::utils::{rng_query, KMeansAlgorithmInput, KMeansError};

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
const MERGE_THRESHOLD: usize = 50;
const NUM_CENTERS_TO_MERGE_TO: usize = 8;

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
        rng_query(
            query,
            self.hnsw_index.clone(),
            NUM_CENTROIDS_TO_SEARCH as usize,
            QUERY_EPSILON,
            RNG_FACTOR,
            self.distance_function.clone(),
            true,
        )
        .await
        .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)
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
        // TODO(Sanket): Don't consider heads that are farther away than the closest.
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
            if self.is_head_deleted(head_id as usize).await? {
                return Ok(());
            }
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
                    .map_err(|_| SpannIndexWriterError::PostingListSetError)?;

                return Ok(());
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
                    // TODO(Sanket): Need to understand what this achieves.
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

    pub async fn update(&self, id: u32, embedding: &[f32]) -> Result<(), SpannIndexWriterError> {
        // Delete and then add.
        self.delete(id).await?;
        self.add(id, embedding).await
    }

    pub async fn delete(&self, id: u32) -> Result<(), SpannIndexWriterError> {
        let mut version_map_guard = self.versions_map.write();
        version_map_guard.versions_map.insert(id, 0);
        Ok(())
    }

    async fn get_up_to_date_count(
        &self,
        doc_offset_ids: &[u32],
        doc_versions: &[u32],
    ) -> Result<usize, SpannIndexWriterError> {
        let mut up_to_date_index = 0;
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
                .map_err(|_| SpannIndexWriterError::PostingListGetError)?
                .ok_or(SpannIndexWriterError::PostingListGetError)?;
            (doc_offset_ids, doc_versions, doc_embeddings) = self
                .remove_outdated_entries(doc_offset_ids, doc_versions, doc_embeddings)
                .await?;
            source_cluster_len = doc_offset_ids.len();
            // Write the PL back and return if within the merge threshold.
            if source_cluster_len > MERGE_THRESHOLD {
                let posting_list = SpannPostingList {
                    doc_offset_ids: &doc_offset_ids,
                    doc_versions: &doc_versions,
                    doc_embeddings: &doc_embeddings,
                };
                pl_guard
                    .set("", head_id as u32, &posting_list)
                    .await
                    .map_err(|_| SpannIndexWriterError::PostingListSetError)?;

                return Ok(());
            }
            // Find candidates for merge.
            let (nearest_head_ids, _, nearest_head_embeddings) = self
                .get_nearby_heads(head_embedding, NUM_CENTERS_TO_MERGE_TO)
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
                    .map_err(|_| SpannIndexWriterError::PostingListGetError)?
                    .ok_or(SpannIndexWriterError::PostingListGetError)?;
                target_cluster_len = self
                    .get_up_to_date_count(&nearest_head_doc_offset_ids, &nearest_head_doc_versions)
                    .await?;
                // If the total count exceeds the max posting list size then skip.
                if target_cluster_len + source_cluster_len >= SPLIT_THRESHOLD {
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
                        .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
                    // Delete from hnsw.
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard
                        .delete(head_id)
                        .map_err(|_| SpannIndexWriterError::HnswIndexAddError)?;
                } else {
                    pl_guard
                        .set("", head_id as u32, &merged_posting_list)
                        .await
                        .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
                    // Delete from hnsw.
                    let hnsw_write_guard = self.hnsw_index.inner.write();
                    hnsw_write_guard
                        .delete(nearest_head_id)
                        .map_err(|_| SpannIndexWriterError::HnswIndexAddError)?;
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
            for idx in source_cluster_len..(source_cluster_len + target_cluster_len) {
                let origin_dist = self.distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    &target_embedding,
                );
                let new_dist = self.distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    head_embedding,
                );
                if new_dist > origin_dist {
                    self.reassign(
                        doc_offset_ids[idx],
                        doc_versions[idx],
                        &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                        head_id as u32,
                    )
                    .await?;
                }
            }
        } else {
            // source_cluster points were merged to target_cluster
            // so they are candidates for reassignment.
            for idx in 0..source_cluster_len {
                let origin_dist = self.distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    head_embedding,
                );
                let new_dist = self.distance_function.distance(
                    &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                    &target_embedding,
                );
                if new_dist > origin_dist {
                    self.reassign(
                        doc_offset_ids[idx],
                        doc_versions[idx],
                        &doc_embeddings[idx * self.dimensionality..(idx + 1) * self.dimensionality],
                        target_head as u32,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    // TODO(Sanket): Hook in the gc policy.
    // TODO(Sanket): Garbage collect HNSW also.
    pub async fn garbage_collect(&self) -> Result<(), SpannIndexWriterError> {
        // Get all the heads.
        let non_deleted_heads;
        {
            let hnsw_read_guard = self.hnsw_index.inner.read();
            (non_deleted_heads, _) = hnsw_read_guard
                .get_all_ids()
                .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?;
        }
        // Iterate over all the heads and gc heads.
        for head_id in non_deleted_heads.into_iter() {
            if self.is_head_deleted(head_id).await? {
                return Ok(());
            }
            let head_embedding = self
                .hnsw_index
                .inner
                .read()
                .get(head_id)
                .map_err(|_| SpannIndexWriterError::HnswIndexSearchError)?
                .ok_or(SpannIndexWriterError::HnswIndexSearchError)?;
            tracing::info!("Garbage collecting head {}", head_id);
            self.garbage_collect_head(head_id, &head_embedding).await?;
        }
        Ok(())
    }

    // TODO(Sanket): Change the error types.
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

#[derive(Error, Debug)]
pub enum SpannIndexReaderError {
    #[error("Error creating/opening hnsw index")]
    HnswIndexConstructionError,
    #[error("Error creating/opening blockfile reader")]
    BlockfileReaderConstructionError,
    #[error("Spann index uninitialized")]
    UninitializedIndex,
    #[error("Error reading posting list")]
    PostingListReadError,
}

impl ChromaError for SpannIndexReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexConstructionError => ErrorCodes::Internal,
            Self::BlockfileReaderConstructionError => ErrorCodes::Internal,
            Self::UninitializedIndex => ErrorCodes::Internal,
            Self::PostingListReadError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug)]
pub struct SpannPosting {
    pub doc_offset_id: u32,
    pub doc_embedding: Vec<f32>,
}

#[derive(Clone)]
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
    ) -> Result<HnswIndexRef, SpannIndexReaderError> {
        match hnsw_provider.get(id, cache_key).await {
            Some(index) => Ok(index),
            None => {
                match hnsw_provider
                    .open(id, cache_key, dimensionality as i32, distance_function)
                    .await
                {
                    Ok(index) => Ok(index),
                    Err(_) => Err(SpannIndexReaderError::HnswIndexConstructionError),
                }
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
            Err(_) => Err(SpannIndexReaderError::BlockfileReaderConstructionError),
        }
    }

    async fn versions_map_reader_from_id(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileReader<'me, u32, u32>, SpannIndexReaderError> {
        match blockfile_provider.read::<u32, u32>(blockfile_id).await {
            Ok(reader) => Ok(reader),
            Err(_) => Err(SpannIndexReaderError::BlockfileReaderConstructionError),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_id(
        hnsw_id: Option<&IndexUuid>,
        hnsw_provider: &HnswIndexProvider,
        hnsw_cache_key: &CollectionUuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
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
            .map_err(|_| SpannIndexReaderError::PostingListReadError)?
            .ok_or(SpannIndexReaderError::PostingListReadError)?;
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
            .map_err(|_| SpannIndexReaderError::PostingListReadError)?
            .ok_or(SpannIndexReaderError::PostingListReadError)?;

        let mut posting_lists = Vec::with_capacity(res.doc_offset_ids.len());
        for (index, doc_offset_id) in res.doc_offset_ids.iter().enumerate() {
            if self
                .is_outdated(*doc_offset_id, res.doc_versions[index])
                .await?
            {
                continue;
            }
            posting_lists.push(SpannPosting {
                doc_offset_id: *doc_offset_id,
                doc_embedding: res.doc_embeddings
                    [index * self.dimensionality..(index + 1) * self.dimensionality]
                    .to_vec(),
            });
        }
        Ok(posting_lists)
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
            let mut version_map_guard = writer.versions_map.write();
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
            let version_map_guard = writer.versions_map.read();
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
            let mut version_map_guard = writer.versions_map.write();
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
            let version_map_guard = writer.versions_map.read();
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
            let mut version_map_guard = writer.versions_map.write();
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
            let mut version_map_guard = writer.versions_map.write();
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
        }
    }
}
