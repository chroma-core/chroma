use std::{
    collections::HashMap,
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
use thiserror::Error;
use uuid::Uuid;

use crate::{
    hnsw_provider::{
        HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderForkError, HnswIndexRef,
    },
    Index, IndexUuid,
};

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
    ) -> Result<(Vec<usize>, Vec<f32>), SpannIndexWriterError> {
        let ids;
        let distances;
        let mut embeddings: Vec<Vec<f32>> = vec![];
        {
            let read_guard = self.hnsw_index.inner.read();
            let allowed_ids = vec![];
            let disallowed_ids = vec![];
            // Query is already normalized so no need to normalize again.
            (ids, distances) = read_guard
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
            for id in ids.iter() {
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
        let mut res_embeddings: Vec<&Vec<f32>> = vec![];
        for (id, (distance, embedding)) in ids.iter().zip(distances.iter().zip(embeddings.iter())) {
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

        Ok((res_ids, res_distances))
    }

    #[allow(dead_code)]
    async fn append(
        &self,
        head_id: u32,
        id: u32,
        version: u32,
        embedding: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        {
            let write_guard = self.posting_list_writer.lock().await;
            // TODO(Sanket): Check if head is deleted, can happen if another concurrent thread
            // deletes it.
            let current_pl = write_guard
                .get_owned::<u32, &SpannPostingList<'_>>("", head_id)
                .await
                .map_err(|_| SpannIndexWriterError::PostingListGetError)?
                .ok_or(SpannIndexWriterError::PostingListGetError)?;
            // Cleanup this posting list and append the new point to it.
            // TODO(Sanket): There is an order in which we are acquiring locks here. Need
            // to ensure the same order in the other places as well.
            let mut updated_doc_offset_ids = vec![];
            let mut updated_versions = vec![];
            let mut updated_embeddings = vec![];
            {
                let version_map_guard = self.versions_map.read();
                for (index, doc_version) in current_pl.1.iter().enumerate() {
                    let current_version = version_map_guard
                        .versions_map
                        .get(&current_pl.0[index])
                        .ok_or(SpannIndexWriterError::VersionNotFound)?;
                    // disregard if either deleted or on an older version.
                    if *current_version == 0 || doc_version < current_version {
                        continue;
                    }
                    updated_doc_offset_ids.push(current_pl.0[index]);
                    updated_versions.push(*doc_version);
                    // Slice. index*dimensionality to index*dimensionality + dimensionality
                    updated_embeddings.push(
                        &current_pl.2[index * self.dimensionality
                            ..index * self.dimensionality + self.dimensionality],
                    );
                }
            }
            // Add the new point.
            updated_doc_offset_ids.push(id);
            updated_versions.push(version);
            updated_embeddings.push(embedding);
            // TODO(Sanket): Trigger a split and reassign if the size exceeds threshold.
            // Write the PL back to the blockfile and release the lock.
            let posting_list = SpannPostingList {
                doc_offset_ids: &updated_doc_offset_ids,
                doc_versions: &updated_versions,
                doc_embeddings: &updated_embeddings.concat(),
            };
            // TODO(Sanket): Split if the size exceeds threshold.
            write_guard
                .set("", head_id, &posting_list)
                .await
                .map_err(|_| SpannIndexWriterError::PostingListSetError)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn add_to_postings_list(
        &self,
        id: u32,
        version: u32,
        embeddings: &[f32],
    ) -> Result<(), SpannIndexWriterError> {
        let (ids, _) = self.rng_query(embeddings).await?;
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
        for head_id in ids.iter() {
            self.append(*head_id as u32, id, version, embeddings)
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
