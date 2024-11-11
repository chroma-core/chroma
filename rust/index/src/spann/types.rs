use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};

use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriter, BlockfileWriterOptions};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::CollectionUuid;
use chroma_types::SpannPostingList;
use parking_lot::RwLock;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    hnsw_provider::{HnswIndexProvider, HnswIndexRef},
    IndexUuid,
};

// TODO(Sanket): Add locking structures as necessary.
struct VersionsMapInner {
    versions_map: HashMap<u32, u32>,
}

#[allow(dead_code)]
pub struct SpannIndexWriter {
    // HNSW index and its provider for centroid search.
    hnsw_index: HnswIndexRef,
    hnsw_provider: HnswIndexProvider,
    // Posting list of the centroids.
    // TODO(Sanket): For now the lock is very coarse grained. But this should
    // be change in future.
    posting_list_writer: Arc<Mutex<BlockfileWriter>>,
    next_head_id: Arc<AtomicU32>,
    // Version number of each point.
    // TODO(Sanket): Finer grained locking for this map in future.
    versions_map: Arc<RwLock<VersionsMapInner>>,
    distance_function: DistanceFunction,
    dimensionality: usize,
}

#[derive(Error, Debug)]
pub enum SpannIndexWriterConstructionError {
    #[error("HNSW index construction error")]
    HnswIndexConstructionError,
    #[error("Blockfile reader construction error")]
    BlockfileReaderConstructionError,
    #[error("Blockfile writer construction error")]
    BlockfileWriterConstructionError,
    #[error("Error loading version data from blockfile")]
    BlockfileVersionDataLoadError,
    #[error("Error resizing hnsw index")]
    HnswIndexResizeError,
    #[error("Error adding to hnsw index")]
    HnswIndexAddError,
    #[error("Error searching from hnsw")]
    HnswIndexSearchError,
    #[error("Error adding to posting list")]
    PostingListAddError,
    #[error("Error searching for posting list")]
    PostingListSearchError,
    #[error("Expected data not found")]
    ExpectedDataNotFound,
}

impl ChromaError for SpannIndexWriterConstructionError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexConstructionError => ErrorCodes::Internal,
            Self::BlockfileReaderConstructionError => ErrorCodes::Internal,
            Self::BlockfileWriterConstructionError => ErrorCodes::Internal,
            Self::BlockfileVersionDataLoadError => ErrorCodes::Internal,
            Self::HnswIndexResizeError => ErrorCodes::Internal,
            Self::HnswIndexAddError => ErrorCodes::Internal,
            Self::PostingListAddError => ErrorCodes::Internal,
            Self::HnswIndexSearchError => ErrorCodes::Internal,
            Self::PostingListSearchError => ErrorCodes::Internal,
            Self::ExpectedDataNotFound => ErrorCodes::Internal,
        }
    }
}

const MAX_HEAD_OFFSET_ID: &str = "max_head_offset_id";

// TODO(Sanket): Make this configurable.
const NUM_CENTROIDS_TO_SEARCH: u32 = 64;
const RNG_FACTOR: f32 = 1.0;
const SPLIT_THRESHOLD: usize = 100;

impl SpannIndexWriter {
    pub fn new(
        hnsw_index: HnswIndexRef,
        hnsw_provider: HnswIndexProvider,
        posting_list_writer: BlockfileWriter,
        next_head_id: u32,
        versions_map: VersionsMapInner,
        distance_function: DistanceFunction,
        dimensionality: usize,
    ) -> Self {
        SpannIndexWriter {
            hnsw_index,
            hnsw_provider,
            posting_list_writer: Arc::new(Mutex::new(posting_list_writer)),
            next_head_id: Arc::new(AtomicU32::new(next_head_id)),
            versions_map: Arc::new(RwLock::new(versions_map)),
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
    ) -> Result<HnswIndexRef, SpannIndexWriterConstructionError> {
        match hnsw_provider
            .fork(id, collection_id, dimensionality as i32, distance_function)
            .await
        {
            Ok(index) => Ok(index),
            Err(_) => Err(SpannIndexWriterConstructionError::HnswIndexConstructionError),
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
    ) -> Result<HnswIndexRef, SpannIndexWriterConstructionError> {
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
            Err(_) => Err(SpannIndexWriterConstructionError::HnswIndexConstructionError),
        }
    }

    async fn load_versions_map(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<VersionsMapInner, SpannIndexWriterConstructionError> {
        // Create a reader for the blockfile. Load all the data into the versions map.
        let mut versions_map = HashMap::new();
        let reader = match blockfile_provider.read::<u32, u32>(blockfile_id).await {
            Ok(reader) => reader,
            Err(_) => {
                return Err(SpannIndexWriterConstructionError::BlockfileReaderConstructionError)
            }
        };
        // Load data using the reader.
        let versions_data = reader
            .get_range(.., ..)
            .await
            .map_err(|_| SpannIndexWriterConstructionError::BlockfileVersionDataLoadError)?;
        versions_data.iter().for_each(|(key, value)| {
            versions_map.insert(*key, *value);
        });
        Ok(VersionsMapInner { versions_map })
    }

    async fn fork_postings_list(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterConstructionError> {
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
        bf_options = bf_options.fork(*blockfile_id);
        match blockfile_provider
            .write::<u32, &SpannPostingList<'_>>(bf_options)
            .await
        {
            Ok(writer) => Ok(writer),
            Err(_) => Err(SpannIndexWriterConstructionError::BlockfileWriterConstructionError),
        }
    }

    async fn create_posting_list(
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterConstructionError> {
        let mut bf_options = BlockfileWriterOptions::new();
        bf_options = bf_options.unordered_mutations();
        match blockfile_provider
            .write::<u32, &SpannPostingList<'_>>(bf_options)
            .await
        {
            Ok(writer) => Ok(writer),
            Err(_) => Err(SpannIndexWriterConstructionError::BlockfileWriterConstructionError),
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
    ) -> Result<Self, SpannIndexWriterConstructionError> {
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
                    Ok(reader) => reader.get("", MAX_HEAD_OFFSET_ID).await.map_err(|_| {
                        SpannIndexWriterConstructionError::BlockfileReaderConstructionError
                    })?,
                    Err(_) => 0,
                }
            }
            None => 0,
        };
        Ok(Self::new(
            hnsw_index,
            hnsw_provider.clone(),
            posting_list_writer,
            1 + max_head_id,
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

    async fn rng_query(
        &self,
        query: &[f32],
    ) -> Result<(Vec<usize>, Vec<f32>), SpannIndexWriterConstructionError> {
        let mut normalized_query = query.to_vec();
        // Normalize the query in case of cosine.
        if self.distance_function == DistanceFunction::Cosine {
            normalized_query = normalize(query)
        }
        let ids;
        let distances;
        let mut embeddings: Vec<Vec<f32>> = vec![];
        {
            let read_guard = self.hnsw_index.inner.read();
            let allowed_ids = vec![];
            let disallowed_ids = vec![];
            (ids, distances) = read_guard
                .query(
                    &normalized_query,
                    NUM_CENTROIDS_TO_SEARCH as usize,
                    &allowed_ids,
                    &disallowed_ids,
                )
                .map_err(|_| SpannIndexWriterConstructionError::HnswIndexSearchError)?;
            // Get the embeddings also for distance computation.
            for id in ids.iter() {
                let emb = read_guard
                    .get(*id)
                    .map_err(|_| SpannIndexWriterConstructionError::HnswIndexSearchError)?
                    .ok_or(SpannIndexWriterConstructionError::HnswIndexSearchError)?;
                embeddings.push(emb);
            }
        }
        // Apply the RNG rule to prune.
        let mut res_ids = vec![];
        let mut res_distances = vec![];
        let mut res_embeddings: Vec<&Vec<f32>> = vec![];
        // Embeddings that were obtained are already normalized.
        for (id, (distance, embedding)) in ids.iter().zip(distances.iter().zip(embeddings.iter())) {
            let mut rng_accepted = true;
            for nbr_embedding in res_embeddings.iter() {
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

    async fn append(
        &self,
        head_id: u32,
        id: u32,
        version: u32,
        embedding: &[f32],
    ) -> Result<(), SpannIndexWriterConstructionError> {
        {
            let write_guard = self.posting_list_writer.lock().await;
            // TODO(Sanket): Check if head is deleted, can happen if another concurrent thread
            // deletes it.
            let current_pl = write_guard
                .get_clone::<u32, &SpannPostingList<'_>>("", head_id)
                .await
                .map_err(|_| SpannIndexWriterConstructionError::PostingListSearchError)?
                .ok_or(SpannIndexWriterConstructionError::PostingListSearchError)?;
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
                        .ok_or(SpannIndexWriterConstructionError::ExpectedDataNotFound)?;
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
                .map_err(|_| SpannIndexWriterConstructionError::PostingListAddError)?;
        }
        Ok(())
    }

    async fn add_postings_list(
        &self,
        id: u32,
        version: u32,
        embeddings: &[f32],
    ) -> Result<(), SpannIndexWriterConstructionError> {
        let (ids, distances) = self.rng_query(embeddings).await?;
        // Create a centroid with just this point.
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
                    .map_err(|_| SpannIndexWriterConstructionError::PostingListAddError)?;
            }
            // Next add to hnsw.
            // This shouldn't exceed the capacity since this will happen only for the first few points
            // so no need to check and increase the capacity.
            {
                let write_guard = self.hnsw_index.inner.write();
                write_guard
                    .add(next_id as usize, embeddings)
                    .map_err(|_| SpannIndexWriterConstructionError::HnswIndexAddError)?;
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

    pub async fn add(&self, id: u32, embeddings: &[f32]) {
        let version = self.add_versions_map(id);
    }
}
