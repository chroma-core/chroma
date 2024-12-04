use std::collections::HashMap;

use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriter, BlockfileWriterOptions};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{CollectionUuid, SpannPostingList};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    hnsw_provider::{HnswIndexProvider, HnswIndexRef},
    IndexUuid,
};

// TODO(Sanket): Add locking structures as necessary.
#[allow(dead_code)]
pub struct SpannIndexWriter {
    // HNSW index and its provider for centroid search.
    hnsw_index: HnswIndexRef,
    hnsw_provider: HnswIndexProvider,
    // Posting list of the centroids.
    // The blockfile also contains next id for the head.
    posting_list_writer: BlockfileWriter,
    // Version number of each point.
    versions_map: HashMap<u32, u32>,
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
}

impl ChromaError for SpannIndexWriterConstructionError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexConstructionError => ErrorCodes::Internal,
            Self::BlockfileReaderConstructionError => ErrorCodes::Internal,
            Self::BlockfileWriterConstructionError => ErrorCodes::Internal,
            Self::BlockfileVersionDataLoadError => ErrorCodes::Internal,
        }
    }
}

impl SpannIndexWriter {
    pub fn new(
        hnsw_index: HnswIndexRef,
        hnsw_provider: HnswIndexProvider,
        posting_list_writer: BlockfileWriter,
        versions_map: HashMap<u32, u32>,
    ) -> Self {
        SpannIndexWriter {
            hnsw_index,
            hnsw_provider,
            posting_list_writer,
            versions_map,
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
    ) -> Result<HashMap<u32, u32>, SpannIndexWriterConstructionError> {
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
        Ok(versions_map)
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
                    distance_function,
                    dimensionality,
                )
                .await?
            }
            None => {
                Self::create_hnsw_index(
                    hnsw_provider,
                    collection_id,
                    distance_function,
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
            None => HashMap::new(),
        };
        // Fork the posting list writer.
        let posting_list_writer = match posting_list_id {
            Some(posting_list_id) => {
                Self::fork_postings_list(posting_list_id, blockfile_provider).await?
            }
            None => Self::create_posting_list(blockfile_provider).await?,
        };
        Ok(Self::new(
            hnsw_index,
            hnsw_provider.clone(),
            posting_list_writer,
            versions_map,
        ))
    }
}
