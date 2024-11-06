use std::collections::HashMap;

use arrow::error;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriter};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::SpannPostingList;
use thiserror::Error;
use uuid::Uuid;

use crate::hnsw_provider::{HnswIndexParams, HnswIndexProvider, HnswIndexRef};

// TODO(Sanket): Add locking structures as necessary.
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
}

impl ChromaError for SpannIndexWriterConstructionError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswIndexConstructionError => ErrorCodes::Internal,
            Self::BlockfileReaderConstructionError => ErrorCodes::Internal,
            Self::BlockfileWriterConstructionError => ErrorCodes::Internal,
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

    pub async fn hnsw_index_from_id(
        hnsw_provider: &HnswIndexProvider,
        id: &Uuid,
        collection_id: &Uuid,
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

    pub async fn create_hnsw_index(
        hnsw_provider: &HnswIndexProvider,
        collection_id: &Uuid,
        distance_function: DistanceFunction,
        dimensionality: usize,
        hnsw_params: HnswIndexParams,
    ) -> Result<HnswIndexRef, SpannIndexWriterConstructionError> {
        let persist_path = &hnsw_provider.temporary_storage_path;
        match hnsw_provider
            .create(
                collection_id,
                hnsw_params,
                persist_path,
                dimensionality as i32,
                distance_function,
            )
            .await
        {
            Ok(index) => Ok(index),
            Err(_) => Err(SpannIndexWriterConstructionError::HnswIndexConstructionError),
        }
    }

    pub async fn load_versions_map(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<HashMap<u32, u32>, SpannIndexWriterConstructionError> {
        // Create a reader for the blockfile. Load all the data into the versions map.
        let mut versions_map = HashMap::new();
        let reader = match blockfile_provider.open::<u32, u32>(blockfile_id).await {
            Ok(reader) => reader,
            Err(_) => {
                return Err(SpannIndexWriterConstructionError::BlockfileReaderConstructionError)
            }
        };
        // Load data using the reader.
        let versions_data = reader.get_all_data().await;
        versions_data.iter().for_each(|(_, key, value)| {
            versions_map.insert(*key, *value);
        });
        Ok(versions_map)
    }

    pub async fn fork_postings_list(
        blockfile_id: &Uuid,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterConstructionError> {
        match blockfile_provider
            .fork::<u32, &SpannPostingList<'_>>(blockfile_id)
            .await
        {
            Ok(writer) => Ok(writer),
            Err(_) => Err(SpannIndexWriterConstructionError::BlockfileWriterConstructionError),
        }
    }

    pub async fn create_posting_list(
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileWriter, SpannIndexWriterConstructionError> {
        match blockfile_provider.create::<u32, &SpannPostingList<'_>>() {
            Ok(writer) => Ok(writer),
            Err(_) => Err(SpannIndexWriterConstructionError::BlockfileWriterConstructionError),
        }
    }
}
