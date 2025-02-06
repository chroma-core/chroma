use std::path::Path;
use std::sync::Arc;

use chroma_cache::{Cache, CacheConfig, CacheError};
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::IndexUuid;
use chroma_sqlite::db::SqliteDb;
use chroma_types::Segment;
use thiserror::Error;

use crate::local_hnsw::{
    LocalHnswIndex, LocalHnswSegmentReader, LocalHnswSegmentReaderError, LocalHnswSegmentWriter,
    LocalHnswSegmentWriterError,
};

pub struct LocalSegmentManagerConfig {
    // TODO(Sanket): Estimate the max number of FDs that can be kept open and
    // use that as a capacity in the cache.
    hnsw_index_pool_cache_config: CacheConfig,
}

#[allow(dead_code)]
pub struct LocalSegmentManager {
    hnsw_index_pool: Arc<dyn Cache<IndexUuid, LocalHnswIndex>>,
    eviction_callback_task_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
    sqlite: SqliteDb,
}

#[async_trait::async_trait]
impl Configurable<(LocalSegmentManagerConfig, SqliteDb)> for LocalSegmentManager {
    async fn try_from_config(
        (config, sql_db): &(LocalSegmentManagerConfig, SqliteDb),
    ) -> Result<Self, Box<dyn chroma_error::ChromaError>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_index_pool: Box<dyn Cache<IndexUuid, LocalHnswIndex>> =
            chroma_cache::from_config_with_event_listener(&config.hnsw_index_pool_cache_config, tx)
                .await?;
        // TODO(Sanket): Might need tokio runtime to be passed here to spawn the task.
        let handle = tokio::spawn(async move {
            while let Some((_, index)) = rx.recv().await {
                // Close the FD here.
                index.close().await;
                // TODO(Sanket): Persist the index.
            }
        });
        Ok(Self {
            hnsw_index_pool: hnsw_index_pool.into(),
            eviction_callback_task_handle: Some(Arc::new(handle)),
            sqlite: sql_db.clone(),
        })
    }
}

#[derive(Error, Debug)]
pub enum LocalSegmentManagerError {
    #[error("Error creating hnsw segment reader")]
    LocalHnswSegmentReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error reading hnsw pool cache")]
    PoolCacheError(#[from] CacheError),
    #[error("Error creating hnsw segment writer")]
    LocalHnswSegmentWriterError(#[from] LocalHnswSegmentWriterError),
}

impl ChromaError for LocalSegmentManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalSegmentManagerError::LocalHnswSegmentReaderError(e) => e.code(),
            LocalSegmentManagerError::PoolCacheError(e) => e.code(),
            LocalSegmentManagerError::LocalHnswSegmentWriterError(e) => e.code(),
        }
    }
}

impl LocalSegmentManager {
    #[allow(dead_code)]
    async fn get_hnsw_reader(
        &self,
        segment: &Segment,
        dimensionality: usize,
        persist_path: String,
    ) -> Result<LocalHnswSegmentReader, LocalSegmentManagerError> {
        let persist_path = Path::new(&persist_path);
        let index_uuid = IndexUuid(segment.id.0);
        match self.hnsw_index_pool.get(&IndexUuid(segment.id.0)).await? {
            Some(hnsw_index) => Ok(LocalHnswSegmentReader::from_index(hnsw_index)),
            None => {
                let reader = LocalHnswSegmentReader::from_segment(
                    segment,
                    dimensionality,
                    persist_path,
                    self.sqlite.clone(),
                )
                .await?;
                // Open the FDs.
                reader.index.start().await;
                self.hnsw_index_pool
                    .insert(index_uuid, reader.index.clone())
                    .await;
                Ok(reader)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn get_hnsw_writer(
        &self,
        segment: &Segment,
        dimensionality: usize,
        persist_path: String,
    ) -> Result<LocalHnswSegmentWriter, LocalSegmentManagerError> {
        let persist_path = Path::new(&persist_path);
        let index_uuid = IndexUuid(segment.id.0);
        match self.hnsw_index_pool.get(&IndexUuid(segment.id.0)).await? {
            Some(hnsw_index) => Ok(LocalHnswSegmentWriter::from_index(hnsw_index)?),
            None => {
                let writer = LocalHnswSegmentWriter::from_segment(
                    segment,
                    dimensionality,
                    persist_path,
                    self.sqlite.clone(),
                )
                .await?;
                // Open the FDs.
                writer.index.start().await;
                self.hnsw_index_pool
                    .insert(index_uuid, writer.index.clone())
                    .await;
                Ok(writer)
            }
        }
    }
}
