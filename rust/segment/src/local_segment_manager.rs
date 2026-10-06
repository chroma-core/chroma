use chroma_cache::{Cache, CacheConfig, CacheError, FoyerCacheConfig};
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::IndexUuid;
use chroma_sqlite::db::SqliteDb;
use chroma_types::{Collection, Segment};
use serde::{Deserialize, Serialize};
use std::{io::ErrorKind, path::Path, sync::Arc};
use thiserror::Error;

use crate::local_hnsw::{
    LocalHnswIndex, LocalHnswSegmentReader, LocalHnswSegmentReaderError, LocalHnswSegmentWriter,
    LocalHnswSegmentWriterError,
};

fn default_hnsw_index_pool_cache_config() -> CacheConfig {
    CacheConfig::Memory(FoyerCacheConfig {
        capacity: 65536,
        ..Default::default()
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalSegmentManagerConfig {
    // TODO(Sanket): Estimate the max number of FDs that can be kept open and
    // use that as a capacity in the cache.
    #[serde(default = "default_hnsw_index_pool_cache_config")]
    pub hnsw_index_pool_cache_config: CacheConfig,
    pub persist_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LocalSegmentManager {
    hnsw_index_pool: Arc<dyn Cache<IndexUuid, LocalHnswIndex>>,
    #[allow(dead_code)]
    eviction_callback_task_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
    sqlite: SqliteDb,
    persist_root: Option<String>,
}

impl Injectable for LocalSegmentManager {}

#[async_trait::async_trait]
impl Configurable<LocalSegmentManagerConfig> for LocalSegmentManager {
    async fn try_from_config(
        config: &LocalSegmentManagerConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn chroma_error::ChromaError>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_index_pool: Box<dyn Cache<IndexUuid, LocalHnswIndex>> =
            chroma_cache::from_config_with_event_listener(&config.hnsw_index_pool_cache_config, tx)
                .await?;
        let sqldb = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        // TODO(Sanket): Might need tokio runtime to be passed here to spawn the task.
        let handle = tokio::spawn(async move {
            while let Some((_, index)) = rx.recv().await {
                // Close the FD here.
                index.close().await;
            }
        });
        let res = Self {
            hnsw_index_pool: hnsw_index_pool.into(),
            eviction_callback_task_handle: Some(Arc::new(handle)),
            sqlite: sqldb,
            persist_root: config.persist_path.clone(),
        };
        registry.register(res.clone());
        Ok(res)
    }
}

#[derive(Error, Debug)]
pub enum LocalSegmentManagerError {
    #[error("Error creating hnsw segment reader: {0}")]
    LocalHnswSegmentReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error reading hnsw pool cache: {0}")]
    PoolCacheError(#[from] CacheError),
    #[error("Error creating hnsw segment writer: {0}")]
    LocalHnswSegmentWriterError(#[from] LocalHnswSegmentWriterError),
    #[error("Error removing persisted HNSW segment directory: {0}")]
    RemoveSegmentDirectoryError(#[from] std::io::Error),
}

impl ChromaError for LocalSegmentManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalSegmentManagerError::LocalHnswSegmentReaderError(e) => e.code(),
            LocalSegmentManagerError::PoolCacheError(e) => e.code(),
            LocalSegmentManagerError::LocalHnswSegmentWriterError(e) => e.code(),
            LocalSegmentManagerError::RemoveSegmentDirectoryError(_) => ErrorCodes::Internal,
        }
    }
}

impl LocalSegmentManager {
    pub async fn get_hnsw_reader(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentReader, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        match self.hnsw_index_pool.get(&IndexUuid(segment.id.0)).await? {
            Some(hnsw_index) => Ok(LocalHnswSegmentReader::from_index(hnsw_index)),
            None => {
                let reader = LocalHnswSegmentReader::from_segment(
                    collection,
                    segment,
                    dimensionality,
                    self.persist_root.clone(),
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

    pub async fn get_hnsw_writer(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentWriter, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        match self.hnsw_index_pool.get(&IndexUuid(segment.id.0)).await? {
            Some(hnsw_index) => Ok(LocalHnswSegmentWriter::from_index(hnsw_index)?),
            None => {
                let writer = LocalHnswSegmentWriter::from_segment(
                    collection,
                    segment,
                    dimensionality,
                    self.persist_root.clone(),
                    self.sqlite.clone(),
                )
                .await?;
                // Open the FDs.
                writer.index.start().await;
                // Backfill.
                self.hnsw_index_pool
                    .insert(index_uuid, writer.index.clone())
                    .await;
                Ok(writer)
            }
        }
    }

    pub async fn reset(&self) -> Result<(), LocalSegmentManagerError> {
        self.hnsw_index_pool.clear().await?;
        Ok(())
    }

    pub async fn delete_segments(
        &self,
        segments: &[Segment],
    ) -> Result<(), LocalSegmentManagerError> {
        let Some(persist_root) = &self.persist_root else {
            return Ok(());
        };

        for segment in segments {
            if segment.r#type != chroma_types::SegmentType::HnswLocalPersisted {
                continue;
            }

            let index_uuid = IndexUuid(segment.id.0);
            if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
                // Close the index before removing its files. Removing it from the cache
                // also prevents subsequent requests from reusing the deleted index.
                index.close().await;
                self.hnsw_index_pool.remove(&index_uuid).await;
            }

            let index_path = Path::new(persist_root).join(segment.id.to_string());
            match tokio::fs::remove_dir_all(index_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(LocalSegmentManagerError::RemoveSegmentDirectoryError(error))
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{CollectionUuid, SegmentScope, SegmentType, SegmentUuid};

    #[tokio::test]
    async fn delete_segments_removes_persisted_hnsw_directory() {
        let persist_dir = tempfile::tempdir().expect("persist directory");
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cache_config = default_hnsw_index_pool_cache_config();
        let hnsw_index_pool = chroma_cache::from_config_with_event_listener(&cache_config, tx)
            .await
            .expect("HNSW index cache");
        let manager = LocalSegmentManager {
            hnsw_index_pool: hnsw_index_pool.into(),
            eviction_callback_task_handle: None,
            sqlite: get_new_sqlite_db().await,
            persist_root: Some(persist_dir.path().to_string_lossy().into_owned()),
        };

        let segment_id = SegmentUuid::new();
        let segment = Segment {
            id: segment_id,
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: CollectionUuid::new(),
            metadata: None,
            file_path: Default::default(),
        };
        let index_dir = persist_dir.path().join(segment_id.to_string());
        tokio::fs::create_dir(&index_dir)
            .await
            .expect("index directory");
        tokio::fs::write(index_dir.join("header.bin"), b"persisted index")
            .await
            .expect("index file");

        manager
            .delete_segments(&[segment.clone()])
            .await
            .expect("delete segment files");

        assert!(!index_dir.exists());
        assert!(manager
            .hnsw_index_pool
            .get(&IndexUuid(segment_id.0))
            .await
            .expect("read index cache")
            .is_none());
    }

    #[tokio::test]
    async fn delete_segments_ignores_non_persisted_segments() {
        let persist_dir = tempfile::tempdir().expect("persist directory");
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cache_config = default_hnsw_index_pool_cache_config();
        let hnsw_index_pool = chroma_cache::from_config_with_event_listener(&cache_config, tx)
            .await
            .expect("HNSW index cache");
        let manager = LocalSegmentManager {
            hnsw_index_pool: hnsw_index_pool.into(),
            eviction_callback_task_handle: None,
            sqlite: get_new_sqlite_db().await,
            persist_root: Some(persist_dir.path().to_string_lossy().into_owned()),
        };

        let segment_id = SegmentUuid::new();
        let segment = Segment {
            id: segment_id,
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::METADATA,
            collection: CollectionUuid::new(),
            metadata: None,
            file_path: Default::default(),
        };
        let unrelated_dir = persist_dir.path().join(segment_id.to_string());
        tokio::fs::create_dir(&unrelated_dir)
            .await
            .expect("unrelated segment directory");

        manager
            .delete_segments(&[segment])
            .await
            .expect("delete segments");

        assert!(unrelated_dir.exists());
    }
}
