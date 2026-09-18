use chroma_cache::{AysncPartitionedMutex, Cache, CacheConfig, CacheError, FoyerCacheConfig};
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::IndexUuid;
use chroma_sqlite::db::SqliteDb;
use chroma_types::{Collection, Segment, SegmentUuid};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::local_hnsw::{
    Inner, LocalHnswIndex, LocalHnswSegmentReader, LocalHnswSegmentReaderError,
    LocalHnswSegmentWriter, LocalHnswSegmentWriterError,
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
    // Cache eviction must not allow a second native writer while a caller
    // still holds the first index. Serialize misses and retain weak identities.
    live_indexes:
        AysncPartitionedMutex<IndexUuid, HashMap<IndexUuid, Weak<tokio::sync::RwLock<Inner>>>>,
    #[allow(dead_code)]
    eviction_callback_task_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
    cleanup_task: Option<Arc<CleanupTask>>,
    sqlite: SqliteDb,
    persist_root: Option<String>,
}

#[derive(Debug)]
struct CleanupTask(tokio::task::JoinHandle<()>);

impl Drop for CleanupTask {
    fn drop(&mut self) {
        self.0.abort();
    }
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
        let mut res = Self {
            hnsw_index_pool: hnsw_index_pool.into(),
            live_indexes: AysncPartitionedMutex::with_parallelism(16, HashMap::new()),
            eviction_callback_task_handle: Some(Arc::new(handle)),
            cleanup_task: None,
            sqlite: sqldb,
            persist_root: config.persist_path.clone(),
        };
        if res.persist_root.is_some() {
            let cleaner = res.clone();
            res.cleanup_task = Some(Arc::new(CleanupTask(tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
                loop {
                    interval.tick().await;
                    if let Err(err) = cleaner.cleanup_deleted_indexes().await {
                        tracing::warn!(error = %err, "failed to clean up deleted HNSW indexes");
                    }
                }
            }))));
        }
        registry.register(res.clone());
        Ok(res)
    }
}

#[derive(Error, Debug)]
pub enum LocalSegmentManagerError {
    #[error("Segment has been deleted")]
    Deleted,
    #[error("Error checking segment existence: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[error("Error removing HNSW files: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error creating hnsw segment reader: {0}")]
    LocalHnswSegmentReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error reading hnsw pool cache: {0}")]
    PoolCacheError(#[from] CacheError),
    #[error("Error creating hnsw segment writer: {0}")]
    LocalHnswSegmentWriterError(#[from] LocalHnswSegmentWriterError),
}

impl ChromaError for LocalSegmentManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Deleted => ErrorCodes::NotFound,
            Self::Sqlite(_) | Self::Io(_) => ErrorCodes::Internal,
            LocalSegmentManagerError::LocalHnswSegmentReaderError(e) => e.code(),
            LocalSegmentManagerError::PoolCacheError(e) => e.code(),
            LocalSegmentManagerError::LocalHnswSegmentWriterError(e) => e.code(),
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
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            let index = LocalHnswIndex { inner };
            self.hnsw_index_pool.insert(index_uuid, index.clone()).await;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        live.retain(|_, index| index.strong_count() != 0);
        let reader = LocalHnswSegmentReader::from_segment(
            collection,
            segment,
            dimensionality,
            self.persist_root.clone(),
            self.sqlite.clone(),
        )
        .await?;
        reader.index.start().await;
        live.insert(index_uuid, Arc::downgrade(&reader.index.inner));
        self.hnsw_index_pool
            .insert(index_uuid, reader.index.clone())
            .await;
        Ok(reader)
    }

    pub async fn get_hnsw_writer(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentWriter, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            let index = LocalHnswIndex { inner };
            self.hnsw_index_pool.insert(index_uuid, index.clone()).await;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        live.retain(|_, index| index.strong_count() != 0);
        let writer = LocalHnswSegmentWriter::from_segment(
            collection,
            segment,
            dimensionality,
            self.persist_root.clone(),
            self.sqlite.clone(),
        )
        .await?;
        writer.index.start().await;
        live.insert(index_uuid, Arc::downgrade(&writer.index.inner));
        self.hnsw_index_pool
            .insert(index_uuid, writer.index.clone())
            .await;
        Ok(writer)
    }

    async fn segment_exists(&self, segment_id: SegmentUuid) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM segments WHERE id = ?)")
            .bind(segment_id.to_string())
            .fetch_one(self.sqlite.get_conn())
            .await
    }

    /// Remove resources only after the sysdb deletion has committed.
    pub async fn delete_hnsw_index(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<(), LocalSegmentManagerError> {
        let id = IndexUuid(segment_id.0);
        let mut live = self.live_indexes.lock(&id).await;
        if self.segment_exists(segment_id).await? {
            return Ok(());
        }
        if let Some(inner) = live.get(&id).and_then(Weak::upgrade) {
            LocalHnswIndex { inner }.mark_deleted().await;
        }
        // Keep the weak identity until invalidation finishes: cancellation
        // while waiting for a writer must leave cleanup retryable.
        live.remove(&id);
        self.hnsw_index_pool.remove(&id).await;
        if let Some(root) = &self.persist_root {
            match tokio::fs::remove_dir_all(Path::new(root).join(segment_id.to_string())).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }

    /// Retry orphan cleanup on startup, periodically, and after database deletion.
    pub async fn cleanup_deleted_indexes(&self) -> Result<(), LocalSegmentManagerError> {
        let Some(root) = &self.persist_root else {
            return Ok(());
        };
        let mut entries = match tokio::fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let Ok(uuid) = uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            // Recheck existence under the same lock used to load this index.
            // Segment rows commit before their directories can be created.
            self.delete_hnsw_index(SegmentUuid(uuid)).await?;
        }
        Ok(())
    }

    pub async fn reset(&self) -> Result<(), LocalSegmentManagerError> {
        self.hnsw_index_pool.clear().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{KnnIndex, Schema, SegmentScope, SegmentType, SegmentUuid};

    #[tokio::test]
    async fn concurrent_misses_and_eviction_share_one_index() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
            .bind(segment.id.to_string())
            .bind("urn:chroma:segment/vector/hnsw-local-memory")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            manager.get_hnsw_writer(&collection, &segment, 3),
            manager.get_hnsw_writer(&collection, &segment, 3),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &second.index.inner));
        manager
            .hnsw_index_pool
            .remove(&IndexUuid(segment.id.0))
            .await;
        let reader = manager
            .get_hnsw_reader(&collection, &segment, 3)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &reader.index.inner));
        let index_path = root.path().join(segment.id.to_string());
        assert!(index_path.is_dir());
        let orphan = root.path().join(SegmentUuid::new().to_string());
        tokio::fs::create_dir(&orphan).await.unwrap();
        manager.cleanup_deleted_indexes().await.unwrap();
        assert!(!orphan.exists());
        assert!(index_path.is_dir());
        sqlx::query("DELETE FROM segments WHERE id = ?")
            .bind(segment.id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let writer_guard = first.index.inner.write().await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            manager.delete_hnsw_index(segment.id),
        )
        .await
        .is_err());
        assert!(manager
            .live_indexes
            .lock(&IndexUuid(segment.id.0))
            .await
            .get(&IndexUuid(segment.id.0))
            .and_then(Weak::upgrade)
            .is_some());
        drop(writer_guard);
        manager.delete_hnsw_index(segment.id).await.unwrap();
        assert!(!index_path.exists());
        assert!(matches!(
            reader.query_embedding(&[], vec![1.0; 3], 1).await,
            Err(LocalHnswSegmentReaderError::Deleted)
        ));
        let mut first = first;
        assert!(matches!(
            first
                .apply_log_chunk(chroma_types::Chunk::new(vec![].into()))
                .await,
            Err(LocalHnswSegmentWriterError::Deleted)
        ));
        assert!(matches!(
            manager.get_hnsw_writer(&collection, &segment, 3).await,
            Err(LocalSegmentManagerError::Deleted)
        ));
    }
}
