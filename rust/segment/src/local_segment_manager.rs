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
    io,
    path::Path,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::local_hnsw::{
    inspect_persisted_hnsw_index, Inner, LocalHnswIndex, LocalHnswSegmentReader,
    LocalHnswSegmentReaderError, LocalHnswSegmentWriter, LocalHnswSegmentWriterError,
    METADATA_FILE,
};

fn default_hnsw_index_pool_cache_config() -> CacheConfig {
    CacheConfig::Memory(FoyerCacheConfig {
        capacity: 65536,
        ..Default::default()
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalSegmentManagerConfig {
    // Controls resident indexes, not file handles. Local HNSW opens files only
    // during bounded disk operations, independent of cache capacity.
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
    /// Validate the on-disk checkpoint before discarding its replay records.
    ///
    /// This inspects native files and the ID map without loading HNSW or relying
    /// on collection configuration or a cached index. Missing files, structural
    /// corruption, and checkpoints requiring replay all prevent purging.
    /// Callers must serialize this check and the subsequent purge with checkpoint
    /// writes; the local compaction manager does so through its message queue.
    pub async fn validate_persisted_checkpoint(&self, segment: &SegmentUuid) -> io::Result<()> {
        let root = self.persist_root.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no persistent HNSW directory")
        })?;
        let path = Path::new(root).join(segment.to_string());
        tokio::task::spawn_blocking(move || {
            // The inspector accepts a missing map for offline diagnostics, but
            // purging requires a complete checkpoint, even for an empty index.
            if !path.join(METADATA_FILE).metadata()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing HNSW ID map",
                ));
            }
            if inspect_persisted_hnsw_index(&path)?.recovery_required {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HNSW checkpoint requires log replay",
                ));
            }
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub async fn get_hnsw_reader(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentReader, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        // A concurrent miss may have filled the cache while this caller waited
        // for the lock. Re-inserting that index would fire the cache's replace
        // event, whose listener closes the files of the index being returned.
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            // Reuse the live in-memory index. Checkpointing opens its files
            // under the write lock, so queued eviction callbacks are harmless.
            let index = LocalHnswIndex { inner };
            index.ensure_usable().await?;
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
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        // A concurrent miss may have filled the cache while this caller waited
        // for the lock. Re-inserting that index would fire the cache's replace
        // event, whose listener closes the files of the index being returned.
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            // Reuse the live in-memory index. Checkpointing opens its files
            // under the write lock, so queued eviction callbacks are harmless.
            let index = LocalHnswIndex { inner };
            index.ensure_usable().await?;
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
        let authorized: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM index_cleanup WHERE segment_id = ?)")
                .bind(segment_id.to_string())
                .fetch_one(self.sqlite.get_conn())
                .await?;
        if !authorized {
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
        sqlx::query("DELETE FROM index_cleanup WHERE segment_id = ?")
            .bind(segment_id.to_string())
            .execute(self.sqlite.get_conn())
            .await?;
        Ok(())
    }

    /// Retry intentional deletion on startup, periodically, and after database deletion.
    pub async fn cleanup_deleted_indexes(&self) -> Result<(), LocalSegmentManagerError> {
        let pending: Vec<String> = sqlx::query_scalar("SELECT segment_id FROM index_cleanup")
            .fetch_all(self.sqlite.get_conn())
            .await?;
        for id in pending {
            let uuid = uuid::Uuid::parse_str(&id)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
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
    use chroma_types::{
        Chunk, KnnIndex, LogRecord, Operation, OperationRecord, Schema, SegmentScope, SegmentType,
        SegmentUuid,
    };

    #[tokio::test]
    async fn fresh_catalog_preserves_unrecognized_indexes() {
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join(SegmentUuid::new().to_string());
        tokio::fs::create_dir(&orphan).await.unwrap();
        tokio::fs::write(orphan.join("header.bin"), b"recoverable")
            .await
            .unwrap();
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
        manager.cleanup_deleted_indexes().await.unwrap();
        assert_eq!(
            tokio::fs::read(orphan.join("header.bin")).await.unwrap(),
            b"recoverable"
        );
    }

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
        assert!(orphan.exists());
        assert!(index_path.is_dir());
        sqlx::query("DELETE FROM segments WHERE id = ?")
            .bind(segment.id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        sqlx::query("INSERT INTO index_cleanup (segment_id) VALUES (?)")
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

    fn add(offset: i64, id: &str) -> Chunk<LogRecord> {
        Chunk::new(
            vec![LogRecord {
                log_offset: offset,
                record: OperationRecord {
                    id: id.to_string(),
                    embedding: Some(vec![offset as f32; 3]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            }]
            .into(),
        )
    }

    #[tokio::test]
    async fn checkpoint_validation_requires_persistent_storage() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        for persist_path in [None, Some(root.path().to_str().unwrap().to_string())] {
            let manager = LocalSegmentManager::try_from_config(
                &LocalSegmentManagerConfig {
                    hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                    persist_path,
                },
                &registry,
            )
            .await
            .unwrap();
            assert_eq!(
                manager
                    .validate_persisted_checkpoint(&SegmentUuid::new())
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::NotFound
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_validation_requires_id_map_and_accepts_deleted_vectors() {
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
            .bind("urn:chroma:segment/vector/hnsw-local-persisted")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let mut writer = manager
            .get_hnsw_writer(&collection, &segment, 3)
            .await
            .unwrap();
        // Native initialization creates structurally valid empty files, but no
        // ID map has been published. This is not a checkpoint authorizing purge.
        let folder = root.path().join(segment.id.to_string());
        assert!(
            !inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        assert_eq!(
            manager
                .validate_persisted_checkpoint(&segment.id)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        writer.index.set_sync_threshold(1).await;
        writer.apply_log_chunk(add(1, "a")).await.unwrap();
        manager
            .validate_persisted_checkpoint(&segment.id)
            .await
            .unwrap();
        writer
            .apply_log_chunk(Chunk::new(
                vec![LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "a".into(),
                        embedding: None,
                        encoding: None,
                        document: None,
                        metadata: None,
                        operation: Operation::Delete,
                    },
                }]
                .into(),
            ))
            .await
            .unwrap();
        // Tombstoned native slots with no live vectors are still a usable checkpoint.
        manager
            .validate_persisted_checkpoint(&segment.id)
            .await
            .unwrap();
        writer.index.close().await;
    }

    // Exceeds the Windows CRT's 512-stream limit if any of creation, save,
    // reader load, or writer load leaves four streams open per cached index.
    // Use a subprocess on Unix so lowering the limit cannot affect other tests.
    #[tokio::test]
    async fn cached_indexes_do_not_exhaust_file_handles() {
        #[cfg(unix)]
        if std::env::var_os("CHROMA_HNSW_FD_LIMIT_TEST_CHILD").is_none() {
            let output = std::process::Command::new("sh")
                .args(["-c", "ulimit -n 512 && exec \"$@\"", "hnsw-file-limit-test"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "local_segment_manager::tests::cached_indexes_do_not_exhaust_file_handles",
                    "--nocapture",
                ])
                .env("CHROMA_HNSW_FD_LIMIT_TEST_CHILD", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "limited-file subprocess failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let sqlite = get_new_sqlite_db().await;
        let config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
            persist_path: Some(root.path().to_str().unwrap().to_owned()),
        };
        let registry = Registry::new();
        registry.register(sqlite.clone());
        let manager = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        let mut segments = Vec::new();
        for _ in 0..160 {
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
            let mut writer = manager
                .get_hnsw_writer(&collection, &segment, 3)
                .await
                .unwrap();
            writer.index.set_sync_threshold(1).await;
            writer.apply_log_chunk(add(1, "a")).await.unwrap();
            segments.push((collection, segment));
        }
        // Keep the original cache alive while loading and retaining readers.
        let readers = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let reader = readers
                .get_hnsw_reader(collection, segment, 3)
                .await
                .unwrap();
            assert_eq!(reader.index.applied_state().await, (1, 1));
        }
        drop(readers);
        drop(manager);
        let writers = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let mut writer = writers
                .get_hnsw_writer(collection, segment, 3)
                .await
                .unwrap();
            writer.index.set_sync_threshold(1).await;
            writer.apply_log_chunk(add(2, "b")).await.unwrap();
            assert_eq!(writer.index.applied_state().await, (2, 2));
        }
        // Reopen the checkpoints to verify that closing streams after saves
        // preserved the new vectors and replay watermark.
        let fresh = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let reader = fresh.get_hnsw_reader(collection, segment, 3).await.unwrap();
            assert_eq!(reader.index.applied_state().await, (2, 2));
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"b".to_string())
                    .await
                    .unwrap(),
                vec![2.0; 3]
            );
        }
    }

    #[tokio::test]
    async fn shared_index_persists_after_replace_and_eviction() {
        let root = tempfile::tempdir().unwrap();
        let sqlite = get_new_sqlite_db().await;
        let config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
            persist_path: Some(root.path().to_str().unwrap().to_string()),
        };
        let registry = Registry::new();
        registry.register(sqlite.clone());
        let manager = LocalSegmentManager::try_from_config(&config, &registry)
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
            .bind("urn:chroma:segment/vector/hnsw-local-persisted")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(sqlite.get_conn())
            .await
            .unwrap();

        // Two concurrent misses share one index, and the loser must not
        // re-insert it into the cache.
        let (first, second) = tokio::join!(
            manager.get_hnsw_writer(&collection, &segment, 3),
            manager.get_hnsw_writer(&collection, &segment, 3),
        );
        let mut first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &second.index.inner));
        first.index.set_sync_threshold(1).await;
        first.apply_log_chunk(add(1, "a")).await.unwrap();

        // Evict while a caller still holds the index. The listener closes its
        // files; the next writer reuses the same index and must still persist.
        manager
            .hnsw_index_pool
            .remove(&IndexUuid(segment.id.0))
            .await;
        let mut reused = manager
            .get_hnsw_writer(&collection, &segment, 3)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &reused.index.inner));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        reused.apply_log_chunk(add(2, "b")).await.unwrap();
        drop((first, second, reused));

        // A fresh manager loads both records from disk.
        let registry = Registry::new();
        registry.register(sqlite);
        let fresh = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        let reader = fresh
            .get_hnsw_reader(&collection, &segment, 3)
            .await
            .unwrap();
        assert_eq!(reader.index.applied_state().await, (2, 2));
    }
}
