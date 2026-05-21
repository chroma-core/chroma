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
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use uuid::Uuid;

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

const HNSW_DELETE_TOMBSTONE_MARKER: &str = ".deleted.";
const ORPHANED_HNSW_INDEX_MIN_AGE: Duration = Duration::from_secs(6 * 60 * 60);
const ORPHANED_HNSW_INDEX_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);

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
    hnsw_index_load_locks: AysncPartitionedMutex<IndexUuid>,
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
        if let Some(persist_root) = config.persist_path.clone() {
            let sqlite = sqldb.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(err) =
                        cleanup_orphaned_hnsw_indexes(Path::new(&persist_root), &sqlite).await
                    {
                        tracing::warn!(
                            error = %err,
                            persist_root = %persist_root,
                            "failed to clean up orphaned persisted HNSW indexes"
                        );
                    }
                    tokio::time::sleep(ORPHANED_HNSW_INDEX_CLEANUP_INTERVAL).await;
                }
            });
        }
        let res = Self {
            hnsw_index_pool: hnsw_index_pool.into(),
            hnsw_index_load_locks: AysncPartitionedMutex::new(()),
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
    #[error("Error deleting hnsw index files: {0}")]
    DeleteHnswIndexError(#[from] std::io::Error),
}

impl ChromaError for LocalSegmentManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalSegmentManagerError::LocalHnswSegmentReaderError(e) => e.code(),
            LocalSegmentManagerError::PoolCacheError(e) => e.code(),
            LocalSegmentManagerError::LocalHnswSegmentWriterError(e) => e.code(),
            LocalSegmentManagerError::DeleteHnswIndexError(_) => ErrorCodes::Internal,
        }
    }
}

type HnswIndexCleanupError = Box<dyn std::error::Error + Send + Sync>;

async fn active_segment_ids(sqlite: &SqliteDb) -> Result<HashSet<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>("SELECT id FROM segments")
        .fetch_all(sqlite.get_conn())
        .await
        .map(|ids| ids.into_iter().collect())
}

fn hnsw_delete_tombstone_path(persist_root: &Path, segment_id: SegmentUuid) -> PathBuf {
    persist_root.join(format!(
        "{}{}{}",
        segment_id,
        HNSW_DELETE_TOMBSTONE_MARKER,
        Uuid::new_v4()
    ))
}

fn hnsw_index_dir_segment_id(file_name: &str) -> Option<&str> {
    let segment_id = file_name
        .split_once(HNSW_DELETE_TOMBSTONE_MARKER)
        .map(|(segment_id, _)| segment_id)
        .unwrap_or(file_name);
    Uuid::parse_str(segment_id).ok().map(|_| segment_id)
}

fn should_remove_hnsw_index_dir(
    file_name: &str,
    modified: SystemTime,
    active_segments: &HashSet<String>,
    now: SystemTime,
) -> bool {
    let Some(segment_id) = hnsw_index_dir_segment_id(file_name) else {
        return false;
    };
    if active_segments.contains(segment_id) {
        return false;
    }
    if file_name.contains(HNSW_DELETE_TOMBSTONE_MARKER) {
        return true;
    }
    now.duration_since(modified)
        .map(|age| age >= ORPHANED_HNSW_INDEX_MIN_AGE)
        .unwrap_or(false)
}

async fn remove_hnsw_index_dir(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

async fn cleanup_orphaned_hnsw_indexes(
    persist_root: &Path,
    sqlite: &SqliteDb,
) -> Result<(), HnswIndexCleanupError> {
    let active_segments = active_segment_ids(sqlite).await?;
    let mut entries = match tokio::fs::read_dir(persist_root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let file_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %entry.path().display(),
                    "failed to inspect persisted HNSW path while cleaning up"
                );
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let metadata = match entry.metadata().await {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %entry.path().display(),
                    "failed to read persisted HNSW path metadata while cleaning up"
                );
                continue;
            }
        };
        let modified = match metadata.modified() {
            Ok(modified) => modified,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %entry.path().display(),
                    "failed to read persisted HNSW path age while cleaning up"
                );
                continue;
            }
        };
        if !should_remove_hnsw_index_dir(
            file_name.as_ref(),
            modified,
            &active_segments,
            SystemTime::now(),
        ) {
            continue;
        }

        if let Err(err) = remove_hnsw_index_dir(&entry.path()).await {
            tracing::warn!(
                error = %err,
                path = %entry.path().display(),
                "failed to remove orphaned persisted HNSW index"
            );
        }
    }

    Ok(())
}

async fn delete_hnsw_index_files(
    persist_root: &Path,
    segment_id: SegmentUuid,
) -> Result<(), std::io::Error> {
    let index_folder = persist_root.join(segment_id.to_string());
    let tombstone_path = hnsw_delete_tombstone_path(persist_root, segment_id);

    match tokio::fs::rename(&index_folder, &tombstone_path).await {
        Ok(()) => {
            if let Err(err) = tokio::fs::write(tombstone_path.join(".delete-tombstone"), b"").await
            {
                tracing::warn!(
                    error = %err,
                    path = %tombstone_path.display(),
                    "failed to refresh persisted HNSW delete tombstone age"
                );
            }
            remove_hnsw_index_dir(&tombstone_path).await
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
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
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentReader::from_index(hnsw_index));
        }

        let _guard = self.hnsw_index_load_locks.lock(&index_uuid).await;
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentReader::from_index(hnsw_index));
        }

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

    pub async fn get_hnsw_writer(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentWriter, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentWriter::from_index(hnsw_index)?);
        }

        let _guard = self.hnsw_index_load_locks.lock(&index_uuid).await;
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            return Ok(LocalHnswSegmentWriter::from_index(hnsw_index)?);
        }

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
        self.hnsw_index_pool
            .insert(index_uuid, writer.index.clone())
            .await;
        Ok(writer)
    }

    pub async fn evict_hnsw_index(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<(), LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment_id.0);
        let _guard = self.hnsw_index_load_locks.lock(&index_uuid).await;
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            self.hnsw_index_pool.remove(&index_uuid).await;
            hnsw_index.close().await;
        }
        Ok(())
    }

    pub async fn delete_hnsw_index(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<(), LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment_id.0);
        let _guard = self.hnsw_index_load_locks.lock(&index_uuid).await;
        if let Some(hnsw_index) = self.hnsw_index_pool.get(&index_uuid).await? {
            self.hnsw_index_pool.remove(&index_uuid).await;
            hnsw_index.close().await;
        }
        if let Some(persist_root) = &self.persist_root {
            let persist_root = Path::new(persist_root);
            if let Err(err) = cleanup_orphaned_hnsw_indexes(persist_root, &self.sqlite).await {
                tracing::warn!(
                    error = %err,
                    persist_root = %persist_root.display(),
                    "failed to clean up orphaned persisted HNSW indexes before deletion"
                );
            }
            delete_hnsw_index_files(persist_root, segment_id).await?;
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
    use chroma_config::{registry::Registry, Configurable};
    use chroma_sqlite::config::SqliteDBConfig;
    use chroma_types::CollectionUuid;

    #[tokio::test]
    async fn cleanup_orphaned_hnsw_indexes_preserves_fresh_segment_dirs() {
        let registry = Registry::new();
        let sqlite = SqliteDb::try_from_config(&SqliteDBConfig::default(), &registry)
            .await
            .expect("sqlite");
        let persist_root = tempfile::tempdir().expect("persist root");

        let collection_id = CollectionUuid::new();
        let active_segment_id = SegmentUuid::new();
        let orphaned_segment_id = SegmentUuid::new();
        let tombstoned_segment_id = SegmentUuid::new();
        let active_path = persist_root.path().join(active_segment_id.to_string());
        let orphaned_path = persist_root.path().join(orphaned_segment_id.to_string());
        let tombstoned_path = persist_root.path().join(format!(
            "{}{}{}",
            tombstoned_segment_id,
            HNSW_DELETE_TOMBSTONE_MARKER,
            Uuid::new_v4()
        ));
        let non_index_path = persist_root.path().join("not-an-index");

        tokio::fs::create_dir_all(&active_path)
            .await
            .expect("active");
        tokio::fs::create_dir_all(&orphaned_path)
            .await
            .expect("orphaned");
        tokio::fs::create_dir_all(&tombstoned_path)
            .await
            .expect("tombstoned");
        tokio::fs::create_dir_all(&non_index_path)
            .await
            .expect("non-index");
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES ($1, $2, $3, $4)")
            .bind(active_segment_id.to_string())
            .bind("hnsw-local-persisted")
            .bind("VECTOR")
            .bind(collection_id.to_string())
            .execute(sqlite.get_conn())
            .await
            .expect("insert segment");

        cleanup_orphaned_hnsw_indexes(persist_root.path(), &sqlite)
            .await
            .expect("cleanup");

        assert!(active_path.exists());
        assert!(orphaned_path.exists());
        assert!(!tombstoned_path.exists());
        assert!(non_index_path.exists());
    }

    #[test]
    fn should_remove_hnsw_index_dir_age_gates_plain_orphans() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(12 * 60 * 60);
        let old = now - ORPHANED_HNSW_INDEX_MIN_AGE - Duration::from_secs(1);
        let fresh = now - Duration::from_secs(60);
        let active_segment_id = SegmentUuid::new();
        let orphaned_segment_id = SegmentUuid::new();
        let tombstoned_name = format!(
            "{}{}{}",
            orphaned_segment_id,
            HNSW_DELETE_TOMBSTONE_MARKER,
            Uuid::new_v4()
        );
        let mut active_segments = HashSet::new();
        active_segments.insert(active_segment_id.to_string());

        assert!(!should_remove_hnsw_index_dir(
            &active_segment_id.to_string(),
            old,
            &active_segments,
            now
        ));
        assert!(!should_remove_hnsw_index_dir(
            &orphaned_segment_id.to_string(),
            fresh,
            &active_segments,
            now
        ));
        assert!(should_remove_hnsw_index_dir(
            &orphaned_segment_id.to_string(),
            old,
            &active_segments,
            now
        ));
        assert!(should_remove_hnsw_index_dir(
            &tombstoned_name,
            fresh,
            &active_segments,
            now
        ));
        active_segments.insert(orphaned_segment_id.to_string());
        assert!(!should_remove_hnsw_index_dir(
            &tombstoned_name,
            fresh,
            &active_segments,
            now
        ));
        assert!(!should_remove_hnsw_index_dir(
            "not-an-index",
            old,
            &active_segments,
            now
        ));
    }
}
