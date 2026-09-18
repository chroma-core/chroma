use chroma_cache::{AysncPartitionedMutex, Cache, CacheConfig, CacheError, FoyerCacheConfig};
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::IndexUuid;
use chroma_sqlite::db::SqliteDb;
use chroma_types::{Collection, Segment};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
            live_indexes: AysncPartitionedMutex::with_parallelism(16, HashMap::new()),
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
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: None,
            },
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalMemory,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
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
    }
}
