use chroma_cache::{AysncPartitionedMutex, Cache, CacheError, Weighted};
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{CollectionAndSegments, CollectionUuid, GetCollectionWithSegmentsError};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct CollectionsWithSegmentsProviderConfig {
    pub cache: chroma_cache::CacheConfig,
    pub cache_ttl_secs: u32,
    pub permitted_parallelism: u32,
}

impl Default for CollectionsWithSegmentsProviderConfig {
    fn default() -> Self {
        Self {
            cache: chroma_cache::CacheConfig::Nop,
            cache_ttl_secs: 60,
            permitted_parallelism: 100,
        }
    }
}

#[async_trait::async_trait]
impl Configurable<CollectionsWithSegmentsProviderConfig> for CollectionsWithSegmentsProvider {
    async fn try_from_config(
        config: &CollectionsWithSegmentsProviderConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let collections_with_segments_cache = chroma_cache::from_config::<
            CollectionUuid,
            CollectionAndSegmentsWithTtl,
        >(&config.cache)
        .await?;
        let sysdb_rpc_lock =
            AysncPartitionedMutex::with_parallelism(config.permitted_parallelism as usize, ());

        let sysdb = registry
            .get::<SysDb>()
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        Ok(Self {
            sysdb_client: sysdb,
            collections_with_segments_cache: collections_with_segments_cache.into(),
            cache_ttl_secs: config.cache_ttl_secs,
            sysdb_rpc_lock,
        })
    }
}

impl Weighted for CollectionAndSegmentsWithTtl {
    fn weight(&self) -> usize {
        1
    }
}

#[derive(Clone, Debug)]
pub struct CollectionAndSegmentsWithTtl {
    pub collection_and_segments: CollectionAndSegments,
    // Duration since unix epoch upto which the cache entry is valid.
    pub expires_at: Duration,
}

#[derive(Clone, Debug)]
pub struct CollectionsWithSegmentsProvider {
    pub(crate) sysdb_client: SysDb,
    pub(crate) collections_with_segments_cache:
        Arc<dyn Cache<CollectionUuid, CollectionAndSegmentsWithTtl>>,
    pub(crate) cache_ttl_secs: u32,
    pub(crate) sysdb_rpc_lock: chroma_cache::AysncPartitionedMutex<CollectionUuid>,
}

#[derive(Debug, Error)]
pub(crate) enum CollectionsWithSegmentsProviderError {
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error(transparent)]
    SysDB(#[from] GetCollectionWithSegmentsError),
}

impl ChromaError for CollectionsWithSegmentsProviderError {
    fn code(&self) -> ErrorCodes {
        match self {
            CollectionsWithSegmentsProviderError::Cache(e) => e.code(),
            CollectionsWithSegmentsProviderError::SysDB(e) => e.code(),
        }
    }
}

impl CollectionsWithSegmentsProvider {
    pub(crate) async fn get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, CollectionsWithSegmentsProviderError> {
        if let Some(collection_and_segments_with_ttl) = self
            .collections_with_segments_cache
            .get(&collection_id)
            .await?
        {
            if collection_and_segments_with_ttl.expires_at
                >= SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Do not deploy before UNIX epoch")
            {
                return Ok(collection_and_segments_with_ttl.collection_and_segments);
            }
        }

        let collection_and_segments_sysdb = {
            // We acquire a lock to prevent the sysdb from experiencing a thundering herd.
            // This can happen when a large number of threads try to get the same collection
            // at the same time.
            let _guard = self.sysdb_rpc_lock.lock(&collection_id).await;
            // Double checked locking pattern to avoid lock contention in the
            // happy path when the collection is already cached.
            if let Some(collection_and_segments_with_ttl) = self
                .collections_with_segments_cache
                .get(&collection_id)
                .await?
            {
                if collection_and_segments_with_ttl.expires_at
                    > SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Do not deploy before UNIX epoch")
                {
                    return Ok(collection_and_segments_with_ttl.collection_and_segments);
                }
            }
            tracing::info!("Cache miss for collection {}", collection_id);
            self.sysdb_client
                .get_collection_with_segments(collection_id)
                .await?
        };

        self.set_collection_with_segments(collection_and_segments_sysdb.clone())
            .await;
        Ok(collection_and_segments_sysdb)
    }

    pub(crate) async fn set_collection_with_segments(
        &mut self,
        collection_and_segments: CollectionAndSegments,
    ) {
        // Insert only if the collection dimension is set.
        if collection_and_segments.collection.dimension.is_some() {
            let collection_id = collection_and_segments.collection.collection_id;
            let collection_and_segments_with_ttl = CollectionAndSegmentsWithTtl {
                collection_and_segments,
                expires_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Do not deploy before UNIX epoch")
                    + Duration::from_secs(self.cache_ttl_secs as u64), // Cache for 1 minute
            };
            self.collections_with_segments_cache
                .insert(collection_id, collection_and_segments_with_ttl)
                .await;
        }
    }
}
