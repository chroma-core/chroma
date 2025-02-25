use backon::ConstantBuilder;
use chroma_cache::{AysncPartitionedMutex, Cache, CacheError};
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{CollectionAndSegments, CollectionUuid, GetCollectionWithSegmentsError};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CacheInvalidationRetryConfig {
    pub delay_ms: u32,
    pub max_retries: u32,
}

impl CacheInvalidationRetryConfig {
    pub fn new(delay_ms: u32, max_retries: u32) -> Self {
        Self {
            delay_ms,
            max_retries,
        }
    }
}

impl Default for CacheInvalidationRetryConfig {
    fn default() -> Self {
        Self {
            delay_ms: 0,
            max_retries: 3,
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct CollectionsWithSegmentsProviderConfig {
    pub cache: chroma_cache::CacheConfig,
    pub permitted_parallelism: u32,
    #[serde(default = "CacheInvalidationRetryConfig::default")]
    pub cache_invalidation_retry_policy: CacheInvalidationRetryConfig,
}

impl Default for CollectionsWithSegmentsProviderConfig {
    fn default() -> Self {
        Self {
            cache: chroma_cache::CacheConfig::Nop,
            permitted_parallelism: 100,
            cache_invalidation_retry_policy: CacheInvalidationRetryConfig::default(),
        }
    }
}

#[async_trait::async_trait]
impl Configurable<CollectionsWithSegmentsProviderConfig> for CollectionsWithSegmentsProvider {
    async fn try_from_config(
        config: &CollectionsWithSegmentsProviderConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let collections_with_segments_cache =
            chroma_cache::from_config::<CollectionUuid, CollectionAndSegments>(&config.cache)
                .await?;
        let sysdb_rpc_lock =
            AysncPartitionedMutex::with_parallelism(config.permitted_parallelism as usize, ());

        let retry_backoff = ConstantBuilder::default()
            .with_delay(Duration::from_millis(
                config.cache_invalidation_retry_policy.delay_ms as u64,
            ))
            .with_max_times(config.cache_invalidation_retry_policy.max_retries as usize);

        let sysdb = registry
            .get::<SysDb>()
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        Ok(Self {
            sysdb_client: sysdb,
            collections_with_segments_cache: collections_with_segments_cache.into(),
            sysdb_rpc_lock,
            retry_backoff,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CollectionsWithSegmentsProvider {
    pub(crate) sysdb_client: SysDb,
    pub(crate) collections_with_segments_cache:
        Arc<dyn Cache<CollectionUuid, CollectionAndSegments>>,
    pub(crate) sysdb_rpc_lock: chroma_cache::AysncPartitionedMutex<CollectionUuid>,
    pub(crate) retry_backoff: ConstantBuilder,
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
            CollectionsWithSegmentsProviderError::Cache(cache_error) => cache_error.code(),
            CollectionsWithSegmentsProviderError::SysDB(get_collection_with_segments_error) => {
                get_collection_with_segments_error.code()
            }
        }
    }
}

impl CollectionsWithSegmentsProvider {
    pub(crate) fn get_retry_backoff(&self) -> ConstantBuilder {
        self.retry_backoff
    }

    pub(crate) async fn get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, CollectionsWithSegmentsProviderError> {
        match self
            .collections_with_segments_cache
            .get(&collection_id)
            .await?
        {
            Some(collection_and_segments) => Ok(collection_and_segments),
            None => {
                tracing::info!("Cache miss for collection {}", collection_id);
                // We acquire a lock to prevent the sysdb from experiencing a thundering herd.
                // This can happen when a large number of threads try to get the same collection
                // at the same time.
                let _guard = self.sysdb_rpc_lock.lock(&collection_id).await;
                // Double checked locking pattern to avoid lock contention in the
                // happy path when the collection is already cached.
                match self
                    .collections_with_segments_cache
                    .get(&collection_id)
                    .await?
                {
                    Some(collection_and_segments) => Ok(collection_and_segments),
                    None => {
                        tracing::info!("Cache miss again for collection {}", collection_id);
                        let collection_and_segments_sysdb = self
                            .sysdb_client
                            .get_collection_with_segments(collection_id)
                            .await?;
                        // Insert only if the collection dimension is set.
                        if collection_and_segments_sysdb.collection.dimension.is_some() {
                            self.collections_with_segments_cache
                                .insert(collection_id, collection_and_segments_sysdb.clone())
                                .await;
                        }
                        Ok(collection_and_segments_sysdb)
                    }
                }
            }
        }
    }
}
