use std::{sync::Arc, time::Duration};

use backon::ConstantBuilder;
use chroma_cache::{AysncPartitionedMutex, Cache};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::{sysdb, SysDb};
use chroma_types::{operator::Scan, CollectionAndSegments, CollectionUuid, QueryError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct CacheInvalidationRetryConfig {
    pub delay_ms: u32,
    pub max_retries: u32,
}

impl Default for CacheInvalidationRetryConfig {
    fn default() -> Self {
        Self {
            delay_ms: 0,
            max_retries: 3,
        }
    }
}

#[derive(Deserialize, Clone, Serialize)]
pub struct CollectionsWithSegmentsProviderConfig {
    pub cache: chroma_cache::CacheConfig,
    pub permitted_parallelism: u32,
    #[serde(default = "CacheInvalidationRetryConfig::default")]
    pub cache_invalidation_retry_policy: CacheInvalidationRetryConfig,
}

#[async_trait::async_trait]
impl Configurable<(CollectionsWithSegmentsProviderConfig, Box<SysDb>)>
    for CollectionsWithSegmentsProvider
{
    async fn try_from_config(
        (config, sysdb_client): &(CollectionsWithSegmentsProviderConfig, Box<SysDb>),
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

        Ok(Self {
            sysdb_client: sysdb_client.clone(),
            collections_with_segments_cache: collections_with_segments_cache.into(),
            sysdb_rpc_lock,
            retry_backoff,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CollectionsWithSegmentsProvider {
    pub(crate) sysdb_client: Box<sysdb::SysDb>,
    pub(crate) collections_with_segments_cache:
        Arc<dyn Cache<CollectionUuid, CollectionAndSegments>>,
    pub(crate) sysdb_rpc_lock: chroma_cache::AysncPartitionedMutex<CollectionUuid>,
    pub(crate) retry_backoff: ConstantBuilder,
}

impl CollectionsWithSegmentsProvider {
    pub(crate) fn get_retry_backoff(&self) -> ConstantBuilder {
        self.retry_backoff
    }

    pub(crate) async fn get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Scan, QueryError> {
        let collection_and_segments = match self
            .collections_with_segments_cache
            .get(&collection_id)
            .await
            .map_err(|_| QueryError::CollectionSegments)?
        {
            Some(collection_and_segments) => collection_and_segments,
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
                    .await
                    .map_err(|_| QueryError::CollectionSegments)?
                {
                    Some(collection_and_segments) => collection_and_segments,
                    None => {
                        tracing::info!("Cache miss again for collection {}", collection_id);
                        let collection_and_segments_sysdb = self
                            .sysdb_client
                            .get_collection_with_segments(collection_id)
                            .await
                            .map_err(|_| QueryError::CollectionSegments)?;
                        self.collections_with_segments_cache
                            .insert(collection_id, collection_and_segments_sysdb.clone())
                            .await;
                        collection_and_segments_sysdb
                    }
                }
            }
        };
        Ok(Scan {
            collection_and_segments,
        })
    }
}
