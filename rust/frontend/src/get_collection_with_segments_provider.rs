use std::sync::Arc;

use chroma_cache::{AysncPartitionedMutex, Cache};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::{sysdb, SysDb};
use chroma_types::{operator::Scan, CollectionAndSegments, CollectionUuid, QueryError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Serialize)]
pub struct CollectionsWithSegmentsProviderConfig {
    pub cache: chroma_cache::CacheConfig,
    pub permitted_parallelism: u32,
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

        Ok(Self {
            sysdb_client: sysdb_client.clone(),
            collections_with_segments_cache: collections_with_segments_cache.into(),
            sysdb_rpc_lock,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CollectionsWithSegmentsProvider {
    pub(crate) sysdb_client: Box<sysdb::SysDb>,
    pub(crate) collections_with_segments_cache:
        Arc<dyn Cache<CollectionUuid, CollectionAndSegments>>,
    pub(crate) sysdb_rpc_lock: chroma_cache::AysncPartitionedMutex<CollectionUuid>,
}

impl CollectionsWithSegmentsProvider {
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
            Some(collection_and_segments) => {
                println!("cache hit for collection {:?}", collection_id);
                collection_and_segments
            }
            None => {
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
                    Some(collection_and_segments) => {
                        println!(
                            "cache hit after acquiring mutex for collection {:?}",
                            collection_id
                        );
                        collection_and_segments
                    }
                    None => {
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
