pub mod cache;
pub mod config;

use crate::cache::Cache;
use crate::config::CacheConfig;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_types::Cacheable;
use std::hash::Hash;

pub async fn from_config<K, V>(config: &CacheConfig) -> Result<Cache<K, V>, Box<dyn ChromaError>>
where
    K: Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Clone + Cacheable + 'static,
{
    match config {
        CacheConfig::Unbounded(_) => Ok(Cache::Unbounded(
            crate::cache::UnboundedCache::try_from_config(config).await?,
        )),
        CacheConfig::Lru(_) => Ok(Cache::Foyer(
            crate::cache::FoyerCacheWrapper::try_from_config(config).await?,
        )),
        CacheConfig::Lfu(_) => Ok(Cache::Foyer(
            crate::cache::FoyerCacheWrapper::try_from_config(config).await?,
        )),
        CacheConfig::WeightedLru(_) => Ok(Cache::Foyer(
            crate::cache::FoyerCacheWrapper::try_from_config(config).await?,
        )),
    }
}
