pub mod cache;
pub mod config;
use crate::cache::cache::Cache;
use crate::cache::config::CacheConfig;
use crate::config::Configurable;
use crate::errors::ChromaError;
use std::hash::Hash;

pub(crate) async fn from_config<K, V>(
    config: &CacheConfig,
) -> Result<Cache<K, V>, Box<dyn ChromaError>>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    match config {
        CacheConfig::Unbounded(_) => Ok(Cache::Unbounded(
            crate::cache::cache::UnboundedCache::try_from_config(config).await?,
        )),
        _ => Ok(Cache::Foyer(
            crate::cache::cache::FoyerCacheWrapper::try_from_config(config).await?,
        )),
    }
}
