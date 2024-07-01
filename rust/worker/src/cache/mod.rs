pub mod cache;
pub mod config;
use crate::cache::cache::Cache;
use crate::cache::config::CacheConfig;
use crate::errors::ChromaError;
use std::hash::Hash;

pub(crate) async fn from_config<K, V>(
    config: &CacheConfig,
) -> Result<Cache<K, V>, Box<dyn ChromaError>>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    Ok(Cache::new(config))
}
