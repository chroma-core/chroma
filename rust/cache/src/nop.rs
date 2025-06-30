use super::{CacheError, StorageKey, StorageValue, Weighted};
use std::fmt::Debug;
use std::hash::Hash;

/// A zero-configuration cache that doesn't evict.
pub struct NopCache;

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for NopCache
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    async fn get(&self, _: &K) -> Result<Option<V>, CacheError> {
        Ok(None)
    }

    async fn insert(&self, _: K, _: V) {}

    async fn remove(&self, _: &K) {}

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }

    async fn obtain(&self, _: K) -> Result<Option<V>, CacheError> {
        Ok(None)
    }
}

impl Debug for NopCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NopCache")
    }
}

impl<K, V> super::PersistentCache<K, V> for NopCache
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + Weighted + StorageValue + 'static,
{
}
