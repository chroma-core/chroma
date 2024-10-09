use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

use super::{CacheError, StorageKey, StorageValue, Weighted};

/// A zero-configuration cache that doesn't evict.
/// Mostly useful for testing.
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct UnboundedCacheConfig {}

impl UnboundedCacheConfig {
    pub fn build<K, V>(&self) -> UnboundedCache<K, V>
    where
        K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
        V: Clone + Send + Sync + Clone + Weighted + 'static,
    {
        UnboundedCache::new(self)
    }
}

/// A zero-configuration cache that doesn't evict.
pub struct UnboundedCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Clone + Weighted + 'static,
{
    cache: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> UnboundedCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Clone + Weighted + 'static,
{
    pub fn new(_: &UnboundedCacheConfig) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for UnboundedCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        let read_guard = self.cache.read();
        let value = read_guard.get(key);
        Ok(value.cloned())
    }

    async fn insert(&self, key: K, value: V) {
        self.cache.write().insert(key, value);
    }

    async fn remove(&self, key: &K) {
        self.cache.write().remove(key);
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.cache.write().clear();
        Ok(())
    }
}

impl<K, V> super::PersistentCache<K, V> for UnboundedCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + Weighted + StorageValue + 'static,
{
}
