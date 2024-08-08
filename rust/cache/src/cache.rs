use crate::config::CacheConfig;
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use core::hash::Hash;
use foyer::{Cache as FoyerCache, CacheBuilder, LfuConfig, LruConfig};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
pub enum Cache<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    Unbounded(UnboundedCache<K, V>),
    Foyer(FoyerCacheWrapper<K, V>),
}

impl<K: Send + Sync + Hash + Eq + 'static, V: Send + Sync + Clone + 'static> Cache<K, V> {
    pub fn new(config: &CacheConfig) -> Self {
        match config {
            CacheConfig::Unbounded(_) => Cache::Unbounded(UnboundedCache::new(config)),
            _ => Cache::Foyer(FoyerCacheWrapper::new(config)),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        match self {
            Cache::Unbounded(cache) => cache.insert(key, value),
            Cache::Foyer(cache) => {
                cache.insert(key, value);
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self {
            Cache::Unbounded(cache) => cache.get(key),
            Cache::Foyer(cache) => {
                let entry = cache.get(key);
                match entry {
                    Some(v) => {
                        let value = v.to_owned();
                        Some(value)
                    }
                    None => None,
                }
            }
        }
    }

    pub fn remove(&self, key: &K) {
        match self {
            Cache::Unbounded(cache) => {
                cache.cache.write().remove(key);
            }
            Cache::Foyer(cache) => {
                cache.cache.remove(key);
            }
        }
    }
}

#[derive(Clone)]
pub struct UnboundedCache<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    cache: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> UnboundedCache<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(config: &CacheConfig) -> Self {
        match config {
            CacheConfig::Unbounded(_) => UnboundedCache {
                cache: Arc::new(RwLock::new(HashMap::new())),
            },
            _ => panic!("Invalid cache configuration"),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.cache.write().insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let read_guard = self.cache.read();
        let value = read_guard.get(key);
        match value {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }
}

#[derive(Clone)]
pub struct FoyerCacheWrapper<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    cache: FoyerCache<K, V>,
}

impl<K, V> FoyerCacheWrapper<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(config: &CacheConfig) -> Self {
        match config {
            CacheConfig::Lru(lru) => {
                // TODO: add more eviction config
                let eviction_config = LruConfig::default();
                let cache_builder =
                    CacheBuilder::new(lru.capacity).with_eviction_config(eviction_config);
                FoyerCacheWrapper {
                    cache: cache_builder.build(),
                }
            }
            CacheConfig::Lfu(lfu) => {
                // TODO: add more eviction config
                let eviction_config = LfuConfig::default();
                let cache_builder =
                    CacheBuilder::new(lfu.capacity).with_eviction_config(eviction_config);
                FoyerCacheWrapper {
                    cache: cache_builder.build(),
                }
            }
            _ => panic!("Invalid cache configuration"),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let entry = self.cache.get(key);
        match entry {
            Some(v) => {
                let value = v.value().to_owned();
                Some(value)
            }
            None => None,
        }
    }
}

#[async_trait]
impl<K, V> Configurable<CacheConfig> for UnboundedCache<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    async fn try_from_config(config: &CacheConfig) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            CacheConfig::Unbounded(_) => Ok(UnboundedCache::new(config)),
            _ => Err(Box::new(CacheConfigError::InvalidCacheConfig)),
        }
    }
}

#[async_trait]
impl<K, V> Configurable<CacheConfig> for FoyerCacheWrapper<K, V>
where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + Clone + 'static,
{
    async fn try_from_config(config: &CacheConfig) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            CacheConfig::Lru(_lru) => Ok(FoyerCacheWrapper::new(config)),
            CacheConfig::Lfu(_lfu) => Ok(FoyerCacheWrapper::new(config)),
            _ => Err(Box::new(CacheConfigError::InvalidCacheConfig)),
        }
    }
}

#[derive(Error, Debug)]
pub enum CacheConfigError {
    #[error("Invalid cache config")]
    InvalidCacheConfig,
}

impl ChromaError for CacheConfigError {
    fn code(&self) -> ErrorCodes {
        match self {
            CacheConfigError::InvalidCacheConfig => ErrorCodes::InvalidArgument,
        }
    }
}
