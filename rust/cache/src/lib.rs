use std::hash::Hash;

use ::foyer::{StorageKey, StorageValue};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod foyer;
mod nop;
mod unbounded;

use crate::nop::NopCache;
use crate::unbounded::UnboundedCache;

pub use foyer::FoyerCacheConfig;
pub use unbounded::UnboundedCacheConfig;

/// A CacheError represents an error that occurred while interacting with a cache.
///
/// InvalidCacheConfig is used at configuration.  It should not be returned in steady state.
/// DiskError captures errors that occur when serving from cache.
#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Invalid cache config")]
    InvalidCacheConfig(String),
    #[error("I/O error when serving from cache")]
    DiskError(#[from] anyhow::Error),
}

impl ChromaError for CacheError {
    fn code(&self) -> ErrorCodes {
        match self {
            CacheError::InvalidCacheConfig(_) => ErrorCodes::InvalidArgument,
            CacheError::DiskError(_) => ErrorCodes::Unavailable,
        }
    }
}

/// A cache configuration.
/// "unbounded" is a cache that doesn't evict.
/// "disk" is a foyer-backed cache that lives on disk.
/// "memory" is a foyer-backed cache that lives in memory.
#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum CacheConfig {
    // case-insensitive
    #[serde(rename = "unbounded")]
    Unbounded(UnboundedCacheConfig),
    #[serde(rename = "disk")]
    Disk(FoyerCacheConfig),
    #[serde(rename = "memory")]
    #[serde(alias = "lru")]
    #[serde(alias = "lfu")]
    #[serde(alias = "weighted_lru")]
    Memory(FoyerCacheConfig),
    #[serde(rename = "nop")]
    Nop,
}

/// A cache offers async access.  It's unspecified whether this cache is persistent or not.
#[async_trait::async_trait]
pub trait Cache<K, V>: Send + Sync
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    async fn insert(&self, key: K, value: V);
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError>;
    async fn remove(&self, key: &K);
    async fn clear(&self) -> Result<(), CacheError>;
}

/// A persistent cache extends the traits of a cache to require StorageKey and StorageValue.
pub trait PersistentCache<K, V>: Cache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
}

/// A trait to capture the weight of objects in the system.
pub trait Weighted {
    fn weight(&self) -> usize;
}

/// Create a new cache from the provided config.  This is solely for caches that cannot implement
/// the persistent cache trait.  Attempts to construct a disk-based cache will return an error.
pub async fn from_config<K, V>(
    config: &CacheConfig,
) -> Result<Box<dyn Cache<K, V>>, Box<dyn ChromaError>>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    match config {
        CacheConfig::Unbounded(unbounded_config) => {
            Ok(Box::new(UnboundedCache::new(unbounded_config)))
        }
        CacheConfig::Memory(c) => Ok(c.build_memory().await? as _),
        CacheConfig::Disk(_) => Err(Box::new(CacheError::InvalidCacheConfig(
            "from_config was called with disk".to_string(),
        ))),
        CacheConfig::Nop => Ok(Box::new(NopCache)),
    }
}

/// Create a new cache from the provided config.
pub async fn from_config_persistent<K, V>(
    config: &CacheConfig,
) -> Result<Box<dyn PersistentCache<K, V>>, Box<dyn ChromaError>>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    match config {
        CacheConfig::Unbounded(unbounded_config) => {
            Ok(Box::new(UnboundedCache::new(unbounded_config)))
        }
        CacheConfig::Memory(c) => Ok(c.build_memory_persistent().await?),
        CacheConfig::Disk(c) => Ok(c.build_hybrid().await? as _),
        CacheConfig::Nop => Ok(Box::new(NopCache)),
    }
}

/// Create a new cache for testing purposes.
pub fn new_cache_for_test<K, V>() -> Box<dyn PersistentCache<K, V>>
where
    K: Send + Sync + Clone + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Send + Sync + Clone + Weighted + StorageValue + 'static,
{
    Box::new(UnboundedCache::new(&UnboundedCacheConfig::default()))
}

/// Create a new cache for testing purposes.
pub fn new_non_persistent_cache_for_test<K, V>() -> Box<dyn Cache<K, V>>
where
    K: Send + Sync + Clone + Eq + PartialEq + Hash + 'static,
    V: Send + Sync + Clone + Weighted + 'static,
{
    Box::new(UnboundedCache::new(&UnboundedCacheConfig::default()))
}
