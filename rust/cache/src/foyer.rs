use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use chroma_error::ChromaError;
use clap::Parser;
use foyer::{
    CacheBuilder, DirectFsDeviceOptionsBuilder, FifoConfig, HybridCacheBuilder, InvalidRatioPicker,
    LfuConfig, LruConfig, RateLimitPicker, S3FifoConfig, StorageKey, StorageValue, TracingConfig,
};
use serde::{Deserialize, Serialize};

use super::{CacheError, Weighted};

const MIB: usize = 1024 * 1024;

const fn default_capacity() -> usize {
    1048576
}

const fn default_mem() -> usize {
    1024
}

const fn default_disk() -> usize {
    1024
}

const fn default_file_size() -> usize {
    64
}

const fn default_flushers() -> usize {
    64
}

const fn default_flush() -> bool {
    false
}

const fn default_reclaimers() -> usize {
    4
}

const fn default_recover_concurrency() -> usize {
    16
}

const fn default_admission_rate_limit() -> usize {
    50
}

const fn default_shards() -> usize {
    64
}

fn default_eviction() -> String {
    "lfu".to_string()
}

const fn default_invalid_ratio() -> f64 {
    0.8
}

const fn default_trace_insert_us() -> usize {
    1000 * 1000
}

const fn default_trace_get_us() -> usize {
    1000 * 1000
}

const fn default_trace_obtain_us() -> usize {
    1000 * 1000
}

const fn default_trace_remove_us() -> usize {
    1000 * 1000
}

const fn default_trace_fetch_us() -> usize {
    1000 * 1000
}

#[derive(Deserialize, Debug, Clone, Serialize, Parser)]
pub struct FoyerCacheConfig {
    /// Directory for disk cache data.
    #[arg(short, long)]
    pub dir: Option<String>,

    /// In-memory cache capacity. (items)
    #[arg(long, default_value_t = 1048576)]
    #[serde(default = "default_capacity")]
    pub capacity: usize,

    /// In-memory cache capacity. (MiB)
    #[arg(long, default_value_t = 1024)]
    #[serde(default = "default_mem")]
    pub mem: usize,

    /// Disk cache capacity. (MiB)
    #[arg(long, default_value_t = 1024)]
    #[serde(default = "default_disk")]
    pub disk: usize,

    /// Disk cache file size. (MiB)
    #[arg(long, default_value_t = 64)]
    #[serde(default = "default_file_size")]
    pub file_size: usize,

    /// Flusher count.
    #[arg(long, default_value_t = 4)]
    #[serde(default = "default_flushers")]
    pub flushers: usize,

    /// AKA fsync
    #[arg(long, default_value_t = false)]
    #[serde(default = "default_flush")]
    pub flush: bool,

    /// Reclaimer count.
    #[arg(long, default_value_t = 4)]
    #[serde(default = "default_reclaimers")]
    pub reclaimers: usize,

    /// Recover concurrency.
    #[arg(long, default_value_t = 16)]
    #[serde(default = "default_recover_concurrency")]
    pub recover_concurrency: usize,

    /// Enable rated ticket admission picker if `admission_rate_limit > 0`. (MiB/s)
    #[arg(long, default_value_t = 50)]
    #[serde(default = "default_admission_rate_limit")]
    pub admission_rate_limit: usize,

    /// Shards of both in-memory cache and disk cache indexer.
    #[arg(long, default_value_t = 64)]
    #[serde(default = "default_shards")]
    pub shards: usize,

    /// Eviction algorithm to use
    #[arg(long, default_value = "lfu")]
    #[serde(default = "default_eviction")]
    pub eviction: String,

    /// Ratio of invalid entries to be evicted.
    #[arg(long, default_value_t = 0.8)]
    #[serde(default = "default_invalid_ratio")]
    pub invalid_ratio: f64,

    /// Record insert trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 1000)]
    #[serde(default = "default_trace_insert_us")]
    pub trace_insert_us: usize,

    /// Record get trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 1000)]
    #[serde(default = "default_trace_get_us")]
    pub trace_get_us: usize,

    /// Record obtain trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 1000)]
    #[serde(default = "default_trace_obtain_us")]
    pub trace_obtain_us: usize,

    /// Record remove trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 1000)]
    #[serde(default = "default_trace_remove_us")]
    pub trace_remove_us: usize,

    /// Record fetch trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 1000)]
    #[serde(default = "default_trace_fetch_us")]
    pub trace_fetch_us: usize,
}

impl FoyerCacheConfig {
    /// Build a hybrid disk and memory cache.
    pub async fn build_hybrid<K, V>(
        &self,
    ) -> Result<Box<dyn super::PersistentCache<K, V>>, Box<dyn ChromaError>>
    where
        K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
        V: Clone + Send + Sync + StorageValue + Weighted + 'static,
    {
        Ok(Box::new(FoyerHybridCache::hybrid(self).await?))
    }

    /// Build an in-memory-only cache.
    pub async fn build_memory<K, V>(
        &self,
    ) -> Result<Box<dyn super::Cache<K, V>>, Box<dyn ChromaError>>
    where
        K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
        V: Clone + Send + Sync + Weighted + 'static,
    {
        Ok(Box::new(FoyerPlainCache::memory(self).await?))
    }

    /// Build an in-memory-only cache.
    pub async fn build_memory_persistent<K, V>(
        &self,
    ) -> Result<Box<dyn super::PersistentCache<K, V>>, Box<dyn ChromaError>>
    where
        K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
        V: Clone + Send + Sync + Weighted + StorageValue + 'static,
    {
        Ok(Box::new(FoyerPlainCache::memory(self).await?))
    }
}

#[derive(Clone)]
pub struct FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    cache: foyer::HybridCache<K, V>,
}

impl<K, V> FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    /// Build a hybrid disk and memory cache.
    pub async fn hybrid(
        config: &FoyerCacheConfig,
    ) -> Result<FoyerHybridCache<K, V>, Box<dyn ChromaError>> {
        let tracing_config = TracingConfig::default();
        tracing_config
            .set_record_hybrid_insert_threshold(Duration::from_micros(config.trace_insert_us as _));
        tracing_config
            .set_record_hybrid_get_threshold(Duration::from_micros(config.trace_get_us as _));
        tracing_config
            .set_record_hybrid_obtain_threshold(Duration::from_micros(config.trace_obtain_us as _));
        tracing_config
            .set_record_hybrid_remove_threshold(Duration::from_micros(config.trace_remove_us as _));
        tracing_config
            .set_record_hybrid_fetch_threshold(Duration::from_micros(config.trace_fetch_us as _));

        let builder = HybridCacheBuilder::<K, V>::new()
            .with_tracing_config(tracing_config)
            .memory(config.mem * MIB)
            .with_shards(config.shards);

        let builder = match config.eviction.as_str() {
            "lru" => builder.with_eviction_config(LruConfig::default()),
            "lfu" => builder.with_eviction_config(LfuConfig::default()),
            "fifo" => builder.with_eviction_config(FifoConfig::default()),
            "s3fifo" => builder.with_eviction_config(S3FifoConfig::default()),
            _ => {
                return Err(Box::new(CacheError::InvalidCacheConfig(format!(
                    "eviction: {}",
                    config.eviction
                ))));
            }
        };

        let Some(dir) = config.dir.as_ref() else {
            return Err(Box::new(CacheError::InvalidCacheConfig(
                "missing dir".to_string(),
            )));
        };

        let mut builder = builder
            .with_weighter(|_, v| v.weight())
            .storage()
            .with_device_config(
                DirectFsDeviceOptionsBuilder::new(dir)
                    .with_capacity(config.disk * MIB)
                    .with_file_size(config.file_size * MIB)
                    .build(),
            )
            .with_flush(config.flush)
            .with_indexer_shards(config.shards)
            .with_recover_concurrency(config.recover_concurrency)
            .with_flushers(config.flushers)
            .with_reclaimers(config.reclaimers)
            .with_eviction_pickers(vec![Box::new(InvalidRatioPicker::new(
                config.invalid_ratio,
            ))]);

        if config.admission_rate_limit > 0 {
            builder = builder.with_admission_picker(Arc::new(RateLimitPicker::new(
                config.admission_rate_limit * MIB,
            )));
        }
        let cache = builder.build().await.map_err(|e| {
            Box::new(CacheError::InvalidCacheConfig(format!(
                "builder failed: {:?}",
                e
            ))) as _
        })?;
        Ok(FoyerHybridCache { cache })
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        Ok(self.cache.get(key).await?.map(|v| v.value().clone()))
    }

    async fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    async fn remove(&self, key: &K) {
        self.cache.remove(key);
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(self.cache.clear().await?)
    }
}

impl<K, V> super::PersistentCache<K, V> for FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
}

#[derive(Clone)]
pub struct FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    cache: foyer::Cache<K, V>,
}

impl<K, V> FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    /// Build an in-memory cache.
    pub async fn memory(
        config: &FoyerCacheConfig,
    ) -> Result<FoyerPlainCache<K, V>, Box<dyn ChromaError>> {
        let tracing_config = TracingConfig::default();
        tracing_config
            .set_record_hybrid_insert_threshold(Duration::from_micros(config.trace_insert_us as _));
        tracing_config
            .set_record_hybrid_get_threshold(Duration::from_micros(config.trace_get_us as _));
        tracing_config
            .set_record_hybrid_obtain_threshold(Duration::from_micros(config.trace_obtain_us as _));
        tracing_config
            .set_record_hybrid_remove_threshold(Duration::from_micros(config.trace_remove_us as _));
        tracing_config
            .set_record_hybrid_fetch_threshold(Duration::from_micros(config.trace_fetch_us as _));

        let cache = CacheBuilder::new(config.capacity)
            .with_shards(config.shards)
            .build();
        Ok(FoyerPlainCache { cache })
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        Ok(self.cache.get(key).map(|v| v.value().clone()))
    }

    async fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    async fn remove(&self, key: &K) {
        self.cache.remove(key);
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.cache.clear();
        Ok(())
    }
}

impl<K, V> super::PersistentCache<K, V> for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + Weighted + StorageValue + 'static,
{
}
