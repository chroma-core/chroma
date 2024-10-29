use opentelemetry::global;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use chroma_error::ChromaError;
use clap::Parser;
use foyer::{
    CacheBuilder, DirectFsDeviceOptions, Engine, FifoConfig, FifoPicker, HybridCacheBuilder,
    InvalidRatioPicker, LargeEngineOptions, LfuConfig, LruConfig, RateLimitPicker, S3FifoConfig,
    StorageKey, StorageValue, TracingOptions,
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

    pub async fn build_memory_with_event_listener<K, V>(
        &self,
        tx: tokio::sync::mpsc::UnboundedSender<(K, V)>,
    ) -> Result<Box<dyn super::Cache<K, V>>, Box<dyn ChromaError>>
    where
        K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
        V: Clone + Send + Sync + Weighted + 'static,
    {
        Ok(Box::new(
            FoyerPlainCache::memory_with_event_listener(self, tx).await?,
        ))
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

struct Stopwatch<'a>(
    &'a opentelemetry::metrics::Histogram<u64>,
    std::time::Instant,
);

impl<'a> Stopwatch<'a> {
    fn new(histogram: &'a opentelemetry::metrics::Histogram<u64>) -> Self {
        Self(histogram, std::time::Instant::now())
    }
}

impl<'a> Drop for Stopwatch<'a> {
    fn drop(&mut self) {
        let elapsed = self.1.elapsed().as_micros() as u64;
        self.0.record(elapsed, &[]);
    }
}

#[derive(Clone)]
pub struct FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    cache: foyer::HybridCache<K, V>,
    get_latency: opentelemetry::metrics::Histogram<u64>,
    insert_latency: opentelemetry::metrics::Histogram<u64>,
    remove_latency: opentelemetry::metrics::Histogram<u64>,
    clear_latency: opentelemetry::metrics::Histogram<u64>,
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
        let tracing_options = TracingOptions::new()
            .with_record_hybrid_insert_threshold(Duration::from_micros(config.trace_insert_us as _))
            .with_record_hybrid_get_threshold(Duration::from_micros(config.trace_get_us as _))
            .with_record_hybrid_obtain_threshold(Duration::from_micros(config.trace_obtain_us as _))
            .with_record_hybrid_remove_threshold(Duration::from_micros(config.trace_remove_us as _))
            .with_record_hybrid_fetch_threshold(Duration::from_micros(config.trace_fetch_us as _));

        let builder = HybridCacheBuilder::<K, V>::new()
            .with_tracing_options(tracing_options)
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
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(dir)
                    .with_capacity(config.disk * MIB)
                    .with_file_size(config.file_size * MIB),
            )
            .with_flush(config.flush)
            .with_large_object_disk_cache_options(
                LargeEngineOptions::new()
                    .with_indexer_shards(config.shards)
                    .with_recover_concurrency(config.recover_concurrency)
                    .with_flushers(config.flushers)
                    .with_reclaimers(config.reclaimers)
                    .with_eviction_pickers(vec![
                        Box::new(InvalidRatioPicker::new(config.invalid_ratio)),
                        Box::new(FifoPicker::default()),
                    ]),
            );

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
        let meter = global::meter("chroma");
        let get_latency = meter.u64_histogram("get_latency").init();
        let insert_latency = meter.u64_histogram("insert_latency").init();
        let remove_latency = meter.u64_histogram("remove_latency").init();
        let clear_latency = meter.u64_histogram("clear_latency").init();
        Ok(FoyerHybridCache {
            cache,
            get_latency,
            insert_latency,
            remove_latency,
            clear_latency,
        })
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    #[tracing::instrument(skip(self, key))]
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        let _stopwatch = Stopwatch::new(&self.get_latency);
        Ok(self.cache.get(key).await?.map(|v| v.value().clone()))
    }

    #[tracing::instrument(skip(self, key, value))]
    async fn insert(&self, key: K, value: V) {
        let _stopwatch = Stopwatch::new(&self.insert_latency);
        self.cache.insert(key, value);
    }

    #[tracing::instrument(skip(self, key))]
    async fn remove(&self, key: &K) {
        let _stopwatch = Stopwatch::new(&self.remove_latency);
        self.cache.remove(key);
    }

    #[tracing::instrument(skip(self))]
    async fn clear(&self) -> Result<(), CacheError> {
        let _stopwatch = Stopwatch::new(&self.clear_latency);
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
    insert_latency: opentelemetry::metrics::Histogram<u64>,
    get_latency: opentelemetry::metrics::Histogram<u64>,
    remove_latency: opentelemetry::metrics::Histogram<u64>,
    clear_latency: opentelemetry::metrics::Histogram<u64>,
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
        let cache = CacheBuilder::new(config.capacity)
            .with_shards(config.shards)
            .build();
        let meter = global::meter("chroma");
        let insert_latency = meter.u64_histogram("insert_latency").init();
        let get_latency = meter.u64_histogram("get_latency").init();
        let remove_latency = meter.u64_histogram("remove_latency").init();
        let clear_latency = meter.u64_histogram("clear_latency").init();
        Ok(FoyerPlainCache {
            cache,
            insert_latency,
            get_latency,
            remove_latency,
            clear_latency,
        })
    }

    /// Build an in-memory cache that emits keys that get evicted to a channel.
    pub async fn memory_with_event_listener(
        config: &FoyerCacheConfig,
        tx: tokio::sync::mpsc::UnboundedSender<(K, V)>,
    ) -> Result<FoyerPlainCache<K, V>, Box<dyn ChromaError>> {
        struct TokioEventListener<K, V>(tokio::sync::mpsc::UnboundedSender<(K, V)>)
        where
            K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
            V: Clone + Send + Sync + Weighted + 'static;
        impl<K, V> foyer::EventListener for TokioEventListener<K, V>
        where
            K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
            V: Clone + Send + Sync + Weighted + 'static,
        {
            type Key = K;
            type Value = V;

            fn on_memory_release(&self, key: Self::Key, value: Self::Value)
            where
                K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
            {
                // NOTE(rescrv):  There's no mechanism by which we can error.  We could log a
                // metric, but this should really never happen.
                let _ = self.0.send((key, value));
            }
        }
        let evl = TokioEventListener(tx);

        let cache = CacheBuilder::new(config.capacity)
            .with_shards(config.shards)
            .with_event_listener(Arc::new(evl))
            .build();
        let get_latency = global::meter("chroma").u64_histogram("get_latency").init();
        let insert_latency = global::meter("chroma")
            .u64_histogram("insert_latency")
            .init();
        let remove_latency = global::meter("chroma")
            .u64_histogram("remove_latency")
            .init();
        let clear_latency = global::meter("chroma")
            .u64_histogram("clear_latency")
            .init();
        Ok(FoyerPlainCache {
            cache,
            insert_latency,
            get_latency,
            remove_latency,
            clear_latency,
        })
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    #[tracing::instrument(skip(self, key))]
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        let _stopwatch = Stopwatch::new(&self.get_latency);
        Ok(self.cache.get(key).map(|v| v.value().clone()))
    }

    #[tracing::instrument(skip(self, key, value))]
    async fn insert(&self, key: K, value: V) {
        let _stopwatch = Stopwatch::new(&self.insert_latency);
        self.cache.insert(key, value);
    }

    #[tracing::instrument(skip(self, key))]
    async fn remove(&self, key: &K) {
        let _stopwatch = Stopwatch::new(&self.remove_latency);
        self.cache.remove(key);
    }

    #[tracing::instrument(skip(self))]
    async fn clear(&self) -> Result<(), CacheError> {
        let _stopwatch = Stopwatch::new(&self.clear_latency);
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
