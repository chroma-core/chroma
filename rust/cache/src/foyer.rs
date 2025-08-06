use super::{CacheError, Weighted};
use ahash::RandomState;
use chroma_error::ChromaError;
use chroma_tracing::util::{StopWatchUnit, Stopwatch};
use clap::Parser;
use foyer::{
    CacheBuilder, DirectFsDeviceOptions, Engine, FifoConfig, FifoPicker, HybridCacheBuilder,
    InvalidRatioPicker, LargeEngineOptions, LfuConfig, LruConfig, S3FifoConfig, StorageKey,
    StorageValue, Throttle, TracingOptions,
};
use opentelemetry::{global, KeyValue};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

pub const MIB: usize = 1024 * 1024;

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
    4
}

const fn default_flush() -> bool {
    false
}

const fn default_reclaimers() -> usize {
    2
}

const fn default_recover_concurrency() -> usize {
    16
}

const fn default_deterministic_hashing() -> bool {
    true
}

const fn default_admission_rate_limit() -> usize {
    100
}

const fn default_shards() -> usize {
    64
}

const fn default_buffer_pool_size() -> usize {
    // See https://github.com/foyer-rs/foyer/discussions/751
    // This should be at least max_entry_size * flushers.
    256
}

fn default_eviction() -> String {
    "lru".to_string()
}

const fn default_invalid_ratio() -> f64 {
    0.8
}

const fn default_trace_insert_us() -> usize {
    1000 * 100
}

const fn default_trace_get_us() -> usize {
    1000 * 100
}

const fn default_trace_obtain_us() -> usize {
    1000 * 100
}

const fn default_trace_remove_us() -> usize {
    1000 * 100
}

const fn default_trace_fetch_us() -> usize {
    1000 * 100
}

fn default_name() -> String {
    String::from("foyer")
}

#[derive(Deserialize, Debug, Clone, Serialize, Parser)]
pub struct FoyerCacheConfig {
    /// Name of the cache. All metrics for the cache are prefixed with name_.
    #[arg(long, default_value = "foyer")]
    #[serde(default = "default_name")]
    pub name: String,

    /// Directory for disk cache data.
    #[arg(short, long)]
    pub dir: Option<String>,

    /// In-memory cache capacity. (weighted units)
    #[arg(long, default_value_t = 1048576)]
    #[serde(default = "default_capacity")]
    pub capacity: usize,

    /// In-memory cache capacity. (weighted units)
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

    /// Buffer pool size. (MiB)
    /// This should be atleast max_entry_size * flushers.
    /// See https://github.com/foyer-rs/foyer/discussions/751
    #[arg(long, default_value_t = 256)]
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool: usize,

    /// AKA fsync
    #[arg(long, default_value_t = false)]
    #[serde(default = "default_flush")]
    pub flush: bool,

    /// Reclaimer count.
    #[arg(long, default_value_t = 2)]
    #[serde(default = "default_reclaimers")]
    pub reclaimers: usize,

    /// Recover concurrency.
    #[arg(long, default_value_t = 16)]
    #[serde(default = "default_recover_concurrency")]
    pub recover_concurrency: usize,

    /// Enable deterministic hashing.
    /// If true, the cache will use a deterministic hasher which is stable
    /// across restarts. Note that this hasher is not necessarily stable across
    /// architectures or versions of foyer, and the underlying AHash.
    #[arg(long, default_value_t = true)]
    #[serde(default = "default_deterministic_hashing")]
    pub deterministic_hashing: bool,

    /// Enable rated ticket admission picker if `admission_rate_limit > 0`. (MiB/s)
    #[arg(long, default_value_t = 100)]
    #[serde(default = "default_admission_rate_limit")]
    pub admission_rate_limit: usize,

    /// Shards of both in-memory cache and disk cache indexer.
    #[arg(long, default_value_t = 64)]
    #[serde(default = "default_shards")]
    pub shards: usize,

    /// Eviction algorithm to use
    #[arg(long, default_value = "lru")]
    #[serde(default = "default_eviction")]
    pub eviction: String,

    /// Ratio of invalid entries to be evicted.
    #[arg(long, default_value_t = 0.8)]
    #[serde(default = "default_invalid_ratio")]
    pub invalid_ratio: f64,

    /// Record insert trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 100)]
    #[serde(default = "default_trace_insert_us")]
    pub trace_insert_us: usize,

    /// Record get trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 100)]
    #[serde(default = "default_trace_get_us")]
    pub trace_get_us: usize,

    /// Record obtain trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 100)]
    #[serde(default = "default_trace_obtain_us")]
    pub trace_obtain_us: usize,

    /// Record remove trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 100)]
    #[serde(default = "default_trace_remove_us")]
    pub trace_remove_us: usize,

    /// Record fetch trace threshold. Only effective with "mtrace" feature.
    #[arg(long, default_value_t = 1000 * 100)]
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

    pub async fn build_hybrid_test<K, V>(
        &self,
    ) -> Result<Box<FoyerHybridCache<K, V>>, Box<dyn ChromaError>>
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

impl Default for FoyerCacheConfig {
    fn default() -> Self {
        FoyerCacheConfig {
            name: default_name(),
            dir: None,
            capacity: default_capacity(),
            mem: default_mem(),
            disk: default_disk(),
            file_size: default_file_size(),
            flushers: default_flushers(),
            flush: default_flush(),
            reclaimers: default_reclaimers(),
            recover_concurrency: default_recover_concurrency(),
            deterministic_hashing: default_deterministic_hashing(),
            admission_rate_limit: default_admission_rate_limit(),
            shards: default_shards(),
            eviction: default_eviction(),
            invalid_ratio: default_invalid_ratio(),
            trace_insert_us: default_trace_insert_us(),
            trace_get_us: default_trace_get_us(),
            trace_obtain_us: default_trace_obtain_us(),
            trace_remove_us: default_trace_remove_us(),
            trace_fetch_us: default_trace_fetch_us(),
            buffer_pool: default_buffer_pool_size(),
        }
    }
}

#[derive(Clone)]
pub struct FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    cache: foyer::HybridCache<K, V, RandomState>,
    cache_hit: opentelemetry::metrics::Counter<u64>,
    cache_miss: opentelemetry::metrics::Counter<u64>,
    get_latency: opentelemetry::metrics::Histogram<u64>,
    obtain_latency: opentelemetry::metrics::Histogram<u64>,
    insert_latency: opentelemetry::metrics::Histogram<u64>,
    remove_latency: opentelemetry::metrics::Histogram<u64>,
    clear_latency: opentelemetry::metrics::Histogram<u64>,
    hostname: KeyValue,
}

impl<K, V> Debug for FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerHybridCache").finish()
    }
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

        let otel_0_27_metrics = Box::new(
            mixtrics::registry::opentelemetry_0_27::OpenTelemetryMetricsRegistry::new(
                global::meter("chroma"),
            ),
        );
        let builder = HybridCacheBuilder::<K, V>::new()
            .with_name(config.name.clone())
            .with_metrics_registry(otel_0_27_metrics)
            .with_tracing_options(tracing_options.clone())
            .with_policy(foyer::HybridCachePolicy::WriteOnInsertion)
            .memory(config.mem)
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

        let builder = match config.deterministic_hashing {
            true => {
                // These are generated from a good RNG.
                let rs = RandomState::with_seeds(
                    18408126631592559320,
                    14098607199905812554,
                    3530350452151671086,
                    4042281453092388365,
                );
                builder.with_hash_builder(rs)
            }
            false => builder.with_hash_builder(RandomState::new()),
        };

        let Some(dir) = config.dir.as_ref() else {
            return Err(Box::new(CacheError::InvalidCacheConfig(
                "missing dir".to_string(),
            )));
        };

        let mut device_options = DirectFsDeviceOptions::new(dir)
            .with_capacity(config.disk * MIB)
            .with_file_size(config.file_size * MIB);
        if config.admission_rate_limit > 0 {
            device_options = device_options.with_throttle(
                Throttle::new().with_write_throughput(config.admission_rate_limit * MIB),
            );
        }

        let builder = builder
            .with_weighter(|_, v| v.weight())
            .storage(Engine::Large)
            .with_device_options(device_options)
            .with_flush(config.flush)
            .with_recover_mode(foyer::RecoverMode::Strict)
            .with_large_object_disk_cache_options(
                LargeEngineOptions::new()
                    .with_indexer_shards(config.shards)
                    .with_recover_concurrency(config.recover_concurrency)
                    .with_flushers(config.flushers)
                    .with_reclaimers(config.reclaimers)
                    .with_buffer_pool_size(config.buffer_pool * MIB)
                    .with_eviction_pickers(vec![
                        Box::new(InvalidRatioPicker::new(config.invalid_ratio)),
                        Box::new(FifoPicker::default()),
                    ]),
            );

        let cache = builder.build().await.map_err(|e| {
            CacheError::InvalidCacheConfig(format!("builder failed: {:?}", e)).boxed()
        })?;
        cache.enable_tracing();
        cache.update_tracing_options(tracing_options);
        let meter = global::meter("chroma");
        let cache_hit = meter.u64_counter("cache_hit").build();
        let cache_miss = meter.u64_counter("cache_miss").build();
        let get_latency = meter.u64_histogram("get_latency").build();
        let obtain_latency = meter.u64_histogram("obtain_latency").build();
        let insert_latency = meter.u64_histogram("insert_latency").build();
        let remove_latency = meter.u64_histogram("remove_latency").build();
        let clear_latency = meter.u64_histogram("clear_latency").build();
        let hostname = std::env::var("HOSTNAME").unwrap_or("unknown".to_string());
        let hostname_kv = KeyValue::new("hostname", hostname);
        Ok(FoyerHybridCache {
            cache,
            cache_hit,
            cache_miss,
            get_latency,
            obtain_latency,
            insert_latency,
            remove_latency,
            clear_latency,
            hostname: hostname_kv,
        })
    }

    #[allow(dead_code)]
    fn insert_to_disk(&self, key: K, value: V) {
        self.cache.storage_writer(key).insert(value);
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerHybridCache<K, V>
where
    K: Clone + Send + Sync + StorageKey + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + StorageValue + Weighted + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.get_latency, hostname, StopWatchUnit::Millis);
        let res = self.cache.get(key).await?.map(|v| v.value().clone());
        if res.is_some() {
            self.cache_hit.add(1, hostname);
        } else {
            self.cache_miss.add(1, hostname);
        }
        Ok(res)
    }

    async fn insert(&self, key: K, value: V) {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.insert_latency, hostname, StopWatchUnit::Millis);
        self.cache.insert(key, value);
    }

    async fn remove(&self, key: &K) {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.remove_latency, hostname, StopWatchUnit::Millis);
        self.cache.remove(key);
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.clear_latency, hostname, StopWatchUnit::Millis);
        Ok(self.cache.clear().await?)
    }

    async fn obtain(&self, key: K) -> Result<Option<V>, CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.obtain_latency, hostname, StopWatchUnit::Millis);
        let res = self.cache.obtain(key).await?.map(|v| v.value().clone());
        if res.is_some() {
            self.cache_hit.add(1, hostname);
        } else {
            self.cache_miss.add(1, hostname);
        }
        Ok(res)
    }

    async fn may_contain(&self, key: &K) -> bool {
        self.cache.contains(key)
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
    cache_hit: opentelemetry::metrics::Counter<u64>,
    cache_miss: opentelemetry::metrics::Counter<u64>,
    get_latency: opentelemetry::metrics::Histogram<u64>,
    obtain_latency: opentelemetry::metrics::Histogram<u64>,
    insert_latency: opentelemetry::metrics::Histogram<u64>,
    remove_latency: opentelemetry::metrics::Histogram<u64>,
    clear_latency: opentelemetry::metrics::Histogram<u64>,
    hostname: KeyValue,
}

impl<K, V> Debug for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerPlainCache").finish()
    }
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
            .with_name(config.name.clone())
            .with_shards(config.shards)
            .with_weighter(|_: &_, v: &V| v.weight())
            .build();
        let meter = global::meter("chroma");
        let cache_hit = meter.u64_counter("cache_hit").build();
        let cache_miss = meter.u64_counter("cache_miss").build();
        let get_latency = meter.u64_histogram("get_latency").build();
        let obtain_latency = meter.u64_histogram("obtain_latency").build();
        let insert_latency = meter.u64_histogram("insert_latency").build();
        let remove_latency = meter.u64_histogram("remove_latency").build();
        let clear_latency = meter.u64_histogram("clear_latency").build();
        let hostname = std::env::var("HOSTNAME").unwrap_or("unknown".to_string());
        let hostname_kv = KeyValue::new("hostname", hostname);
        Ok(FoyerPlainCache {
            cache,
            cache_hit,
            cache_miss,
            get_latency,
            obtain_latency,
            insert_latency,
            remove_latency,
            clear_latency,
            hostname: hostname_kv,
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

            fn on_leave(&self, _: foyer::Event, key: &Self::Key, value: &Self::Value)
            where
                Self::Key: foyer::Key,
                Self::Value: foyer::Value,
            {
                // NOTE(rescrv):  There's no mechanism by which we can error.  We could log a
                // metric, but this should really never happen.
                let _ = self.0.send((key.clone(), value.clone()));
            }
        }
        let evl = TokioEventListener(tx);

        let cache = CacheBuilder::new(config.capacity)
            .with_name(config.name.clone())
            .with_shards(config.shards)
            .with_weighter(|_: &_, v: &V| v.weight())
            .with_event_listener(Arc::new(evl))
            .build();
        let meter = global::meter("chroma");
        let cache_hit = meter.u64_counter("cache_hit").build();
        let cache_miss = meter.u64_counter("cache_miss").build();
        let get_latency = meter.u64_histogram("get_latency").build();
        let obtain_latency = meter.u64_histogram("obtain_latency").build();
        let insert_latency = meter.u64_histogram("insert_latency").build();
        let remove_latency = meter.u64_histogram("remove_latency").build();
        let clear_latency = meter.u64_histogram("clear_latency").build();
        let hostname = std::env::var("HOSTNAME").unwrap_or("unknown".to_string());
        let hostname_kv = KeyValue::new("hostname", hostname);
        Ok(FoyerPlainCache {
            cache,
            cache_hit,
            cache_miss,
            get_latency,
            obtain_latency,
            insert_latency,
            remove_latency,
            clear_latency,
            hostname: hostname_kv,
        })
    }
}

#[async_trait::async_trait]
impl<K, V> super::Cache<K, V> for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + 'static,
    V: Clone + Send + Sync + Weighted + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.get_latency, hostname, StopWatchUnit::Millis);
        let res = self.cache.get(key).map(|v| v.value().clone());
        if res.is_some() {
            self.cache_hit.add(1, hostname);
        } else {
            self.cache_miss.add(1, hostname);
        }
        Ok(res)
    }

    async fn insert(&self, key: K, value: V) {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.insert_latency, hostname, StopWatchUnit::Millis);
        self.cache.insert(key, value);
    }

    async fn remove(&self, key: &K) {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.remove_latency, hostname, StopWatchUnit::Millis);
        self.cache.remove(key);
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.clear_latency, hostname, StopWatchUnit::Millis);
        self.cache.clear();
        Ok(())
    }

    async fn obtain(&self, key: K) -> Result<Option<V>, CacheError> {
        let hostname = &[self.hostname.clone()];
        let _stopwatch = Stopwatch::new(&self.obtain_latency, hostname, StopWatchUnit::Millis);
        let res = self.cache.get(&key).map(|v| v.value().clone());
        if res.is_some() {
            self.cache_hit.add(1, hostname);
        } else {
            self.cache_miss.add(1, hostname);
        }
        Ok(res)
    }
}

impl<K, V> super::PersistentCache<K, V> for FoyerPlainCache<K, V>
where
    K: Clone + Send + Sync + Eq + PartialEq + Hash + StorageKey + 'static,
    V: Clone + Send + Sync + Weighted + StorageValue + 'static,
{
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use tokio::{fs::File, sync::mpsc};

    use crate::Cache;

    use super::*;

    impl crate::Weighted for Arc<File> {
        fn weight(&self) -> usize {
            1
        }
    }

    impl crate::Weighted for String {
        fn weight(&self) -> usize {
            self.len()
        }
    }

    #[tokio::test]
    async fn test_foyer_memory_cache_can_close_file_descriptor() {
        let dir = tempfile::tempdir().expect("Should be able to create temp path");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let fd_pool = FoyerCacheConfig {
            capacity: 54,
            ..Default::default()
        }
        .build_memory_with_event_listener::<PathBuf, Arc<File>>(tx)
        .await
        .expect("Should be able to build in memory cache");

        tokio::spawn(async move { while rx.recv().await.is_some() {} });

        for i in 0..10000 {
            let path = dir.path().join(i.to_string());
            let file = Arc::new(
                File::create(path.as_path())
                    .await
                    .expect("Should be able to create new file descriptor"),
            );
            fd_pool.insert(path, file).await;
        }
    }

    #[tokio::test]
    async fn test_foyer_hybrid_cache_can_recover() {
        let dir = tempfile::tempdir()
            .expect("To be able to create temp path")
            .path()
            .to_str()
            .expect("To be able to parse path")
            .to_string();
        let cache = FoyerCacheConfig {
            dir: Some(dir.clone()),
            ..Default::default()
        }
        .build_hybrid::<String, String>()
        .await
        .unwrap();

        cache.insert("key1".to_string(), "value1".to_string()).await;

        // Wait for flush to disk
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        drop(cache);

        // Test that we can recover the cache from disk.
        let cache2 = FoyerCacheConfig {
            dir: Some(dir.clone()),
            ..FoyerCacheConfig::default()
        }
        .build_hybrid::<String, String>()
        .await
        .unwrap();

        assert_eq!(
            cache2.get(&"key1".to_string()).await.unwrap(),
            Some("value1".to_string())
        );

        // Deterministic hashing off should not be able to recover the cache.
        let cache3 = FoyerCacheConfig {
            dir: Some(dir.clone()),
            deterministic_hashing: false,
            ..FoyerCacheConfig::default()
        }
        .build_hybrid::<String, String>()
        .await
        .unwrap();

        assert_eq!(cache3.get(&"key1".to_string()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_writing_only_to_disk_works() {
        let dir = tempfile::tempdir()
            .expect("To be able to create temp path")
            .path()
            .to_str()
            .expect("To be able to parse path")
            .to_string();
        let cache = FoyerCacheConfig {
            dir: Some(dir.clone()),
            flush: true,
            ..Default::default()
        }
        .build_hybrid_test::<String, String>()
        .await
        .unwrap();
        // Insert a 512KB string value generated at random by passing memory.
        let large_value = "val1".repeat(512 * 1024);
        cache.insert_to_disk("key1".to_string(), large_value.clone());
        // Sleep for 2 secs.
        // This should give the cache enough time to flush the data to disk.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let value = cache
            .get(&"key1".to_string())
            .await
            .expect("Expected to be able to get value")
            .expect("Value should not be None");
        assert_eq!(value, large_value);
    }

    #[tokio::test]
    async fn test_inserted_key_immediately_available() {
        let dir = tempfile::tempdir()
            .expect("To be able to create temp path")
            .path()
            .to_str()
            .expect("To be able to parse path")
            .to_string();
        let cache = FoyerCacheConfig {
            dir: Some(dir.clone()),
            flush: true,
            ..Default::default()
        }
        .build_hybrid_test::<String, String>()
        .await
        .unwrap();

        cache.insert("key1".to_string(), "foo".to_string()).await;

        let value = cache
            .get(&"key1".to_string())
            .await
            .expect("Expected to be able to get value")
            .expect("Value should not be None");
        assert_eq!(value, "foo");
    }

    #[tokio::test]
    async fn test_may_contain() {
        let dir = tempfile::tempdir()
            .expect("To be able to create temp path")
            .path()
            .to_str()
            .expect("To be able to parse path")
            .to_string();
        let cache = FoyerCacheConfig {
            dir: Some(dir.clone()),
            flush: true,
            ..Default::default()
        }
        .build_hybrid_test::<String, String>()
        .await
        .unwrap();

        cache.insert("key1".to_string(), "foo".to_string()).await;
        assert!(cache.may_contain(&"key1".to_string()).await);
        assert!(!cache.may_contain(&"key2".to_string()).await);
    }
}
