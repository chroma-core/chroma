// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{borrow::Cow, fmt::Debug, future::Future, hash::Hash, ops::Deref, sync::Arc};

use equivalent::Equivalent;
use foyer_common::{
    code::{DefaultHasher, HashBuilder, Key, Value},
    event::EventListener,
    future::Diversion,
    metrics::Metrics,
    properties::{Hint, Location, Properties, Source},
    runtime::SingletonHandle,
};
use mixtrics::{metrics::BoxedRegistry, registry::noop::NoopMetricsRegistry};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    eviction::{
        fifo::{Fifo, FifoConfig},
        lfu::{Lfu, LfuConfig},
        lru::{Lru, LruConfig},
        s3fifo::{S3Fifo, S3FifoConfig},
    },
    raw::{FetchContext, FetchState, RawCache, RawCacheConfig, RawCacheEntry, RawFetch, Weighter},
    Piece, Pipe, Result,
};

/// Entry properties for in-memory only cache.
#[derive(Debug, Clone, Default)]
pub struct CacheProperties {
    ephemeral: bool,
    hint: Hint,
}

impl CacheProperties {
    /// Set the entry to be ephemeral.
    ///
    /// An ephemeral entry will be evicted immediately after all its holders drop it,
    /// no matter if the capacity is reached.
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = ephemeral;
        self
    }

    /// Get if the entry is ephemeral.
    pub fn ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Set entry hint.
    pub fn with_hint(mut self, hint: Hint) -> Self {
        self.hint = hint;
        self
    }

    /// Get entry hint.
    pub fn hint(&self) -> Hint {
        self.hint
    }
}

impl Properties for CacheProperties {
    fn with_ephemeral(self, ephemeral: bool) -> Self {
        self.with_ephemeral(ephemeral)
    }

    fn ephemeral(&self) -> Option<bool> {
        Some(self.ephemeral())
    }

    fn with_hint(self, hint: Hint) -> Self {
        self.with_hint(hint)
    }

    fn hint(&self) -> Option<Hint> {
        Some(self.hint())
    }

    fn with_location(self, _: Location) -> Self {
        self
    }

    fn location(&self) -> Option<Location> {
        None
    }

    fn with_source(self, _: Source) -> Self {
        self
    }

    fn source(&self) -> Option<Source> {
        None
    }
}

pub type FifoCache<K, V, S = DefaultHasher, P = CacheProperties> = RawCache<Fifo<K, V, P>, S>;
pub type FifoCacheEntry<K, V, S = DefaultHasher, P = CacheProperties> = RawCacheEntry<Fifo<K, V, P>, S>;
pub type FifoFetch<K, V, ER, S = DefaultHasher, P = CacheProperties> = RawFetch<Fifo<K, V, P>, ER, S>;

pub type S3FifoCache<K, V, S = DefaultHasher, P = CacheProperties> = RawCache<S3Fifo<K, V, P>, S>;
pub type S3FifoCacheEntry<K, V, S = DefaultHasher, P = CacheProperties> = RawCacheEntry<S3Fifo<K, V, P>, S>;
pub type S3FifoFetch<K, V, ER, S = DefaultHasher, P = CacheProperties> = RawFetch<S3Fifo<K, V, P>, ER, S>;

pub type LruCache<K, V, S = DefaultHasher, P = CacheProperties> = RawCache<Lru<K, V, P>, S>;
pub type LruCacheEntry<K, V, S = DefaultHasher, P = CacheProperties> = RawCacheEntry<Lru<K, V, P>, S>;
pub type LruFetch<K, V, ER, S = DefaultHasher, P = CacheProperties> = RawFetch<Lru<K, V, P>, ER, S>;

pub type LfuCache<K, V, S = DefaultHasher, P = CacheProperties> = RawCache<Lfu<K, V, P>, S>;
pub type LfuCacheEntry<K, V, S = DefaultHasher, P = CacheProperties> = RawCacheEntry<Lfu<K, V, P>, S>;
pub type LfuFetch<K, V, ER, S = DefaultHasher, P = CacheProperties> = RawFetch<Lfu<K, V, P>, ER, S>;

/// A cached entry holder of the in-memory cache.
#[derive(Debug)]
pub enum CacheEntry<K, V, S = DefaultHasher, P = CacheProperties>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// A cached entry holder of the in-memory FIFO cache.
    Fifo(FifoCacheEntry<K, V, S, P>),
    /// A cached entry holder of the in-memory S3FIFO cache.
    S3Fifo(S3FifoCacheEntry<K, V, S, P>),
    /// A cached entry holder of the in-memory LRU cache.
    Lru(LruCacheEntry<K, V, S, P>),
    /// A cached entry holder of the in-memory LFU cache.
    Lfu(LfuCacheEntry<K, V, S, P>),
}

impl<K, V, S, P> Clone for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(entry) => Self::Fifo(entry.clone()),
            Self::Lru(entry) => Self::Lru(entry.clone()),
            Self::Lfu(entry) => Self::Lfu(entry.clone()),
            Self::S3Fifo(entry) => Self::S3Fifo(entry.clone()),
        }
    }
}

impl<K, V, S, P> Deref for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match self {
            CacheEntry::Fifo(entry) => entry.deref(),
            CacheEntry::Lru(entry) => entry.deref(),
            CacheEntry::Lfu(entry) => entry.deref(),
            CacheEntry::S3Fifo(entry) => entry.deref(),
        }
    }
}

impl<K, V, S, P> From<FifoCacheEntry<K, V, S, P>> for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: FifoCacheEntry<K, V, S, P>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, S, P> From<LruCacheEntry<K, V, S, P>> for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: LruCacheEntry<K, V, S, P>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, S, P> From<LfuCacheEntry<K, V, S, P>> for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: LfuCacheEntry<K, V, S, P>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, S, P> From<S3FifoCacheEntry<K, V, S, P>> for CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: S3FifoCacheEntry<K, V, S, P>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, S, P> CacheEntry<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// Key hash of the cached entry.
    pub fn hash(&self) -> u64 {
        match self {
            CacheEntry::Fifo(entry) => entry.hash(),
            CacheEntry::Lru(entry) => entry.hash(),
            CacheEntry::Lfu(entry) => entry.hash(),
            CacheEntry::S3Fifo(entry) => entry.hash(),
        }
    }

    /// Key of the cached entry.
    pub fn key(&self) -> &K {
        match self {
            CacheEntry::Fifo(entry) => entry.key(),
            CacheEntry::Lru(entry) => entry.key(),
            CacheEntry::Lfu(entry) => entry.key(),
            CacheEntry::S3Fifo(entry) => entry.key(),
        }
    }

    /// Value of the cached entry.
    pub fn value(&self) -> &V {
        match self {
            CacheEntry::Fifo(entry) => entry.value(),
            CacheEntry::Lru(entry) => entry.value(),
            CacheEntry::Lfu(entry) => entry.value(),
            CacheEntry::S3Fifo(entry) => entry.value(),
        }
    }

    /// Properties of the cached entry.
    pub fn properties(&self) -> &P {
        match self {
            CacheEntry::Fifo(entry) => entry.properties(),
            CacheEntry::Lru(entry) => entry.properties(),
            CacheEntry::Lfu(entry) => entry.properties(),
            CacheEntry::S3Fifo(entry) => entry.properties(),
        }
    }

    /// Weight of the cached entry.
    pub fn weight(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.weight(),
            CacheEntry::Lru(entry) => entry.weight(),
            CacheEntry::Lfu(entry) => entry.weight(),
            CacheEntry::S3Fifo(entry) => entry.weight(),
        }
    }

    /// External reference count of the cached entry.
    pub fn refs(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.refs(),
            CacheEntry::Lru(entry) => entry.refs(),
            CacheEntry::Lfu(entry) => entry.refs(),
            CacheEntry::S3Fifo(entry) => entry.refs(),
        }
    }

    /// If the cached entry is updated and outdated.
    pub fn is_outdated(&self) -> bool {
        match self {
            CacheEntry::Fifo(entry) => entry.is_outdated(),
            CacheEntry::Lru(entry) => entry.is_outdated(),
            CacheEntry::Lfu(entry) => entry.is_outdated(),
            CacheEntry::S3Fifo(entry) => entry.is_outdated(),
        }
    }

    /// Get the piece of the entry record.
    pub fn piece(&self) -> Piece<K, V, P> {
        match self {
            CacheEntry::Fifo(entry) => entry.piece(),
            CacheEntry::Lru(entry) => entry.piece(),
            CacheEntry::Lfu(entry) => entry.piece(),
            CacheEntry::S3Fifo(entry) => entry.piece(),
        }
    }
}

/// Eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionConfig {
    /// FIFO eviction algorithm config.
    Fifo(FifoConfig),
    /// S3FIFO eviction algorithm config.
    S3Fifo(S3FifoConfig),
    /// LRU eviction algorithm config.
    Lru(LruConfig),
    /// LFU eviction algorithm config.
    Lfu(LfuConfig),
}

impl From<FifoConfig> for EvictionConfig {
    fn from(value: FifoConfig) -> EvictionConfig {
        EvictionConfig::Fifo(value)
    }
}

impl From<S3FifoConfig> for EvictionConfig {
    fn from(value: S3FifoConfig) -> EvictionConfig {
        EvictionConfig::S3Fifo(value)
    }
}

impl From<LruConfig> for EvictionConfig {
    fn from(value: LruConfig) -> EvictionConfig {
        EvictionConfig::Lru(value)
    }
}

impl From<LfuConfig> for EvictionConfig {
    fn from(value: LfuConfig) -> EvictionConfig {
        EvictionConfig::Lfu(value)
    }
}

/// In-memory cache builder.
pub struct CacheBuilder<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    name: Cow<'static, str>,

    capacity: usize,
    shards: usize,
    eviction_config: EvictionConfig,

    hash_builder: S,
    weighter: Arc<dyn Weighter<K, V>>,

    event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,

    registry: BoxedRegistry,
    metrics: Option<Arc<Metrics>>,
}

impl<K, V> CacheBuilder<K, V, DefaultHasher>
where
    K: Key,
    V: Value,
{
    /// Create a new in-memory cache builder.
    pub fn new(capacity: usize) -> Self {
        Self {
            name: "foyer".into(),

            capacity,
            shards: 8,
            eviction_config: LruConfig::default().into(),

            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,

            registry: Box::new(NoopMetricsRegistry),
            metrics: None,
        }
    }
}

impl<K, V, S> CacheBuilder<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// Set the name of the foyer in-memory cache instance.
    ///
    /// foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.name = name.into();
        self
    }

    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(mut self, eviction_config: impl Into<EvictionConfig>) -> Self {
        self.eviction_config = eviction_config.into();
        self
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> CacheBuilder<K, V, OS>
    where
        OS: HashBuilder,
    {
        CacheBuilder {
            name: self.name,
            capacity: self.capacity,
            shards: self.shards,
            eviction_config: self.eviction_config,
            hash_builder,
            weighter: self.weighter,
            event_listener: self.event_listener,
            registry: self.registry,
            metrics: self.metrics,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(mut self, weighter: impl Weighter<K, V>) -> Self {
        self.weighter = Arc::new(weighter);
        self
    }

    /// Set event listener.
    pub fn with_event_listener(mut self, event_listener: Arc<dyn EventListener<Key = K, Value = V>>) -> Self {
        self.event_listener = Some(event_listener);
        self
    }

    /// Set metrics registry.
    ///
    /// Default: [`NoopMetricsRegistry`].
    pub fn with_metrics_registry(mut self, registry: BoxedRegistry) -> CacheBuilder<K, V, S> {
        self.registry = registry;
        self
    }

    /// Set metrics.
    ///
    /// Note: `with_metrics` is only supposed to be called by other foyer components.
    #[doc(hidden)]
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build in-memory cache with the given configuration.
    pub fn build<P>(self) -> Cache<K, V, S, P>
    where
        P: Properties,
    {
        if self.capacity < self.shards {
            tracing::warn!(
                "The in-memory cache capacity({}) < shards({}).",
                self.capacity,
                self.shards
            );
        }

        let metrics = self
            .metrics
            .unwrap_or_else(|| Arc::new(Metrics::new(self.name, &self.registry)));

        match self.eviction_config {
            EvictionConfig::Fifo(eviction_config) => Cache::Fifo(Arc::new(RawCache::new(RawCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
                metrics,
            }))),
            EvictionConfig::S3Fifo(eviction_config) => Cache::S3Fifo(Arc::new(RawCache::new(RawCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
                metrics,
            }))),
            EvictionConfig::Lru(eviction_config) => Cache::Lru(Arc::new(RawCache::new(RawCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
                metrics,
            }))),
            EvictionConfig::Lfu(eviction_config) => Cache::Lfu(Arc::new(RawCache::new(RawCacheConfig {
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
                metrics,
            }))),
        }
    }
}

/// In-memory cache with plug-and-play algorithms.
pub enum Cache<K, V, S = DefaultHasher, P = CacheProperties>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// In-memory FIFO cache.
    Fifo(Arc<FifoCache<K, V, S, P>>),
    /// In-memory LRU cache.
    Lru(Arc<LruCache<K, V, S, P>>),
    /// In-memory LFU cache.
    Lfu(Arc<LfuCache<K, V, S, P>>),
    /// In-memory S3FIFO cache.
    S3Fifo(Arc<S3FifoCache<K, V, S, P>>),
}

impl<K, V, S, P> Debug for Cache<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fifo(_) => f.debug_tuple("Cache::FifoCache").finish(),
            Self::S3Fifo(_) => f.debug_tuple("Cache::S3FifoCache").finish(),
            Self::Lru(_) => f.debug_tuple("Cache::LruCache").finish(),
            Self::Lfu(_) => f.debug_tuple("Cache::LfuCache").finish(),
        }
    }
}

impl<K, V, S, P> Clone for Cache<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(cache) => Self::Fifo(cache.clone()),
            Self::S3Fifo(cache) => Self::S3Fifo(cache.clone()),
            Self::Lru(cache) => Self::Lru(cache.clone()),
            Self::Lfu(cache) => Self::Lfu(cache.clone()),
        }
    }
}

impl<K, V> Cache<K, V, DefaultHasher, CacheProperties>
where
    K: Key,
    V: Value,
{
    /// Create a new in-memory cache builder with capacity.
    pub fn builder(capacity: usize) -> CacheBuilder<K, V, DefaultHasher> {
        CacheBuilder::new(capacity)
    }
}

impl<K, V, S, P> Cache<K, V, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// Update capacity and evict overflowed entries.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::resize"))]
    pub fn resize(&self, capacity: usize) -> Result<()> {
        match self {
            Cache::Fifo(cache) => cache.resize(capacity),
            Cache::S3Fifo(cache) => cache.resize(capacity),
            Cache::Lru(cache) => cache.resize(capacity),
            Cache::Lfu(cache) => cache.resize(capacity),
        }
    }

    /// Insert cache entry to the in-memory cache.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::insert"))]
    pub fn insert(&self, key: K, value: V) -> CacheEntry<K, V, S, P> {
        match self {
            Cache::Fifo(cache) => cache.insert(key, value).into(),
            Cache::S3Fifo(cache) => cache.insert(key, value).into(),
            Cache::Lru(cache) => cache.insert(key, value).into(),
            Cache::Lfu(cache) => cache.insert(key, value).into(),
        }
    }

    /// Insert cache entry to the in-memory cache with properties.
    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::cache::insert_with_properties")
    )]
    pub fn insert_with_properties(&self, key: K, value: V, properties: P) -> CacheEntry<K, V, S, P> {
        match self {
            Cache::Fifo(cache) => cache.insert_with_properties(key, value, properties).into(),
            Cache::S3Fifo(cache) => cache.insert_with_properties(key, value, properties).into(),
            Cache::Lru(cache) => cache.insert_with_properties(key, value, properties).into(),
            Cache::Lfu(cache) => cache.insert_with_properties(key, value, properties).into(),
        }
    }

    /// Remove a cached entry with the given key from the in-memory cache.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::remove"))]
    pub fn remove<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, S, P>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.remove(key).map(CacheEntry::from),
        }
    }

    /// Get cached entry with the given key from the in-memory cache.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::get"))]
    pub fn get<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, S, P>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.get(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.get(key).map(CacheEntry::from),
        }
    }

    /// Check if the in-memory cache contains a cached entry with the given key.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::contains"))]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.contains(key),
            Cache::S3Fifo(cache) => cache.contains(key),
            Cache::Lru(cache) => cache.contains(key),
            Cache::Lfu(cache) => cache.contains(key),
        }
    }

    /// Access the cached entry with the given key but don't return.
    ///
    /// Note: This method can be used to update the cache eviction information and order based on the algorithm.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::touch"))]
    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.touch(key),
            Cache::S3Fifo(cache) => cache.touch(key),
            Cache::Lru(cache) => cache.touch(key),
            Cache::Lfu(cache) => cache.touch(key),
        }
    }

    /// Clear the in-memory cache.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::clear"))]
    pub fn clear(&self) {
        match self {
            Cache::Fifo(cache) => cache.clear(),
            Cache::S3Fifo(cache) => cache.clear(),
            Cache::Lru(cache) => cache.clear(),
            Cache::Lfu(cache) => cache.clear(),
        }
    }

    /// Get the capacity of the in-memory cache.
    pub fn capacity(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.capacity(),
            Cache::S3Fifo(cache) => cache.capacity(),
            Cache::Lru(cache) => cache.capacity(),
            Cache::Lfu(cache) => cache.capacity(),
        }
    }

    /// Get the usage of the in-memory cache.
    pub fn usage(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.usage(),
            Cache::S3Fifo(cache) => cache.usage(),
            Cache::Lru(cache) => cache.usage(),
            Cache::Lfu(cache) => cache.usage(),
        }
    }

    /// Hash the given key with the hash builder of the cache.
    pub fn hash<Q>(&self, key: &Q) -> u64
    where
        Q: Hash + ?Sized,
    {
        self.hash_builder().hash_one(key)
    }

    /// Get the hash builder of the in-memory cache.
    pub fn hash_builder(&self) -> &Arc<S> {
        match self {
            Cache::Fifo(cache) => cache.hash_builder(),
            Cache::S3Fifo(cache) => cache.hash_builder(),
            Cache::Lru(cache) => cache.hash_builder(),
            Cache::Lfu(cache) => cache.hash_builder(),
        }
    }

    /// Get the shards of the in-memory cache.
    pub fn shards(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.shards(),
            Cache::S3Fifo(cache) => cache.shards(),
            Cache::Lru(cache) => cache.shards(),
            Cache::Lfu(cache) => cache.shards(),
        }
    }

    /// Set the pipe for the hybrid cache.
    #[doc(hidden)]
    pub fn set_pipe(&self, pipe: Box<dyn Pipe<Key = K, Value = V, Properties = P>>) {
        match self {
            Cache::Fifo(cache) => cache.set_pipe(pipe),
            Cache::S3Fifo(cache) => cache.set_pipe(pipe),
            Cache::Lru(cache) => cache.set_pipe(pipe),
            Cache::Lfu(cache) => cache.set_pipe(pipe),
        }
    }

    /// Evict all entries from the in-memory cache.
    ///
    /// Instead of [`Cache::clear`], [`Cache::evict_all`] will send the evicted pipe to the pipe.
    /// It is useful when the cache is used as a hybrid cache.
    pub fn evict_all(&self) {
        match self {
            Cache::Fifo(cache) => cache.evict_all(),
            Cache::S3Fifo(cache) => cache.evict_all(),
            Cache::Lru(cache) => cache.evict_all(),
            Cache::Lfu(cache) => cache.evict_all(),
        }
    }

    /// Evict all entries in the cache and offload them into the disk cache via the pipe if needed.
    ///
    /// This function obeys the io throttler of the disk cache and make sure all entries will be offloaded.
    /// Therefore, this function is asynchronous.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::offload"))]
    pub async fn flush(&self) {
        match self {
            Cache::Fifo(cache) => cache.flush().await,
            Cache::S3Fifo(cache) => cache.flush().await,
            Cache::Lru(cache) => cache.flush().await,
            Cache::Lfu(cache) => cache.flush().await,
        }
    }
}

/// A future that is used to get entry value from the remote storage for the in-memory cache.
#[pin_project(project = FetchProj)]
pub enum Fetch<K, V, ER, S = DefaultHasher, P = CacheProperties>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// A future that is used to get entry value from the remote storage for the in-memory FIFO cache.
    Fifo(#[pin] FifoFetch<K, V, ER, S, P>),
    /// A future that is used to get entry value from the remote storage for the in-memory S3FIFO cache.
    S3Fifo(#[pin] S3FifoFetch<K, V, ER, S, P>),
    /// A future that is used to get entry value from the remote storage for the in-memory LRU cache.
    Lru(#[pin] LruFetch<K, V, ER, S, P>),
    /// A future that is used to get entry value from the remote storage for the in-memory LFU cache.
    Lfu(#[pin] LfuFetch<K, V, ER, S, P>),
}

impl<K, V, ER, S, P> From<FifoFetch<K, V, ER, S, P>> for Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: FifoFetch<K, V, ER, S, P>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, ER, S, P> From<S3FifoFetch<K, V, ER, S, P>> for Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: S3FifoFetch<K, V, ER, S, P>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, ER, S, P> From<LruFetch<K, V, ER, S, P>> for Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: LruFetch<K, V, ER, S, P>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, ER, S, P> From<LfuFetch<K, V, ER, S, P>> for Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    fn from(entry: LfuFetch<K, V, ER, S, P>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, ER, S, P> Future for Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    ER: From<oneshot::error::RecvError>,
    S: HashBuilder,
    P: Properties,
{
    type Output = std::result::Result<CacheEntry<K, V, S, P>, ER>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match self.project() {
            FetchProj::Fifo(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::S3Fifo(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::Lru(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::Lfu(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
        }
    }
}

impl<K, V, ER, S, P> Fetch<K, V, ER, S, P>
where
    K: Key,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// Get the fetch state.
    pub fn state(&self) -> FetchState {
        match self {
            Fetch::Fifo(fetch) => fetch.state(),
            Fetch::S3Fifo(fetch) => fetch.state(),
            Fetch::Lru(fetch) => fetch.state(),
            Fetch::Lfu(fetch) => fetch.state(),
        }
    }

    /// Get the ext of the fetch.
    #[doc(hidden)]
    pub fn store(&self) -> &Option<FetchContext> {
        match self {
            Fetch::Fifo(fetch) => fetch.store(),
            Fetch::S3Fifo(fetch) => fetch.store(),
            Fetch::Lru(fetch) => fetch.store(),
            Fetch::Lfu(fetch) => fetch.store(),
        }
    }
}

impl<K, V, S, P> Cache<K, V, S, P>
where
    K: Key + Clone,
    V: Value,
    S: HashBuilder,
    P: Properties,
{
    /// Get the cached entry with the given key from the in-memory cache.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::fetch"))]
    pub fn fetch<F, FU, ER>(&self, key: K, fetch: F) -> Fetch<K, V, ER, S, P>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<V, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::Lru(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch(key, fetch)),
        }
    }

    /// Get the cached entry with the given key and properties from the in-memory cache.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::cache::fetch"))]
    pub fn fetch_with_properties<F, FU, ER>(&self, key: K, properties: P, fetch: F) -> Fetch<K, V, ER, S, P>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<V, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch_with_properties(key, properties, fetch)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch_with_properties(key, properties, fetch)),
            Cache::Lru(cache) => Fetch::from(cache.fetch_with_properties(key, properties, fetch)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch_with_properties(key, properties, fetch)),
        }
    }

    /// Get the cached entry with the given key from the in-memory cache.
    ///
    /// The fetch task will be spawned in the give `runtime`.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[doc(hidden)]
    pub fn fetch_inner<F, FU, ER, ID>(
        &self,
        key: K,
        properties: P,
        fetch: F,
        runtime: &SingletonHandle,
    ) -> Fetch<K, V, ER, S, P>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = ID> + Send + 'static,
        ER: Send + 'static + Debug,
        ID: Into<Diversion<std::result::Result<V, ER>, FetchContext>>,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch_inner(key, properties, fetch, runtime)),
            Cache::Lru(cache) => Fetch::from(cache.fetch_inner(key, properties, fetch, runtime)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch_inner(key, properties, fetch, runtime)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch_inner(key, properties, fetch, runtime)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, time::Duration};

    use futures_util::future::join_all;
    use itertools::Itertools;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use super::*;
    use crate::eviction::{fifo::FifoConfig, lfu::LfuConfig, lru::LruConfig, s3fifo::S3FifoConfig};

    const CAPACITY: usize = 100;
    const SHARDS: usize = 4;
    const RANGE: Range<u64> = 0..1000;
    const OPS: usize = 10000;
    const CONCURRENCY: usize = 8;

    fn fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(FifoConfig {})
            .build()
    }

    fn lru() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.1,
            })
            .build()
    }

    fn lfu() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            })
            .build()
    }

    fn s3fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(S3FifoConfig {
                small_queue_capacity_ratio: 0.1,
                ghost_queue_capacity_ratio: 10.0,
                small_to_main_freq_threshold: 2,
            })
            .build()
    }

    fn init_cache(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let mut v = RANGE.collect_vec();
        v.shuffle(rng);
        for i in v {
            cache.insert(i, i);
        }
    }

    async fn operate(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let i = rng.random_range(RANGE);
        match rng.random_range(0..=3) {
            0 => {
                let entry = cache.insert(i, i);
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            1 => {
                if let Some(entry) = cache.get(&i) {
                    assert_eq!(*entry.key(), i);
                    assert_eq!(entry.key(), entry.value());
                }
            }
            2 => {
                cache.remove(&i);
            }
            3 => {
                let entry = cache
                    .fetch(i, || async move {
                        tokio::time::sleep(Duration::from_micros(10)).await;
                        Ok::<_, tokio::sync::oneshot::error::RecvError>(i)
                    })
                    .await
                    .unwrap();
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            _ => unreachable!(),
        }
    }

    async fn case(cache: Cache<u64, u64>) {
        let mut rng = StdRng::seed_from_u64(42);

        init_cache(&cache, &mut rng);

        let handles = (0..CONCURRENCY)
            .map(|_| {
                let cache = cache.clone();
                let mut rng = rng.clone();
                tokio::spawn(async move {
                    for _ in 0..OPS {
                        operate(&cache, &mut rng).await;
                    }
                })
            })
            .collect_vec();

        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_fifo_cache() {
        case(fifo()).await
    }

    #[tokio::test]
    async fn test_lru_cache() {
        case(lru()).await
    }

    #[tokio::test]
    async fn test_lfu_cache() {
        case(lfu()).await
    }

    #[tokio::test]
    async fn test_s3fifo_cache() {
        case(s3fifo()).await
    }
}
