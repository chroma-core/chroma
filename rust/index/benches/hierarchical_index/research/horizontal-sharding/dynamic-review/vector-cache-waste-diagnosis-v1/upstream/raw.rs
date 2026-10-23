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

use std::{
    collections::hash_map::{Entry as HashMapEntry, HashMap},
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use equivalent::Equivalent;
#[cfg(feature = "tracing")]
use fastrace::{
    future::{FutureExt, InSpan},
    Span,
};
use foyer_common::{
    code::HashBuilder,
    event::{Event, EventListener},
    future::{Diversion, DiversionFuture},
    metrics::Metrics,
    properties::{Location, Properties, Source},
    runtime::SingletonHandle,
    strict_assert,
    utils::scope::Scope,
};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use pin_project::pin_project;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    error::{Error, Result},
    eviction::{Eviction, Op},
    indexer::{hash_table::HashTableIndexer, sentry::Sentry, Indexer},
    pipe::NoopPipe,
    record::{Data, Record},
    Piece, Pipe,
};

/// The weighter for the in-memory cache.
///
/// The weighter is used to calculate the weight of the cache entry.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

pub struct RawCacheConfig<E, S>
where
    E: Eviction,
    S: HashBuilder,
{
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub hash_builder: S,
    pub weighter: Arc<dyn Weighter<E::Key, E::Value>>,
    pub event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
    pub metrics: Arc<Metrics>,
}

struct RawCacheShard<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    eviction: E,
    indexer: Sentry<I>,

    usage: usize,
    capacity: usize,

    #[expect(clippy::type_complexity)]
    waiters: Mutex<HashMap<E::Key, Vec<oneshot::Sender<RawCacheEntry<E, S, I>>>>>,

    metrics: Arc<Metrics>,
    _event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
}

impl<E, S, I> RawCacheShard<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    /// Evict entries to fit the target usage.
    fn evict(&mut self, target: usize, garbages: &mut Vec<(Event, Arc<Record<E>>)>) {
        // Evict overflow records.
        while self.usage > target {
            let evicted = match self.eviction.pop() {
                Some(evicted) => evicted,
                None => break,
            };
            self.metrics.memory_evict.increase(1);

            let e = self.indexer.remove(evicted.hash(), evicted.key()).unwrap();
            assert_eq!(Arc::as_ptr(&evicted), Arc::as_ptr(&e));

            strict_assert!(!evicted.as_ref().is_in_indexer());
            strict_assert!(!evicted.as_ref().is_in_eviction());

            self.usage -= evicted.weight();

            garbages.push((Event::Evict, evicted));
        }
    }

    fn emplace(
        &mut self,
        data: Data<E>,
        garbages: &mut Vec<(Event, Arc<Record<E>>)>,
        waiters: &mut Vec<oneshot::Sender<RawCacheEntry<E, S, I>>>,
    ) -> Arc<Record<E>> {
        std::mem::swap(waiters, &mut self.waiters.lock().remove(&data.key).unwrap_or_default());

        let weight = data.weight;
        let old_usage = self.usage;

        let record = Arc::new(Record::new(data));

        // Evict overflow records.
        self.evict(self.capacity.saturating_sub(weight), garbages);

        // Insert new record
        if let Some(old) = self.indexer.insert(record.clone()) {
            self.metrics.memory_replace.increase(1);

            strict_assert!(!old.is_in_indexer());

            if old.is_in_eviction() {
                self.eviction.remove(&old);
            }
            strict_assert!(!old.is_in_eviction());

            self.usage -= old.weight();

            garbages.push((Event::Replace, old));
        } else {
            self.metrics.memory_insert.increase(1);
        }
        strict_assert!(record.is_in_indexer());

        let ephemeral = record.properties().ephemeral().unwrap_or_default();
        record.set_ephemeral(ephemeral);
        if !ephemeral {
            self.eviction.push(record.clone());
            strict_assert!(record.is_in_eviction());
        }

        self.usage += weight;
        // Increase the reference count within the lock section.
        // The reference count of the new record must be at the moment.
        let refs = waiters.len() + 1;
        let inc = record.inc_refs(refs);
        assert_eq!(refs, inc);

        match self.usage.cmp(&old_usage) {
            std::cmp::Ordering::Greater => self.metrics.memory_usage.increase((self.usage - old_usage) as _),
            std::cmp::Ordering::Less => self.metrics.memory_usage.decrease((old_usage - self.usage) as _),
            std::cmp::Ordering::Equal => {}
        }

        record
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::shard::remove"))]
    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let record = self.indexer.remove(hash, key)?;

        if record.is_in_eviction() {
            self.eviction.remove(&record);
        }
        strict_assert!(!record.is_in_indexer());
        strict_assert!(!record.is_in_eviction());

        self.usage -= record.weight();

        self.metrics.memory_remove.increase(1);
        self.metrics.memory_usage.decrease(record.weight() as _);

        record.inc_refs(1);

        Some(record)
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::shard::get_noop"))]
    fn get_noop<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key)
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::get_immutable")
    )]
    fn get_immutable<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key)
            .inspect(|record| self.acquire_immutable(record))
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::get_mutable")
    )]
    fn get_mutable<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        self.get_inner(hash, key).inspect(|record| self.acquire_mutable(record))
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::shard::get_inner"))]
    fn get_inner<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Record<E>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let record = match self.indexer.get(hash, key).cloned() {
            Some(record) => {
                self.metrics.memory_hit.increase(1);
                record
            }
            None => {
                self.metrics.memory_miss.increase(1);
                return None;
            }
        };

        strict_assert!(record.is_in_indexer());

        record.set_ephemeral(false);

        record.inc_refs(1);

        Some(record)
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::shard::clear"))]
    fn clear(&mut self, garbages: &mut Vec<Arc<Record<E>>>) {
        let records = self.indexer.drain().collect_vec();
        self.eviction.clear();

        let mut count = 0;

        for record in records {
            count += 1;
            strict_assert!(!record.is_in_indexer());
            strict_assert!(!record.is_in_eviction());

            garbages.push(record);
        }

        self.metrics.memory_remove.increase(count);
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::acquire_immutable")
    )]
    fn acquire_immutable(&self, record: &Arc<Record<E>>) {
        match E::acquire() {
            Op::Immutable(f) => f(&self.eviction, record),
            _ => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::acquire_mutable")
    )]
    fn acquire_mutable(&mut self, record: &Arc<Record<E>>) {
        match E::acquire() {
            Op::Mutable(mut f) => f(&mut self.eviction, record),
            _ => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::release_immutable")
    )]
    fn release_immutable(&self, record: &Arc<Record<E>>) {
        match E::release() {
            Op::Immutable(f) => f(&self.eviction, record),
            _ => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::release_mutable")
    )]
    fn release_mutable(&mut self, record: &Arc<Record<E>>) {
        match E::release() {
            Op::Mutable(mut f) => f(&mut self.eviction, record),
            _ => unreachable!(),
        }
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::shard::fetch_noop"))]
    fn fetch_noop(&self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_noop(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::fetch_immutable")
    )]
    fn fetch_immutable(&self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_immutable(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::fetch_mutable")
    )]
    fn fetch_mutable(&mut self, hash: u64, key: &E::Key) -> RawShardFetch<E, S, I>
    where
        E::Key: Clone,
    {
        if let Some(record) = self.get_mutable(hash, key) {
            return RawShardFetch::Hit(record);
        }

        self.fetch_queue(key.clone())
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::shard::fetch_queue")
    )]
    fn fetch_queue(&self, key: E::Key) -> RawShardFetch<E, S, I> {
        match self.waiters.lock().entry(key) {
            HashMapEntry::Occupied(mut o) => {
                let (tx, rx) = oneshot::channel();
                o.get_mut().push(tx);
                self.metrics.memory_queue.increase(1);
                #[cfg(feature = "tracing")]
                let wait = rx.in_span(Span::enter_with_local_parent(
                    "foyer::memory::raw::fetch_with_runtime::wait",
                ));
                #[cfg(not(feature = "tracing"))]
                let wait = rx;
                RawShardFetch::Wait(wait)
            }
            HashMapEntry::Vacant(v) => {
                v.insert(vec![]);
                self.metrics.memory_fetch.increase(1);
                RawShardFetch::Miss
            }
        }
    }
}

#[expect(clippy::type_complexity)]
struct RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    shards: Vec<RwLock<RawCacheShard<E, S, I>>>,

    capacity: usize,

    hash_builder: Arc<S>,
    weighter: Arc<dyn Weighter<E::Key, E::Value>>,

    metrics: Arc<Metrics>,
    event_listener: Option<Arc<dyn EventListener<Key = E::Key, Value = E::Value>>>,
    pipe: ArcSwap<Box<dyn Pipe<Key = E::Key, Value = E::Value, Properties = E::Properties>>>,
}

impl<E, S, I> RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::inner::clear"))]
    fn clear(&self) {
        let mut garbages = vec![];

        self.shards
            .iter()
            .map(|shard| shard.write())
            .for_each(|mut shard| shard.clear(&mut garbages));

        // Do not deallocate data within the lock section.
        if let Some(listener) = self.event_listener.as_ref() {
            for record in garbages {
                listener.on_leave(Event::Clear, record.key(), record.value());
            }
        }
    }
}

pub struct RawCache<E, S = ahash::RandomState, I = HashTableIndexer<E>>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inner: Arc<RawCacheInner<E, S, I>>,
}

impl<E, S, I> Drop for RawCacheInner<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn drop(&mut self) {
        self.clear();
    }
}

impl<E, S, I> Clone for RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<E, S, I> RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn new(config: RawCacheConfig<E, S>) -> Self {
        let shard_capacity = config.capacity / config.shards;

        let shards = (0..config.shards)
            .map(|_| RawCacheShard {
                eviction: E::new(shard_capacity, &config.eviction_config),
                indexer: Sentry::default(),
                usage: 0,
                capacity: shard_capacity,
                waiters: Mutex::default(),
                metrics: config.metrics.clone(),
                _event_listener: config.event_listener.clone(),
            })
            .map(RwLock::new)
            .collect_vec();

        let pipe: Box<dyn Pipe<Key = E::Key, Value = E::Value, Properties = E::Properties>> =
            Box::new(NoopPipe::default());

        let inner = RawCacheInner {
            shards,
            capacity: config.capacity,
            hash_builder: Arc::new(config.hash_builder),
            weighter: config.weighter,
            metrics: config.metrics,
            event_listener: config.event_listener,
            pipe: ArcSwap::new(Arc::new(pipe)),
        };

        Self { inner: Arc::new(inner) }
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::resize"))]
    pub fn resize(&self, capacity: usize) -> Result<()> {
        let shards = self.inner.shards.len();
        let shard_capacity = capacity / shards;

        let handles = (0..shards)
            .map(|i| {
                let inner = self.inner.clone();
                std::thread::spawn(move || {
                    let mut garbages = vec![];
                    let res = inner.shards[i].write().with(|mut shard| {
                        shard.eviction.update(shard_capacity, None).inspect(|_| {
                            shard.capacity = shard_capacity;
                            shard.evict(shard_capacity, &mut garbages)
                        })
                    });
                    // Deallocate data out of the lock critical section.
                    let pipe = inner.pipe.load();
                    let piped = pipe.is_enabled();
                    if inner.event_listener.is_some() || piped {
                        for (event, record) in garbages {
                            if let Some(listener) = inner.event_listener.as_ref() {
                                listener.on_leave(event, record.key(), record.value())
                            }
                            if piped && event == Event::Evict {
                                pipe.send(Piece::new(record));
                            }
                        }
                    }
                    res
                })
            })
            .collect_vec();

        let errs = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|res| res.is_err())
            .map(|res| res.unwrap_err())
            .collect_vec();
        if !errs.is_empty() {
            return Err(Error::multiple(errs));
        }

        Ok(())
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::insert"))]
    pub fn insert(&self, key: E::Key, value: E::Value) -> RawCacheEntry<E, S, I> {
        self.insert_with_properties(key, value, Default::default())
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::insert_with_properties")
    )]
    pub fn insert_with_properties(
        &self,
        key: E::Key,
        value: E::Value,
        properties: E::Properties,
    ) -> RawCacheEntry<E, S, I> {
        let hash = self.inner.hash_builder.hash_one(&key);
        let weight = (self.inner.weighter)(&key, &value);

        let mut garbages = vec![];
        let mut waiters = vec![];

        let record = self.inner.shards[self.shard(hash)].write().with(|mut shard| {
            shard.emplace(
                Data {
                    key,
                    value,
                    properties,
                    hash,
                    weight,
                },
                &mut garbages,
                &mut waiters,
            )
        });

        // Notify waiters out of the lock critical section.
        for waiter in waiters {
            let _ = waiter.send(RawCacheEntry {
                record: record.clone(),
                inner: self.inner.clone(),
            });
        }

        // Deallocate data out of the lock critical section.
        let pipe = self.inner.pipe.load();
        let piped = pipe.is_enabled();
        if self.inner.event_listener.is_some() || piped {
            for (event, record) in garbages {
                if let Some(listener) = self.inner.event_listener.as_ref() {
                    listener.on_leave(event, record.key(), record.value())
                }
                if piped && event == Event::Evict {
                    pipe.send(Piece::new(record));
                }
            }
        }

        RawCacheEntry {
            record,
            inner: self.inner.clone(),
        }
    }

    /// Evict all entries in the cache and offload them into the disk cache via the pipe if needed.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::evict_all"))]
    pub fn evict_all(&self) {
        let mut garbages = vec![];
        for shard in self.inner.shards.iter() {
            shard.write().evict(0, &mut garbages);
        }

        // Deallocate data out of the lock critical section.
        let pipe = self.inner.pipe.load();
        let piped = pipe.is_enabled();
        if self.inner.event_listener.is_some() || piped {
            for (event, record) in garbages {
                if let Some(listener) = self.inner.event_listener.as_ref() {
                    listener.on_leave(event, record.key(), record.value())
                }
                if piped && event == Event::Evict {
                    pipe.send(Piece::new(record));
                }
            }
        }
    }

    /// Evict all entries in the cache and offload them into the disk cache via the pipe if needed.
    ///
    /// This function obeys the io throttler of the disk cache and make sure all entries will be offloaded.
    /// Therefore, this function is asynchronous.
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::flush"))]
    pub async fn flush(&self) {
        let mut garbages = vec![];
        for shard in self.inner.shards.iter() {
            shard.write().evict(0, &mut garbages);
        }

        // Deallocate data out of the lock critical section.
        let pipe = self.inner.pipe.load();
        let piped = pipe.is_enabled();

        if let Some(listener) = self.inner.event_listener.as_ref() {
            for (event, record) in garbages.iter() {
                listener.on_leave(*event, record.key(), record.value());
            }
        }
        if piped {
            let pieces = garbages.into_iter().map(|(_, record)| Piece::new(record)).collect_vec();
            pipe.flush(pieces).await;
        }
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::remove"))]
    pub fn remove<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.shards[self.shard(hash)]
            .write()
            .with(|mut shard| {
                shard.remove(hash, key).map(|record| RawCacheEntry {
                    inner: self.inner.clone(),
                    record,
                })
            })
            .inspect(|record| {
                // Deallocate data out of the lock critical section.
                if let Some(listener) = self.inner.event_listener.as_ref() {
                    listener.on_leave(Event::Remove, record.key(), record.value());
                }
            })
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::get"))]
    pub fn get<Q>(&self, key: &Q) -> Option<RawCacheEntry<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        let record = match E::acquire() {
            Op::Noop => self.inner.shards[self.shard(hash)].read().get_noop(hash, key),
            Op::Immutable(_) => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_immutable(hash, key)),
            Op::Mutable(_) => self.inner.shards[self.shard(hash)]
                .write()
                .with(|mut shard| shard.get_mutable(hash, key)),
        }?;

        Some(RawCacheEntry {
            inner: self.inner.clone(),
            record,
        })
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::contains"))]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        self.inner.shards[self.shard(hash)]
            .read()
            .with(|shard| shard.indexer.get(hash, key).is_some())
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::touch"))]
    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);

        match E::acquire() {
            Op::Noop => self.inner.shards[self.shard(hash)].read().get_noop(hash, key),
            Op::Immutable(_) => self.inner.shards[self.shard(hash)]
                .read()
                .with(|shard| shard.get_immutable(hash, key)),
            Op::Mutable(_) => self.inner.shards[self.shard(hash)]
                .write()
                .with(|mut shard| shard.get_mutable(hash, key)),
        }
        .is_some()
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::clear"))]
    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn usage(&self) -> usize {
        self.inner.shards.iter().map(|shard| shard.read().usage).sum()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    pub fn hash_builder(&self) -> &Arc<S> {
        &self.inner.hash_builder
    }

    pub fn shards(&self) -> usize {
        self.inner.shards.len()
    }

    pub fn set_pipe(&self, pipe: Box<dyn Pipe<Key = E::Key, Value = E::Value, Properties = E::Properties>>) {
        self.inner.pipe.store(Arc::new(pipe));
    }

    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.inner.shards.len()
    }
}

pub struct RawCacheEntry<E, S = ahash::RandomState, I = HashTableIndexer<E>>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inner: Arc<RawCacheInner<E, S, I>>,
    record: Arc<Record<E>>,
}

impl<E, S, I> Debug for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawCacheEntry").field("record", &self.record).finish()
    }
}

impl<E, S, I> Drop for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn drop(&mut self) {
        let hash = self.record.hash();
        let shard = &self.inner.shards[hash as usize % self.inner.shards.len()];

        if self.record.dec_refs(1) == 0 {
            match E::release() {
                Op::Noop => {}
                Op::Immutable(_) => shard.read().with(|shard| shard.release_immutable(&self.record)),
                Op::Mutable(_) => shard.write().with(|mut shard| shard.release_mutable(&self.record)),
            }

            if self.record.is_ephemeral() {
                shard
                    .write()
                    .with(|mut shard| shard.remove(hash, self.key()))
                    .inspect(|record| {
                        // Deallocate data out of the lock critical section.
                        let pipe = self.inner.pipe.load();
                        let piped = pipe.is_enabled();
                        let event = Event::Evict;
                        if self.inner.event_listener.is_some() || piped {
                            if let Some(listener) = self.inner.event_listener.as_ref() {
                                listener.on_leave(Event::Evict, record.key(), record.value());
                            }
                        }
                        if piped && event == Event::Evict {
                            pipe.send(self.piece());
                        }
                    });
            }
        }
    }
}

impl<E, S, I> Clone for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn clone(&self) -> Self {
        self.record.inc_refs(1);
        Self {
            inner: self.inner.clone(),
            record: self.record.clone(),
        }
    }
}

impl<E, S, I> Deref for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    type Target = E::Value;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<E, S, I> Send for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
}

unsafe impl<E, S, I> Sync for RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
}

impl<E, S, I> RawCacheEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn hash(&self) -> u64 {
        self.record.hash()
    }

    pub fn key(&self) -> &E::Key {
        self.record.key()
    }

    pub fn value(&self) -> &E::Value {
        self.record.value()
    }

    pub fn properties(&self) -> &E::Properties {
        self.record.properties()
    }

    pub fn weight(&self) -> usize {
        self.record.weight()
    }

    pub fn refs(&self) -> usize {
        self.record.refs()
    }

    pub fn is_outdated(&self) -> bool {
        !self.record.is_in_indexer()
    }

    pub fn piece(&self) -> Piece<E::Key, E::Value, E::Properties> {
        Piece::new(self.record.clone())
    }
}

/// The state of `fetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchState {
    /// Cache hit.
    Hit,
    /// Cache miss, but wait in queue.
    Wait,
    /// Cache miss, and there is no other waiters at the moment.
    Miss,
}

/// Context for fetch calls.
#[derive(Debug)]
pub struct FetchContext {
    /// If this fetch is caused by disk cache throttled.
    pub throttled: bool,
    /// Fetched entry source.
    pub source: Source,
}

enum RawShardFetch<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Hit(Arc<Record<E>>),
    Wait(RawFetchWait<E, S, I>),
    Miss,
}

pub type RawFetch<E, ER, S = ahash::RandomState, I = HashTableIndexer<E>> =
    DiversionFuture<RawFetchInner<E, ER, S, I>, std::result::Result<RawCacheEntry<E, S, I>, ER>, FetchContext>;

type RawFetchHit<E, S, I> = Option<RawCacheEntry<E, S, I>>;
#[cfg(feature = "tracing")]
type RawFetchWait<E, S, I> = InSpan<oneshot::Receiver<RawCacheEntry<E, S, I>>>;
#[cfg(not(feature = "tracing"))]
type RawFetchWait<E, S, I> = oneshot::Receiver<RawCacheEntry<E, S, I>>;
type RawFetchMiss<E, I, S, ER, DFS> = JoinHandle<Diversion<std::result::Result<RawCacheEntry<E, S, I>, ER>, DFS>>;

#[pin_project(project = RawFetchInnerProj)]
pub enum RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Hit(RawFetchHit<E, S, I>),
    Wait(#[pin] RawFetchWait<E, S, I>),
    Miss(#[pin] RawFetchMiss<E, I, S, ER, FetchContext>),
}

impl<E, ER, S, I> RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn state(&self) -> FetchState {
        match self {
            RawFetchInner::Hit(_) => FetchState::Hit,
            RawFetchInner::Wait(_) => FetchState::Wait,
            RawFetchInner::Miss(_) => FetchState::Miss,
        }
    }
}

impl<E, ER, S, I> Future for RawFetchInner<E, ER, S, I>
where
    E: Eviction,
    ER: From<oneshot::error::RecvError>,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    type Output = Diversion<std::result::Result<RawCacheEntry<E, S, I>, ER>, FetchContext>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            RawFetchInnerProj::Hit(opt) => Poll::Ready(Ok(opt.take().unwrap()).into()),
            RawFetchInnerProj::Wait(waiter) => waiter.poll(cx).map_err(|err| err.into()).map(Diversion::from),
            RawFetchInnerProj::Miss(handle) => handle.poll(cx).map(|join| join.unwrap()),
        }
    }
}

impl<E, S, I> RawCache<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
    E::Key: Clone,
{
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::fetch"))]
    pub fn fetch<F, FU, ER>(&self, key: E::Key, fetch: F) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<E::Value, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        self.fetch_inner(
            key,
            Default::default(),
            fetch,
            &tokio::runtime::Handle::current().into(),
        )
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::memory::raw::fetch_with_properties")
    )]
    pub fn fetch_with_properties<F, FU, ER, ID>(
        &self,
        key: E::Key,
        properties: E::Properties,
        fetch: F,
    ) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = ID> + Send + 'static,
        ER: Send + 'static + Debug,
        ID: Into<Diversion<std::result::Result<E::Value, ER>, FetchContext>>,
    {
        self.fetch_inner(key, properties, fetch, &tokio::runtime::Handle::current().into())
    }

    /// Advanced fetch with specified runtime.
    ///
    /// This function is for internal usage and the doc is hidden.
    #[doc(hidden)]
    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::memory::raw::fetch_inner"))]
    pub fn fetch_inner<F, FU, ER, ID>(
        &self,
        key: E::Key,
        mut properties: E::Properties,
        fetch: F,
        runtime: &SingletonHandle,
    ) -> RawFetch<E, ER, S, I>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = ID> + Send + 'static,
        ER: Send + 'static + Debug,
        ID: Into<Diversion<std::result::Result<E::Value, ER>, FetchContext>>,
    {
        let hash = self.inner.hash_builder.hash_one(&key);

        let raw = match E::acquire() {
            Op::Noop => self.inner.shards[self.shard(hash)].read().fetch_noop(hash, &key),
            Op::Immutable(_) => self.inner.shards[self.shard(hash)].read().fetch_immutable(hash, &key),
            Op::Mutable(_) => self.inner.shards[self.shard(hash)].write().fetch_mutable(hash, &key),
        };

        match raw {
            RawShardFetch::Hit(record) => {
                return RawFetch::new(RawFetchInner::Hit(Some(RawCacheEntry {
                    record,
                    inner: self.inner.clone(),
                })))
            }
            RawShardFetch::Wait(future) => return RawFetch::new(RawFetchInner::Wait(future)),
            RawShardFetch::Miss => {}
        }

        let cache = self.clone();
        let future = fetch();
        let join = runtime.spawn({
            let task = async move {
                #[cfg(feature = "tracing")]
                let Diversion { target, store } = future
                    .in_span(Span::enter_with_local_parent("foyer::memory::raw::fetch_inner::fn"))
                    .await
                    .into();
                #[cfg(not(feature = "tracing"))]
                let Diversion { target, store } = future.await.into();

                let value = match target {
                    Ok(value) => value,
                    Err(e) => {
                        cache.inner.shards[cache.shard(hash)].read().waiters.lock().remove(&key);
                        tracing::debug!("[fetch]: error raise while fetching, all waiter are dropped, err: {e:?}");
                        return Diversion { target: Err(e), store };
                    }
                };
                if let Some(ctx) = store.as_ref() {
                    if ctx.throttled {
                        properties = properties.with_location(Location::InMem)
                    }
                    properties = properties.with_source(ctx.source)
                };
                let location = properties.location().unwrap_or_default();
                properties = properties.with_ephemeral(matches! {location, Location::OnDisk});
                let entry = cache.insert_with_properties(key, value, properties);
                Diversion {
                    target: Ok(entry),
                    store,
                }
            };
            #[cfg(feature = "tracing")]
            let task = task.in_span(Span::enter_with_local_parent(
                "foyer::memory::generic::fetch_with_runtime::spawn",
            ));
            task
        });

        RawFetch::new(RawFetchInner::Miss(join))
    }
}

#[cfg(test)]
mod tests {
    use foyer_common::hasher::ModRandomState;
    use rand::{rngs::SmallRng, seq::IndexedRandom, RngCore, SeedableRng};

    use super::*;
    use crate::{
        eviction::{
            fifo::{Fifo, FifoConfig},
            lfu::{Lfu, LfuConfig},
            lru::{Lru, LruConfig},
            s3fifo::{S3Fifo, S3FifoConfig},
            test_utils::TestProperties,
        },
        test_utils::PiecePipe,
    };

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<RawCache<Fifo<(), (), TestProperties>>>();
        is_send_sync_static::<RawCache<S3Fifo<(), (), TestProperties>>>();
        is_send_sync_static::<RawCache<Lfu<(), (), TestProperties>>>();
        is_send_sync_static::<RawCache<Lru<(), (), TestProperties>>>();
    }

    #[expect(clippy::type_complexity)]
    fn fifo_cache_for_test(
    ) -> RawCache<Fifo<u64, u64, TestProperties>, ModRandomState, HashTableIndexer<Fifo<u64, u64, TestProperties>>>
    {
        RawCache::new(RawCacheConfig {
            capacity: 256,
            shards: 4,
            eviction_config: FifoConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
            metrics: Arc::new(Metrics::noop()),
        })
    }

    #[expect(clippy::type_complexity)]
    fn s3fifo_cache_for_test(
    ) -> RawCache<S3Fifo<u64, u64, TestProperties>, ModRandomState, HashTableIndexer<S3Fifo<u64, u64, TestProperties>>>
    {
        RawCache::new(RawCacheConfig {
            capacity: 256,
            shards: 4,
            eviction_config: S3FifoConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
            metrics: Arc::new(Metrics::noop()),
        })
    }

    #[expect(clippy::type_complexity)]
    fn lru_cache_for_test(
    ) -> RawCache<Lru<u64, u64, TestProperties>, ModRandomState, HashTableIndexer<Lru<u64, u64, TestProperties>>> {
        RawCache::new(RawCacheConfig {
            capacity: 256,
            shards: 4,
            eviction_config: LruConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
            metrics: Arc::new(Metrics::noop()),
        })
    }

    #[expect(clippy::type_complexity)]
    fn lfu_cache_for_test(
    ) -> RawCache<Lfu<u64, u64, TestProperties>, ModRandomState, HashTableIndexer<Lfu<u64, u64, TestProperties>>> {
        RawCache::new(RawCacheConfig {
            capacity: 256,
            shards: 4,
            eviction_config: LfuConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
            metrics: Arc::new(Metrics::noop()),
        })
    }

    #[test_log::test]
    fn test_insert_ephemeral() {
        let fifo = fifo_cache_for_test();

        let e1 = fifo.insert_with_properties(1, 1, TestProperties::default().with_ephemeral(true));
        assert_eq!(fifo.usage(), 1);
        drop(e1);
        assert_eq!(fifo.usage(), 0);

        let e2a = fifo.insert_with_properties(2, 2, TestProperties::default().with_ephemeral(true));
        assert_eq!(fifo.usage(), 1);
        let e2b = fifo.get(&2).expect("entry 2 should exist");
        drop(e2a);
        assert_eq!(fifo.usage(), 1);
        drop(e2b);
        assert_eq!(fifo.usage(), 1);
    }

    #[test]
    fn test_evict_all() {
        let pipe = Box::new(PiecePipe::default());

        let fifo = fifo_cache_for_test();
        fifo.set_pipe(pipe.clone());
        for i in 0..fifo.capacity() as _ {
            fifo.insert(i, i);
        }
        assert_eq!(fifo.usage(), fifo.capacity());

        fifo.evict_all();
        let mut pieces = pipe
            .pieces()
            .iter()
            .map(|p| (p.hash(), *p.key(), *p.value()))
            .collect_vec();
        pieces.sort_by_key(|t| t.0);
        let expected = (0..fifo.capacity() as u64).map(|i| (i, i, i)).collect_vec();
        assert_eq!(pieces, expected);
    }

    #[test]
    fn test_insert_size_over_capacity() {
        let cache: RawCache<Fifo<Vec<u8>, Vec<u8>, TestProperties>, ModRandomState> = RawCache::new(RawCacheConfig {
            capacity: 4 * 1024, // 4KB
            shards: 1,
            eviction_config: FifoConfig::default(),
            hash_builder: Default::default(),
            weighter: Arc::new(|k, v| k.len() + v.len()),
            event_listener: None,
            metrics: Arc::new(Metrics::noop()),
        });

        let key = vec![b'k'; 1024]; // 1KB
        let value = vec![b'v'; 5 * 1024]; // 5KB

        cache.insert(key.clone(), value.clone());
        assert_eq!(cache.usage(), 6 * 1024);
        assert_eq!(cache.get(&key).unwrap().value(), &value);
    }

    fn test_resize<E>(cache: &RawCache<E, ModRandomState, HashTableIndexer<E>>)
    where
        E: Eviction<Key = u64, Value = u64>,
    {
        let capacity = cache.capacity();
        for i in 0..capacity as u64 * 2 {
            cache.insert(i, i);
        }
        assert_eq!(cache.usage(), capacity);
        cache.resize(capacity / 2).unwrap();
        assert_eq!(cache.usage(), capacity / 2);
        for i in 0..capacity as u64 * 2 {
            cache.insert(i, i);
        }
        assert_eq!(cache.usage(), capacity / 2);
    }

    #[test]
    fn test_fifo_cache_resize() {
        let cache = fifo_cache_for_test();
        test_resize(&cache);
    }

    #[test]
    fn test_s3fifo_cache_resize() {
        let cache = s3fifo_cache_for_test();
        test_resize(&cache);
    }

    #[test]
    fn test_lru_cache_resize() {
        let cache = lru_cache_for_test();
        test_resize(&cache);
    }

    #[test]
    fn test_lfu_cache_resize() {
        let cache = lfu_cache_for_test();
        test_resize(&cache);
    }

    mod fuzzy {
        use foyer_common::properties::Hint;

        use super::*;

        fn fuzzy<E>(cache: RawCache<E>, hints: Vec<Hint>)
        where
            E: Eviction<Key = u64, Value = u64, Properties = TestProperties>,
        {
            let handles = (0..8)
                .map(|i| {
                    let c = cache.clone();
                    let hints = hints.clone();
                    std::thread::spawn(move || {
                        let mut rng = SmallRng::seed_from_u64(i);
                        for _ in 0..100000 {
                            let key = rng.next_u64();
                            if let Some(entry) = c.get(&key) {
                                assert_eq!(key, *entry);
                                drop(entry);
                                continue;
                            }
                            let hint = hints.choose(&mut rng).cloned().unwrap();
                            c.insert_with_properties(key, key, TestProperties::default().with_hint(hint));
                        }
                    })
                })
                .collect_vec();

            handles.into_iter().for_each(|handle| handle.join().unwrap());

            assert_eq!(cache.usage(), cache.capacity());
        }

        #[test_log::test]
        fn test_fifo_cache_fuzzy() {
            let cache: RawCache<Fifo<u64, u64, TestProperties>> = RawCache::new(RawCacheConfig {
                capacity: 256,
                shards: 4,
                eviction_config: FifoConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
                metrics: Arc::new(Metrics::noop()),
            });
            let hints = vec![Hint::Normal];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_s3fifo_cache_fuzzy() {
            let cache: RawCache<S3Fifo<u64, u64, TestProperties>> = RawCache::new(RawCacheConfig {
                capacity: 256,
                shards: 4,
                eviction_config: S3FifoConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
                metrics: Arc::new(Metrics::noop()),
            });
            let hints = vec![Hint::Normal];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_lru_cache_fuzzy() {
            let cache: RawCache<Lru<u64, u64, TestProperties>> = RawCache::new(RawCacheConfig {
                capacity: 256,
                shards: 4,
                eviction_config: LruConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
                metrics: Arc::new(Metrics::noop()),
            });
            let hints = vec![Hint::Normal, Hint::Low];
            fuzzy(cache, hints);
        }

        #[test_log::test]
        fn test_lfu_cache_fuzzy() {
            let cache: RawCache<Lfu<u64, u64, TestProperties>> = RawCache::new(RawCacheConfig {
                capacity: 256,
                shards: 4,
                eviction_config: LfuConfig::default(),
                hash_builder: Default::default(),
                weighter: Arc::new(|_, _| 1),
                event_listener: None,
                metrics: Arc::new(Metrics::noop()),
            });
            let hints = vec![Hint::Normal];
            fuzzy(cache, hints);
        }
    }
}
