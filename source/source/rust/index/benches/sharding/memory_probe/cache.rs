//! Diagnostic-only cache accounting. Census reads affect cache recency.
use async_trait::async_trait;
use chroma_cache::{foyer::Code, Cache, CacheError, PersistentCache, Weighted};
use parking_lot::Mutex;
use serde::Serialize;
use std::{collections::HashSet, fmt::Debug, sync::Arc};
use uuid::Uuid;

#[derive(Default, Serialize, Clone)]
pub struct Counts {
    pub gets: u64,
    pub obtains: u64,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub clears: u64,
    pub may_contains: u64,
    pub census_gets: u64,
    pub census_present: u64,
    pub census_payload_estimate_bytes: u64,
    pub census_weight: u64,
}
#[derive(Default)]
pub struct Ledger {
    counts: Mutex<Counts>,
    known: Mutex<HashSet<Uuid>>,
}
pub struct Observed<V: Clone + Send + Sync + Weighted + Code + 'static> {
    pub inner: Arc<dyn PersistentCache<Uuid, V>>,
    pub ledger: Arc<Ledger>,
    pub size: fn(&V) -> u64,
}
impl<V: Clone + Send + Sync + Weighted + Code + 'static> Clone for Observed<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            ledger: self.ledger.clone(),
            size: self.size,
        }
    }
}
impl<V: Clone + Send + Sync + Weighted + Code + 'static> Debug for Observed<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DiagnosticObservedCache")
    }
}
impl<V: Clone + Send + Sync + Weighted + Code + 'static> Observed<V> {
    pub async fn census(&self) -> Result<Counts, CacheError> {
        let keys: Vec<_> = self.ledger.known.lock().iter().copied().collect();
        let (mut present, mut bytes, mut weight) = (0, 0, 0);
        for key in &keys {
            // This bypasses operation counters, and each temporary clone ends here.
            if let Some(value) = self.inner.get(key).await? {
                present += 1;
                bytes += (self.size)(&value);
                weight += value.weight() as u64;
            }
        }
        let mut counts = self.ledger.counts.lock();
        counts.census_gets += keys.len() as u64;
        counts.census_present = present;
        counts.census_payload_estimate_bytes = bytes;
        counts.census_weight = weight;
        Ok(counts.clone())
    }
    fn record(&self, hit: bool, obtain: bool) {
        let mut counts = self.ledger.counts.lock();
        if obtain {
            counts.obtains += 1;
        } else {
            counts.gets += 1;
        }
        if hit {
            counts.hits += 1;
        } else {
            counts.misses += 1;
        }
    }
}
#[async_trait]
impl<V: Clone + Send + Sync + Weighted + Code + 'static> Cache<Uuid, V> for Observed<V> {
    async fn insert(&self, key: Uuid, value: V) {
        self.ledger.known.lock().insert(key);
        self.ledger.counts.lock().inserts += 1;
        self.inner.insert(key, value).await;
    }
    async fn get(&self, key: &Uuid) -> Result<Option<V>, CacheError> {
        self.ledger.known.lock().insert(*key);
        let result = self.inner.get(key).await?;
        self.record(result.is_some(), false);
        Ok(result)
    }
    async fn obtain(&self, key: Uuid) -> Result<Option<V>, CacheError> {
        self.ledger.known.lock().insert(key);
        let result = self.inner.obtain(key).await?;
        self.record(result.is_some(), true);
        Ok(result)
    }
    async fn remove(&self, key: &Uuid) {
        self.inner.remove(key).await;
    }
    async fn may_contain(&self, key: &Uuid) -> bool {
        self.ledger.counts.lock().may_contains += 1;
        self.inner.may_contain(key).await
    }
    async fn clear(&self) -> Result<(), CacheError> {
        self.inner.clear().await?;
        self.ledger.counts.lock().clears += 1;
        Ok(())
    }
}
#[async_trait]
impl<V: Clone + Send + Sync + Weighted + Code + 'static> PersistentCache<Uuid, V> for Observed<V> {}
