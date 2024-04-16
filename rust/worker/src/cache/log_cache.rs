use crate::types::LogRecord;
use ahash::RandomState;
use foyer_memory::Cache;
use foyer_memory::CacheContext;
use foyer_memory::CacheEntry;
use foyer_memory::CacheEventListener;
use foyer_memory::LruCacheConfig;
use foyer_memory::LruConfig;
use once_cell::sync::OnceCell;
use std::cmp::Ord;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

static LOG_CACHE: OnceCell<LogCache> = OnceCell::new();

pub fn init(config: LogCacheConfig) {
    if LOG_CACHE.set(LogCache::new(config)).is_err() {
        panic!("log cache already initialized");
    }
}

pub fn get() -> &'static LogCache {
    LOG_CACHE.get().expect("log cache not initialized")
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub(crate) struct LogCacheKey {
    pub(crate) collection_id: String,
    pub(crate) start_log_offset: i64,
}

impl Ord for LogCacheKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.collection_id
            .cmp(&other.collection_id)
            .then(self.start_log_offset.cmp(&other.start_log_offset))
    }
}

impl PartialOrd for LogCacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) type LogCacheEntry = CacheEntry<LogCacheKey, Arc<[LogRecord]>, LogCacheEventListener>;

pub(crate) struct LogCacheEventListener {}

impl CacheEventListener<LogCacheKey, Arc<[LogRecord]>> for LogCacheEventListener {
    fn on_release(
        &self,
        key: LogCacheKey,
        value: Arc<[LogRecord]>,
        context: CacheContext,
        charges: usize,
    ) {
        println!(
            "LogCacheEventListener::on_release: key: {:?}, value: {:?}, context: {:?}, charges: {}",
            key, value, context, charges
        );
    }
}

pub(crate) struct LogCache {
    cache: Cache<LogCacheKey, Arc<[LogRecord]>, LogCacheEventListener>,
}

impl Debug for LogCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogCache").finish()
    }
}

pub(crate) struct EntryDataHandle {
    pub handle: LogCacheEntry,
}

impl EntryDataHandle {
    pub fn new(entry: LogCacheEntry) -> EntryDataHandle {
        EntryDataHandle { handle: entry }
    }
}

pub(crate) enum EvictionConfig {
    Lru,
}

pub(crate) struct LogCacheConfig {
    pub capacity: usize,
    pub shard_num: usize,
    pub eviction: EvictionConfig,
    pub listener: LogCacheEventListener,
}

impl LogCache {
    pub fn new(config: LogCacheConfig) -> LogCache {
        let capacity = config.capacity;
        let shards = config.shard_num;
        let object_pool_capacity = shards * 1024;
        let hash_builder = RandomState::default();
        let event_listener = config.listener;

        let cache = match config.eviction {
            EvictionConfig::Lru => Cache::lru(LruCacheConfig {
                capacity,
                shards,
                object_pool_capacity,
                eviction_config: LruConfig {
                    high_priority_pool_ratio: 0.1,
                },
                hash_builder,
                event_listener,
            }),
        };
        LogCache { cache }
    }

    pub fn get(&self, collection_id: String, start_log_offset: i64) -> Option<EntryDataHandle> {
        let key = LogCacheKey {
            collection_id,
            start_log_offset,
        };
        let value = self.cache.get(&key);
        match value {
            Some(value) => Some(EntryDataHandle::new(value)),
            None => None,
        }
    }

    pub fn insert(
        &self,
        collection_id: String,
        start_log_offset: i64,
        logs: Arc<[LogRecord]>,
    ) -> EntryDataHandle {
        let key = LogCacheKey {
            collection_id,
            start_log_offset,
        };
        let entry = self.cache.insert(key, logs, 1);
        EntryDataHandle::new(entry)
    }

    pub fn usage(&self) -> usize {
        self.cache.usage()
    }

    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Operation;
    use crate::types::OperationRecord;

    #[tokio::test]
    async fn test_log_cache() {
        let config = LogCacheConfig {
            capacity: 1,
            shard_num: 1,
            eviction: EvictionConfig::Lru,
            listener: LogCacheEventListener {},
        };
        let log_cache = LogCache::new(config);

        let collection_id_1 = "collection_id".to_string();
        let start_log_offset = 0;
        let mut logs = vec![LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
            },
        }];
        log_cache.insert(
            collection_id_1.clone(),
            start_log_offset,
            logs.clone().into(),
        );
        let holder = log_cache.get(collection_id_1.clone(), start_log_offset);

        assert_eq!(holder.is_some(), true);
        let holder = holder.unwrap();
        let value = holder.handle.value();
        assert_eq!(value.len(), 1);
        assert_eq!(value[0].log_offset, 0);
        assert_eq!(value[0].record.id, "embedding_id_1");
        drop(holder);

        logs.append(
            vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            }]
            .as_mut(),
        );
        log_cache.insert(collection_id_1.clone(), start_log_offset, logs.into());
        let holder = log_cache.get(collection_id_1.clone(), start_log_offset);
        assert_eq!(holder.is_some(), true);
        let holder = holder.unwrap();
        let value = holder.handle.value();
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].log_offset, 0);
        assert_eq!(value[0].record.id, "embedding_id_1");
        assert_eq!(value[1].log_offset, 1);
        assert_eq!(value[1].record.id, "embedding_id_2");
        drop(holder);

        let collection_id_2 = "collection_id_2".to_string();
        let start_log_offset = 0;
        let logs = vec![LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "embedding_id_3".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
            },
        }];
        log_cache.insert(
            collection_id_2.clone(),
            start_log_offset,
            logs.clone().into(),
        );
        let holder = log_cache.get(collection_id_1.clone(), start_log_offset);
        assert_eq!(holder.is_none(), true);
        drop(holder);

        let holder = log_cache.get(collection_id_2.clone(), start_log_offset);
        assert_eq!(holder.is_some(), true);
        let holder = holder.unwrap();
        let value = holder.handle.value();
        assert_eq!(value.len(), 1);
        assert_eq!(value[0].log_offset, 0);
        assert_eq!(value[0].record.id, "embedding_id_3");
        drop(holder);

        assert_eq!(log_cache.capacity(), 1);
        assert_eq!(log_cache.usage(), 1);
    }
}
