use crate::cache::log_cache;
use crate::cache::log_cache::LogCache;
use crate::log::log::CollectionInfo;
use crate::log::log::GetCollectionsWithNewDataError;
use crate::log::log::Log;
use crate::log::log::PullLogsError;
use crate::types::LogRecord;
use async_trait::async_trait;

use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct CachedLog {
    cache: &'static LogCache,
    log: Box<dyn Log>,
}

impl CachedLog {
    pub(crate) fn new(log: Box<dyn Log>) -> Self {
        let cache = log_cache::get();
        Self { cache, log }
    }
}

#[async_trait]
impl Log for CachedLog {
    async fn read(
        &mut self,
        collection_id: Uuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Arc<[LogRecord]>, PullLogsError> {
        let entry: Option<log_cache::EntryDataHandle> =
            self.cache.get(collection_id.to_string(), offset);
        if let Some(entry) = entry {
            if entry.handle.value().len() < batch_size as usize {
                let data = self
                    .log
                    .read(collection_id, offset, batch_size, end_timestamp)
                    .await?;
                let entry = self.cache.insert(collection_id.to_string(), offset, data);
                return Ok(entry.handle.value().to_owned());
            }
            return Ok(entry.handle.value().to_owned());
        }

        let data = self
            .log
            .read(collection_id, offset, batch_size, end_timestamp)
            .await?;
        let entry = self.cache.insert(collection_id.to_string(), offset, data);
        Ok(entry.handle.value().to_owned())
    }

    async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        self.log.get_collections_with_new_data().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::log_cache;
    use crate::cache::log_cache::EvictionConfig;
    use crate::cache::log_cache::LogCacheConfig;
    use crate::cache::log_cache::LogCacheEventListener;
    use crate::log::log::InMemoryLog;
    use crate::log::log::InternalLogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_cached_log() {
        let config = LogCacheConfig {
            capacity: 1,
            shard_num: 1,
            eviction: EvictionConfig::Lru,
            listener: LogCacheEventListener {},
        };
        log_cache::init(config);

        let mut log = Box::new(InMemoryLog::new());
        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let collection_id_1 = collection_uuid_1.to_string();
        log.add_log(
            collection_id_1.clone(),
            Box::new(InternalLogRecord {
                collection_id: collection_id_1.clone(),
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        operation: Operation::Add,
                    },
                },
            }),
        );
        let mut cached_log = CachedLog::new(log);

        let offset = 0;
        let batch_size = 1;
        let end_timestamp = None;
        let result = cached_log
            .read(collection_uuid_1.clone(), offset, batch_size, end_timestamp)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
        let log_cache = log_cache::get();
        let entry = log_cache.get(collection_id_1.clone(), offset);
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.handle.value().len(), 1);
        assert_eq!(entry.handle.value()[0].log_offset, 0);
        assert_eq!(entry.handle.value()[0].record.id, "embedding_id_1");
    }
}
