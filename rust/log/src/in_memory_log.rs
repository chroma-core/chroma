use std::collections::HashMap;
use std::fmt::Debug;

use chroma_error::ChromaError;
use chroma_types::{CollectionUuid, LogRecord};

use crate::types::CollectionInfo;

// This is used for testing only, it represents a log record that is stored in memory
// internal to a mock log implementation
#[derive(Clone)]
pub struct InternalLogRecord {
    pub collection_id: CollectionUuid,
    pub log_offset: i64,
    pub log_ts: i64,
    pub record: LogRecord,
}

impl Debug for InternalLogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRecord")
            .field("collection_id", &self.collection_id)
            .field("log_offset", &self.log_offset)
            .field("log_ts", &self.log_ts)
            .field("record", &self.record)
            .finish()
    }
}

// This is used for testing only
#[derive(Clone, Debug)]
pub struct InMemoryLog {
    collection_to_log: HashMap<CollectionUuid, Vec<InternalLogRecord>>,
    offsets: HashMap<CollectionUuid, i64>,
}

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog {
            collection_to_log: HashMap::new(),
            offsets: HashMap::new(),
        }
    }

    pub fn add_log(&mut self, collection_id: CollectionUuid, log: InternalLogRecord) {
        let logs = self.collection_to_log.entry(collection_id).or_default();
        // Ensure that the log offset is correct. Since we only use the InMemoryLog for testing,
        // we expect callers to send us logs in the correct order.
        let next_offset = logs.len() as i64;
        if log.log_offset != next_offset {
            panic!(
                "Expected log offset to be {}, but got {}",
                next_offset, log.log_offset
            );
        }
        logs.push(log);
    }
}

impl InMemoryLog {
    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Vec<LogRecord> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };

        let logs = match self.collection_to_log.get(&collection_id) {
            Some(logs) => logs,
            None => return Vec::new(),
        };
        let mut result = Vec::new();
        for i in offset..(offset + batch_size as i64) {
            if i < logs.len() as i64 && logs[i as usize].log_ts <= end_timestamp {
                result.push(logs[i as usize].record.clone());
            }
        }
        result
    }

    pub(super) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Vec<CollectionInfo> {
        let mut collections = Vec::new();
        for (collection_id, log_records) in self.collection_to_log.iter() {
            if log_records.is_empty() {
                continue;
            }
            let filtered_records = match self.offsets.get(collection_id) {
                Some(last_offset) => {
                    // Make sure there is at least one record past the last offset
                    let max_offset = log_records.len() as i64 - 1;
                    if *last_offset + 1 > max_offset {
                        continue;
                    }
                    &log_records[(*last_offset + 1) as usize..]
                }
                None => &log_records[..],
            };

            if (filtered_records.len() as u64) < min_compaction_size {
                continue;
            }

            let mut logs = filtered_records.to_vec();
            logs.sort_by(|a, b| a.log_offset.cmp(&b.log_offset));
            collections.push(CollectionInfo {
                collection_id: *collection_id,
                first_log_offset: logs[0].log_offset,
                first_log_ts: logs[0].log_ts,
            });
        }
        collections
    }

    pub(super) async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) {
        self.offsets.insert(collection_id, new_offset);
    }

    pub(super) async fn scout_logs(
        &mut self,
        collection_id: CollectionUuid,
        starting_offset: u64,
    ) -> Result<u64, Box<dyn ChromaError>> {
        let answer = self
            .collection_to_log
            .get(&collection_id)
            .iter()
            .flat_map(|x| x.iter().map(|rec| rec.log_offset + 1).max())
            .max()
            .unwrap_or(starting_offset as i64) as u64;
        if answer >= starting_offset {
            Ok(answer)
        } else {
            Ok(starting_offset)
        }
    }
}

impl Default for InMemoryLog {
    fn default() -> Self {
        Self::new()
    }
}
