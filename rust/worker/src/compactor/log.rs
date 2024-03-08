use crate::types::EmbeddingRecord;
use std::collections::HashMap;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct CollectionRecord {
    pub(crate) id: String,
    pub(crate) tenant_id: Option<String>,
    pub(crate) last_compaction_time: Option<i64>,
    pub(crate) first_record_time: Option<i64>,
    pub(crate) cursor: Option<i64>,
}

pub(crate) trait Log {
    fn write(&mut self, msg: EmbeddingRecord);
    fn read(
        &self,
        collection_id: String,
        index: usize,
        batch_size: usize,
    ) -> Option<Vec<EmbeddingRecord>>;
    fn get_collections(&self) -> Vec<CollectionRecord>;
}

pub struct InMemoryLog {
    logs: HashMap<String, Vec<EmbeddingRecord>>,
}

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog {
            logs: HashMap::new(),
        }
    }
}

impl Log for InMemoryLog {
    fn write(&mut self, msg: EmbeddingRecord) {
        let collection_id = msg.collection_id.to_string();
        if !self.logs.contains_key(collection_id.as_str()) {
            self.logs.insert(collection_id.clone(), Vec::new());
        }
        self.logs.get_mut(collection_id.as_str()).unwrap().push(msg);
    }

    fn read(
        &self,
        collection_id: String,
        index: usize,
        batch_size: usize,
    ) -> Option<Vec<EmbeddingRecord>> {
        if !self.logs.contains_key(collection_id.as_str()) {
            return None;
        }
        let logs = self.logs.get(collection_id.as_str()).unwrap();
        let start = index;
        if start >= logs.len() {
            return None;
        }
        let mut end = index + batch_size;
        if end > logs.len() {
            end = logs.len();
        }
        let mut result = Vec::new();
        for i in start..end {
            result.push(logs[i].clone());
        }
        Some(result)
    }

    fn get_collections(&self) -> Vec<CollectionRecord> {
        let mut collections = Vec::new();
        for collection_id in self.logs.keys() {
            collections.push(CollectionRecord {
                id: collection_id.clone(),
                tenant_id: None,
                last_compaction_time: None,
                first_record_time: None,
                cursor: None,
            });
        }
        collections
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EmbeddingRecord;
    use crate::types::Operation;
    use num_bigint::BigInt;
    use uuid::Uuid;

    #[test]
    fn test_in_memory_simple_log() {
        let mut log = InMemoryLog::new();
        let embedding_record_1 = EmbeddingRecord {
            id: "id_1".to_string(),
            seq_id: BigInt::from(42),
            embedding: Some(vec![1.0, 2.0, 3.0]),
            encoding: None,
            metadata: None,
            operation: Operation::Add,
            collection_id: Uuid::new_v4(),
        };
        log.write(embedding_record_1.clone());
        let embedding_record_2 = EmbeddingRecord {
            id: "id_2".to_string(),
            seq_id: BigInt::from(43),
            embedding: Some(vec![1.0, 2.0, 3.0]),
            encoding: None,
            metadata: None,
            operation: Operation::Add,
            collection_id: Uuid::new_v4(),
        };
        log.write(embedding_record_2.clone());
        let records = log.read(embedding_record_1.collection_id.to_string(), 0, 100);
        assert_eq!(records.unwrap().len(), 1);
        let records = log.read(embedding_record_2.collection_id.to_string(), 0, 100);
        assert_eq!(records.unwrap().len(), 1);

        let collections = log.get_collections();
        assert_eq!(collections.len(), 2);
        // TODO: assert_eq!(collections[0].id, embedding_record_1.collection_id.to_string());
        assert_eq!(collections[0].tenant_id, None);
        assert_eq!(collections[0].last_compaction_time, None);
        assert_eq!(collections[0].first_record_time, None);
        assert_eq!(collections[0].cursor, None);

        assert_eq!(collections[1].tenant_id, None);
        assert_eq!(collections[1].last_compaction_time, None);
        assert_eq!(collections[1].first_record_time, None);
        assert_eq!(collections[1].cursor, None);
    }
}
