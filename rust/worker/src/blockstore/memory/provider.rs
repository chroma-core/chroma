use super::{
    reader_writer::{HashMapBlockfileReader, MemoryBlockfileWriter},
    storage::{Readable, StorageManager, Writeable},
};
use crate::blockstore::{
    key::KeyWrapper,
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileReader, BlockfileWriter, Key, Value,
};

/// A BlockFileProvider that creates HashMapBlockfiles (in-memory blockfiles used for testing).
/// It bookkeeps the blockfiles locally.
/// # Note
/// This is not intended for production use.
pub(crate) struct HashMapBlockfileProvider {
    storage_manager: StorageManager,
}

impl BlockfileProvider for HashMapBlockfileProvider {
    fn new() -> Self {
        Self {
            storage_manager: StorageManager::new(),
        }
    }

    fn open<'new, K: Key + Into<KeyWrapper> + 'new, V: Value + Readable<'new> + 'new>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<K, V>, Box<OpenError>> {
        let reader = HashMapBlockfileReader::open(*id, self.storage_manager.clone());
        Ok(BlockfileReader::<K, V>::HashMapBlockfileReader(reader))
    }

    fn create<'new, K: Key + Into<KeyWrapper> + 'new, V: Value + Writeable + 'new>(
        &self,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>> {
        let writer: MemoryBlockfileWriter<K, V> =
            MemoryBlockfileWriter::new(self.storage_manager.clone());
        Ok(BlockfileWriter::<K, V>::HashMapBlockfileWriter(writer))
    }

    fn fork<K: Key, V: Value>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        execution::data::data_chunk::Chunk,
        segment::DataRecord,
        types::{LogRecord, Operation, OperationRecord},
    };

    use super::*;

    #[test]
    fn test_data_record() {
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(vec![4.0, 5.0, 6.0]),
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let data_records = data
            .iter()
            .map(|record| DataRecord {
                id: &record.0.record.id,
                embedding: record.0.record.embedding.as_ref().unwrap(),
                document: &None,
                metadata: &None,
                serialized_metadata: None,
            })
            .collect::<Vec<_>>();

        let provider = HashMapBlockfileProvider::new();
        let mut writer = provider.create::<String, DataRecord>().unwrap();
        let id = writer.id();
        for record in data_records {
            let res = writer.set("", record.id.to_owned(), &record);
        }
        let _ = writer.commit_transaction();

        let reader = provider.open::<String, DataRecord>(&id).unwrap();
        let record = reader.get("", "embedding_id_1".to_string()).unwrap();
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.embedding, &[7.0, 8.0, 9.0]);
    }
}
