use super::{
    reader_writer::{MemoryBlockfileReader, MemoryBlockfileWriter},
    storage::{Readable, StorageManager},
};
use crate::{
    arrow::types::{ArrowReadableKey, ArrowReadableValue},
    key::{KeyWrapper, InvalidKeyConversion},
    provider::{CreateError, OpenError},
    BlockfileReader, BlockfileWriter, Key, Value,
};

/// A BlockFileProvider that creates HashMapBlockfiles (in-memory blockfiles used for testing).
/// It bookkeeps the blockfiles locally.
/// # Note
/// This is not intended for production use.
#[derive(Clone)]
pub struct MemoryBlockfileProvider {
    storage_manager: StorageManager,
}

impl MemoryBlockfileProvider {
    pub(crate) fn new() -> Self {
        Self {
            storage_manager: StorageManager::new(),
        }
    }

    pub(crate) fn open<
        'new,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'new KeyWrapper, Error = InvalidKeyConversion>
            + ArrowReadableKey<'new>
            + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let reader = MemoryBlockfileReader::open(*id, self.storage_manager.clone());
        Ok(BlockfileReader::<K, V>::MemoryBlockfileReader(reader))
    }

    pub(crate) fn create(&self) -> Result<BlockfileWriter, Box<CreateError>> {
        let writer: MemoryBlockfileWriter =
            MemoryBlockfileWriter::new(self.storage_manager.clone());
        Ok(BlockfileWriter::MemoryBlockfileWriter(writer))
    }

    pub(crate) fn clear(&self) {
        self.storage_manager.clear();
    }

    pub(crate) fn fork(&self, _id: &uuid::Uuid) -> Result<BlockfileWriter, Box<CreateError>> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::{Chunk, DataRecord, LogRecord, Operation, OperationRecord};

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
                    document: None,
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
                    document: None,
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
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let _data_records = data
            .iter()
            .map(|record| DataRecord {
                id: &record.0.record.id,
                embedding: record.0.record.embedding.as_ref().unwrap(),
                document: None,
                metadata: None,
            })
            .collect::<Vec<_>>();

        let _provider = MemoryBlockfileProvider::new();
        // let mut writer = provider.create::<&str, DataRecord>().unwrap();
        // let id = writer.id();
        // for record in data_records {
        //     let res = writer.set("", &record.id, record);
        // }
        // let _ = writer.commit();

        // let reader = provider.open::<&str, DataRecord>(&id).unwrap();
        // let record = reader.get("", "embedding_id_1").unwrap();
        // assert_eq!(record.id, "embedding_id_1");
        // assert_eq!(record.embedding, &[7.0, 8.0, 9.0]);
    }
}
