use super::{
    super::{BlockfileError, Key, Value},
    storage::{Readable, Storage, StorageBuilder, StorageManager, Writeable},
};
use crate::key::KeyWrapper;
use chroma_error::ChromaError;

#[derive(Clone)]
pub(crate) struct MemoryBlockfileWriter {
    builder: StorageBuilder,
    storage_manager: StorageManager,
    id: uuid::Uuid,
}

pub(crate) struct MemoryBlockfileFlusher {
    id: uuid::Uuid,
}

impl MemoryBlockfileFlusher {
    pub(crate) fn id(&self) -> uuid::Uuid {
        self.id
    }
}

impl MemoryBlockfileWriter {
    pub(super) fn new(storage_manager: StorageManager) -> Self {
        let builder = storage_manager.create();
        let id = builder.id;
        Self {
            builder,
            storage_manager,
            id,
        }
    }

    pub(crate) fn commit(&self) -> Result<MemoryBlockfileFlusher, Box<dyn ChromaError>> {
        self.storage_manager.commit(self.builder.id);
        Ok(MemoryBlockfileFlusher { id: self.id })
    }

    pub(crate) fn set<K: Key + Into<KeyWrapper>, V: Value + Writeable>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        let key = key.clone().into();
        V::write_to_storage(prefix, key, value, &self.builder);
        Ok(())
    }

    pub(crate) fn delete<K: Key + Into<KeyWrapper>, V: Value + Writeable>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        let key = key.into();
        V::remove_from_storage(prefix, key, &self.builder);
        Ok(())
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        self.id
    }
}

#[derive(Clone)]
pub struct MemoryBlockfileReader<K: Key, V: Value> {
    storage_manager: StorageManager,
    storage: Storage,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<
        'storage,
        K: Key + Into<KeyWrapper> + From<&'storage KeyWrapper>,
        V: Value + Readable<'storage>,
    > MemoryBlockfileReader<K, V>
{
    pub(crate) fn open(id: uuid::Uuid, storage_manager: StorageManager) -> Self {
        // TODO: don't unwrap
        let storage = storage_manager.get(id).unwrap();
        Self {
            storage_manager,
            storage,
            marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn get(&'storage self, prefix: &str, key: K) -> Result<V, Box<dyn ChromaError>> {
        let key = key.into();
        let value = V::read_from_storage(prefix, key, &self.storage);
        match value {
            Some(value) => Ok(value),
            None => Err(Box::new(BlockfileError::NotFoundError)),
        }
    }

    pub(crate) fn get_by_prefix(
        &'storage self,
        prefix: &str,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let values = V::get_by_prefix_from_storage(prefix, &self.storage);
        if values.is_empty() {
            return Err(Box::new(BlockfileError::NotFoundError));
        }
        let values = values
            .iter()
            .map(|(key, value)| (key.prefix.as_str(), K::from(&key.key), value.clone()))
            .collect();
        Ok(values)
    }

    pub(crate) fn get_gt(
        &'storage self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let key = key.into();
        let values = V::read_gt_from_storage(prefix, key, &self.storage);
        if values.is_empty() {
            return Err(Box::new(BlockfileError::NotFoundError));
        }
        let values = values
            .iter()
            .map(|(key, value)| (key.prefix.as_str(), K::from(&key.key), value.clone()))
            .collect();
        Ok(values)
    }

    pub(crate) fn get_lt(
        &'storage self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let key = key.into();
        let values = V::read_lt_from_storage(prefix, key, &self.storage);
        if values.is_empty() {
            return Err(Box::new(BlockfileError::NotFoundError));
        }
        let values = values
            .iter()
            .map(|(key, value)| (key.prefix.as_str(), K::from(&key.key), value.clone()))
            .collect();
        Ok(values)
    }

    pub(crate) fn get_gte(
        &'storage self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let key = key.into();
        let values = V::read_gte_from_storage(prefix, key, &self.storage);
        if values.is_empty() {
            return Err(Box::new(BlockfileError::NotFoundError));
        }
        let values = values
            .iter()
            .map(|(key, value)| (key.prefix.as_str(), K::from(&key.key), value.clone()))
            .collect();
        Ok(values)
    }

    pub(crate) fn get_lte(
        &'storage self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let key = key.into();
        let values = V::read_lte_from_storage(prefix, key, &self.storage);
        if values.is_empty() {
            return Err(Box::new(BlockfileError::NotFoundError));
        }
        let values = values
            .iter()
            .map(|(key, value)| (key.prefix.as_str(), K::from(&key.key), value.clone()))
            .collect();
        Ok(values)
    }

    pub(crate) fn get_at_index(
        &'storage self,
        index: usize,
    ) -> Result<(&str, K, V), Box<dyn ChromaError>> {
        let res = V::get_at_index(&self.storage, index);
        let (key, value) = match res {
            Some((key, value)) => (key, value),
            None => return Err(Box::new(BlockfileError::NotFoundError)),
        };
        Ok((key.prefix.as_str(), K::from(&key.key), value))
    }

    pub(crate) fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        V::count(&self.storage)
    }

    pub(crate) fn contains(&'storage self, prefix: &str, key: K) -> bool {
        V::contains(prefix, key.into(), &self.storage)
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        self.storage.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::{Chunk, DataRecord, LogRecord, Operation, OperationRecord};

    #[test]
    fn test_blockfile_string() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", "key1", "value1");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<&str, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", "key1").unwrap();
        assert_eq!(value, "value1");
    }

    #[test]
    fn test_string_key_rbm_value() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(3);
        let _ = writer.set("prefix", "bitmap1", &bitmap);
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<&str, roaring::RoaringBitmap> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", "bitmap1").unwrap();
        assert!(value.contains(1));
        assert!(value.contains(2));
        assert!(value.contains(3));
    }

    #[test]
    fn test_string_key_data_record_value() {
        // TODO: cleanup this test
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let id = uuid::Uuid::new_v4().to_string();
        let embedding = vec![1.0, 2.0, 3.0];
        let record = DataRecord {
            id: &id,
            embedding: &embedding,
            metadata: None,
            document: None,
        };

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
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
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
                document: None,
                metadata: None,
            })
            .collect::<Vec<_>>();
        let id = writer.id();
        let _ = writer.set("prefix", "key1", &record);
        for record in data_records {
            let _ = writer.set("prefix", record.id, &record);
        }

        writer.commit().unwrap();

        let reader: MemoryBlockfileReader<&str, DataRecord> =
            MemoryBlockfileReader::open(id, storage_manager);
        let record = reader.get("prefix", "embedding_id_1").unwrap();
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.embedding, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_bool_key() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", true, "value1");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<bool, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", true).unwrap();
        assert_eq!(value, "value1");
    }

    #[test]
    fn test_u32_key() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", 1).unwrap();
        assert_eq!(value, "value1");
    }

    #[test]
    fn test_float32_key() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", 1.0).unwrap();
        assert_eq!(value, "value1");
    }

    #[test]
    fn test_get_by_prefix() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", "key1", "value1");
        let _ = writer.set("prefix", "key2", "value2");
        let _ = writer.set("different_prefix", "key3", "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<&str, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_by_prefix("prefix").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|(prefix, key, value)| *prefix == "prefix"
            && *key == "key1"
            && *value == "value1"));
        assert!(values.iter().any(|(prefix, key, value)| *prefix == "prefix"
            && *key == "key2"
            && *value == "value2"));
    }

    #[test]
    fn test_get_gt_int_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 3);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_gt_int_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 0).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_gt_int_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 1).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_gt_float_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 3.0);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_gt_float_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 0.0).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_gt_float_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gt("prefix", 1.0).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_gte_int_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 4);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_gte_int_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 1).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_gte_int_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 2).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_gte_float_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 3.5);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_gte_float_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 0.5).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_gte_float_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_gte("prefix", 1.5).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_lt_int_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 1);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_lt_int_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 4).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_lt_int_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 3).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
    }

    #[test]
    fn test_get_lt_float_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 0.5);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_lt_float_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 3.5).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_lt_float_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lt("prefix", 2.5).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
    }

    #[test]
    fn test_get_lte_int_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 0);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_lte_int_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 3).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3 && *value == "value3"));
    }

    #[test]
    fn test_get_lte_int_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1, "value1");
        let _ = writer.set("prefix", 2, "value2");
        let _ = writer.set("prefix", 3, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<u32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 2).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2 && *value == "value2"));
    }

    #[test]
    fn test_get_lte_float_none_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 0.5);
        assert!(values.is_err());
    }

    #[test]
    fn test_get_lte_float_all_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 3.0).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 3.0 && *value == "value3"));
    }

    #[test]
    fn test_get_lte_float_some_returned() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", 1.0, "value1");
        let _ = writer.set("prefix", 2.0, "value2");
        let _ = writer.set("prefix", 3.0, "value3");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<f32, &str> =
            MemoryBlockfileReader::open(writer.id, storage_manager);
        let values = reader.get_lte("prefix", 2.0).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 1.0 && *value == "value1"));
        assert!(values
            .iter()
            .any(|(prefix, key, value)| *prefix == "prefix" && *key == 2.0 && *value == "value2"));
    }

    #[test]
    fn test_delete() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let id = writer.id;
        let _ = writer.set("prefix", "key1", "value1");
        let _ = writer.set("prefix", "key2", "value2");
        let _ = writer.set("different_prefix", "key3", "value3");
        // delete
        let _ = writer.delete::<&str, &str>("prefix", "key1");
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<&str, &str> =
            MemoryBlockfileReader::open(id, storage_manager.clone());
        let key_2 = reader.get("prefix", "key2").unwrap();
        assert_eq!(key_2, "value2");
        let key_3 = reader.get("different_prefix", "key3").unwrap();
        assert_eq!(key_3, "value3");

        let key_1 = reader.get("prefix", "key1");
        assert!(key_1.is_err());
    }

    #[tokio::test]
    async fn test_get_by_index() {
        let storage_manager = StorageManager::new();
        let writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let id = writer.id;

        let n = 2000;
        for i in 0..n {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            let _ = writer.set("prefix", key.as_str(), value.as_str());
        }
        let _ = writer.commit();

        let reader: MemoryBlockfileReader<&str, &str> =
            MemoryBlockfileReader::open(id, storage_manager.clone());
        for i in 0..n {
            let expected_key = format!("key{:04}", i);
            let expected_value = format!("value{:04}", i);
            let (prefix, key, value) = reader.get_at_index(i).unwrap();
            assert_eq!(prefix, "prefix");
            assert_eq!(key, expected_key.as_str());
            assert_eq!(value, expected_value.as_str());
        }
    }
}
