use super::{
    super::{BlockfileError, Key, Value},
    storage::{Readable, Storage, StorageBuilder, StorageManager, Writeable},
};
use crate::{blockstore::key::KeyWrapper, errors::ChromaError};

pub(crate) struct MemoryBlockfileWriter<K: Key, V: Value> {
    builder: StorageBuilder,
    storage_manager: StorageManager,
    marker: std::marker::PhantomData<(K, V)>,
    id: uuid::Uuid,
}

impl<K: Key + Into<KeyWrapper>, V: Value + Writeable> MemoryBlockfileWriter<K, V> {
    pub(super) fn new(storage_manager: StorageManager) -> Self {
        let builder = storage_manager.create();
        let id = builder.id;
        Self {
            builder,
            storage_manager,
            marker: std::marker::PhantomData,
            id,
        }
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        Ok(())
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.storage_manager.commit(self.builder.id);
        Ok(())
    }

    pub(crate) fn set(&self, prefix: &str, key: K, value: &V) -> Result<(), Box<dyn ChromaError>> {
        let key = key.into();
        V::write_to_storage(prefix, key, value, &self.builder);
        Ok(())
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        self.id
    }
}

pub(crate) struct HashMapBlockfileReader<K: Key, V: Value> {
    storage_manager: StorageManager,
    storage: Storage,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<'storage, K: Key + Into<KeyWrapper>, V: Value + Readable<'storage>>
    HashMapBlockfileReader<K, V>
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
        &self,
        prefix: String,
    ) -> Result<Vec<(&str, &K, &V)>, Box<dyn ChromaError>> {
        todo!()
    }

    pub(crate) fn get_gt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&str, &K, &V)>, Box<dyn ChromaError>> {
        todo!()
    }

    pub(crate) fn get_lt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&str, &K, &V)>, Box<dyn ChromaError>> {
        todo!()
    }

    pub(crate) fn get_gte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&str, &K, &V)>, Box<dyn ChromaError>> {
        todo!()
    }

    pub(crate) fn get_lte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&str, &K, &V)>, Box<dyn ChromaError>> {
        todo!()
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        self.storage.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::data::data_chunk::Chunk;
    use crate::segment::DataRecord;
    use crate::types::{LogRecord, Operation, OperationRecord};

    #[test]
    fn test_blockfile_string() {
        let storage_manager = StorageManager::new();
        let mut writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let _ = writer.set("prefix", "key1".to_string(), &"value1".to_string());
        let _ = writer.commit_transaction();

        let reader: HashMapBlockfileReader<String, &String> =
            HashMapBlockfileReader::open(writer.id, storage_manager);
        let value = reader.get("prefix", "key1".to_string()).unwrap();
        assert_eq!(value, "value1");
    }

    #[test]
    fn test_data_record() {
        let storage_manager = StorageManager::new();
        let mut writer = MemoryBlockfileWriter::new(storage_manager.clone());
        let id = uuid::Uuid::new_v4().to_string();
        let embedding = vec![1.0, 2.0, 3.0];
        let record = DataRecord {
            id: &id,
            embedding: &embedding,
            metadata: &None,
            document: &None,
            serialized_metadata: None,
        };

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
        let id = writer.id();
        let _ = writer.set("prefix", "key1".to_string(), &record);
        for record in data_records {
            let _ = writer.set("prefix", record.id.to_owned(), &record);
        }

        writer.commit_transaction().unwrap();

        let reader: HashMapBlockfileReader<String, DataRecord> =
            HashMapBlockfileReader::open(id, storage_manager);
        let record = reader.get("prefix", "embedding_id_1".to_string()).unwrap();
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.embedding, vec![1.0, 2.0, 3.0]);
    }

    // #[test]
    // fn test_blockfile_set_get() {
    //     let mut blockfile_writer = HashMapBlockfileWriter::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: "key1".to_string(),
    //     };
    //     let _res = blockfile_writer
    //         .set(key.clone(), &Int32Array::from(vec![1, 2, 3]))
    //         .unwrap();

    //     let blockfile_reader = blockfile_writer.to_reader();
    //     let value = blockfile_reader.get(key).unwrap();
    //     assert_eq!(value, Int32Array::from(vec![1, 2, 3]));
    // }

    // #[test]
    // fn test_data_record_key() {
    //     let mut blockfile_writer = HashMapBlockfileWriter::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: "key1".to_string(),
    //     };
    //     let id = "id1".to_string();
    //     let embedding = vec![1.0, 2.0, 3.0];
    //     let mut metadata = HashMap::new();
    //     metadata.insert("key".to_string(), MetadataValue::Str("value".to_string()));
    //     let record = DataRecord {
    //         id: &id,
    //         embedding: &embedding,
    //         metadata: &Some(metadata),
    //         document: &None,
    //         serialized_metadata: None,
    //     };
    //     let _res = blockfile_writer.set(key.clone(), &record).unwrap();

    //     let blockfile_reader = blockfile_writer.to_reader();
    //     let value = blockfile_reader.get(key).unwrap();
    //     assert_eq!(value.id, "id1");
    //     assert_eq!(value.embedding, vec![1.0, 2.0, 3.0]);
    //     assert_eq!(value.metadata.as_ref().unwrap().len(), 1);
    //     assert_eq!(
    //         *value.metadata.as_ref().unwrap().get("key").unwrap(),
    //         MetadataValue::Str("value".to_string())
    //     );
    //     assert!(value.document.is_none());
    // }

    // #[test]
    // fn test_blockfile_get_by_prefix() {
    //     let mut blockfile = HashMapBlockfile::new();
    //     let key1 = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key1".to_string()),
    //     };
    //     let key2 = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key2".to_string()),
    //     };
    //     let _res = blockfile
    //         .set(
    //             key1.clone(),
    //             &Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3])),
    //         )
    //         .unwrap();
    //     let _res = blockfile
    //         .set(
    //             key2.clone(),
    //             &Value::Int32ArrayValue(Int32Array::from(vec![4, 5, 6])),
    //         )
    //         .unwrap();
    //     let values = blockfile.get_by_prefix("text_prefix".to_string()).unwrap();
    //     assert_eq!(values.len(), 2);
    //     // May return values in any order
    //     match &values[0].1 {
    //         Value::Int32ArrayValue(arr) => assert!(
    //             arr == &Int32Array::from(vec![1, 2, 3]) || arr == &Int32Array::from(vec![4, 5, 6])
    //         ),
    //         _ => panic!("Value is not a string"),
    //     }
    //     match &values[1].1 {
    //         Value::Int32ArrayValue(arr) => assert!(
    //             arr == &Int32Array::from(vec![1, 2, 3]) || arr == &Int32Array::from(vec![4, 5, 6])
    //         ),
    //         _ => panic!("Value is not a string"),
    //     }
    // }

    // #[test]
    // fn test_bool_key() {
    //     let mut blockfile = HashMapBlockfileWriter::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: true,
    //     };
    //     let _res = blockfile.set(key.clone(), &Int32Array::from(vec![1]));

    //     let blockfile = blockfile.to_reader();
    //     let value = blockfile.get(key).unwrap();
    //     assert_eq!(value, Int32Array::from(vec![1]));
    // }

    // #[test]
    // fn test_string_value() {
    //     let mut blockfile = HashMapBlockfileWriter::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: "key1".to_string(),
    //     };
    //     let _res = blockfile.set(key.clone(), &"value1".to_string());

    //     let blockfile = blockfile.to_reader();
    //     let value = blockfile.get(key).unwrap();
    //     assert_eq!(value, "value1".to_string());
    // }

    // #[test]
    // fn test_storing_arrow_in_blockfile() {
    //     let mut blockfile = HashMapBlockfile::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key1".to_string()),
    //     };
    //     let array = Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3]));
    //     let _res = blockfile.set(key.clone(), &array).unwrap();
    //     let value = blockfile.get(key).unwrap();
    //     match value {
    //         Value::Int32ArrayValue(arr) => assert_eq!(arr, Int32Array::from(vec![1, 2, 3])),
    //         _ => panic!("Value is not an arrow int32 array"),
    //     }
    // }

    // #[test]
    // fn test_blockfile_get_gt() {
    //     let mut blockfile = HashMapBlockfile::new();
    //     let key1 = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key1".to_string()),
    //     };
    //     let key2 = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key2".to_string()),
    //     };
    //     let key3 = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("key3".to_string()),
    //     };
    //     let _res = blockfile.set(
    //         key1.clone(),
    //         &Value::Int32ArrayValue(Int32Array::from(vec![1])),
    //     );
    //     let _res = blockfile.set(
    //         key2.clone(),
    //         &Value::Int32ArrayValue(Int32Array::from(vec![2])),
    //     );
    //     let _res = blockfile.set(
    //         key3.clone(),
    //         &Value::Int32ArrayValue(Int32Array::from(vec![3])),
    //     );
    //     let values = blockfile
    //         .get_gt("text_prefix".to_string(), Key::String("key1".to_string()))
    //         .unwrap();
    //     assert_eq!(values.len(), 2);
    //     match &values[0].0.key {
    //         Key::String(s) => assert!(s == "key2" || s == "key3"),
    //         _ => panic!("Key is not a string"),
    //     }
    //     match &values[1].0.key {
    //         Key::String(s) => assert!(s == "key2" || s == "key3"),
    //         _ => panic!("Key is not a string"),
    //     }
    // }

    // #[test]
    // fn test_learning_arrow_struct() {
    //     let mut builder = PositionalPostingListBuilder::new();
    //     let _res = builder.add_doc_id_and_positions(1, vec![0]);
    //     let _res = builder.add_doc_id_and_positions(2, vec![0, 1]);
    //     let _res = builder.add_doc_id_and_positions(3, vec![0, 1, 2]);
    //     let list_term_1 = builder.build();

    //     // Example of how to use the struct array, which is one value for a term
    //     let mut blockfile = HashMapBlockfile::new();
    //     let key = BlockfileKey {
    //         prefix: "text_prefix".to_string(),
    //         key: Key::String("term1".to_string()),
    //     };
    //     let _res = blockfile
    //         .set(key.clone(), &Value::PositionalPostingListValue(list_term_1))
    //         .unwrap();
    //     let posting_list = blockfile.get(key).unwrap();
    //     let posting_list = match posting_list {
    //         Value::PositionalPostingListValue(arr) => arr,
    //         _ => panic!("Value is not an arrow struct array"),
    //     };

    //     let ids = posting_list.get_doc_ids();
    //     let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
    //     // find index of target id
    //     let target_id = 2;

    //     // imagine this is binary search instead of linear
    //     for i in 0..ids.len() {
    //         if ids.is_null(i) {
    //             continue;
    //         }
    //         if ids.value(i) == target_id {
    //             let pos_list = posting_list.get_positions_for_doc_id(target_id).unwrap();
    //             let pos_list = pos_list.as_any().downcast_ref::<Int32Array>().unwrap();
    //             assert_eq!(pos_list.len(), 2);
    //             assert_eq!(pos_list.value(0), 0);
    //             assert_eq!(pos_list.value(1), 1);
    //             break;
    //         }
    //     }
    // }

    // #[test]
    // fn test_roaring_bitmap_example() {
    //     let mut bitmap = RoaringBitmap::new();
    //     bitmap.insert(1);
    //     bitmap.insert(2);
    //     bitmap.insert(3);
    //     let mut blockfile = HashMapBlockfile::new();
    //     let key = BlockfileKey::new(
    //         "text_prefix".to_string(),
    //         Key::String("bitmap1".to_string()),
    //     );
    //     let _res = blockfile
    //         .set(key.clone(), &Value::RoaringBitmapValue(bitmap))
    //         .unwrap();
    //     let value = blockfile.get(key).unwrap();
    //     match value {
    //         Value::RoaringBitmapValue(bitmap) => {
    //             assert!(bitmap.contains(1));
    //             assert!(bitmap.contains(2));
    //             assert!(bitmap.contains(3));
    //         }
    //         _ => panic!("Value is not a roaring bitmap"),
    //     }
    // }
}
