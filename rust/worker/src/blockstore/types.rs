use super::positional_posting_list_value::PositionalPostingList;
use crate::chroma_proto;
use crate::errors::{ChromaError, ErrorCodes};
use crate::types::LogRecord;
use arrow::array::{Array, Int32Array};
use parking_lot::RwLock;
use prost::Message;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum BlockfileError {
    #[error("Key not found")]
    NotFoundError,
    #[error("Invalid Key Type")]
    InvalidKeyType,
    #[error("Invalid Value Type")]
    InvalidValueType,
    #[error("Transaction already in progress")]
    TransactionInProgress,
    #[error("Transaction not in progress")]
    TransactionNotInProgress,
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send>),
}

impl ChromaError for BlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockfileError::NotFoundError => ErrorCodes::NotFound,
            BlockfileError::InvalidKeyType | BlockfileError::InvalidValueType => {
                ErrorCodes::InvalidArgument
            }
            BlockfileError::TransactionInProgress | BlockfileError::TransactionNotInProgress => {
                ErrorCodes::FailedPrecondition
            }
            BlockfileError::Other(_) => ErrorCodes::Internal,
        }
    }
}

// ===== Key Types =====
#[derive(Clone)]
pub(crate) struct BlockfileKey {
    pub(crate) prefix: String,
    pub(crate) key: Key,
}

impl Key {
    pub(crate) fn get_size(&self) -> usize {
        match self {
            Key::String(s) => s.len(),
            Key::Float(_) => 4,
            Key::Bool(_) => 1,
            Key::Uint(_) => 4,
        }
    }
}

impl BlockfileKey {
    pub(super) fn get_size(&self) -> usize {
        self.get_prefix_size() + self.key.get_size()
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.prefix.len()
    }
}

impl From<&BlockfileKey> for KeyType {
    fn from(key: &BlockfileKey) -> Self {
        match key.key {
            Key::String(_) => KeyType::String,
            Key::Float(_) => KeyType::Float,
            Key::Bool(_) => KeyType::Bool,
            Key::Uint(_) => KeyType::Uint,
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub(crate) enum Key {
    String(String),
    Float(f32),
    Bool(bool),
    Uint(u32),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum KeyType {
    String,
    Float,
    Bool,
    Uint,
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::String(s) => write!(f, "{}", s),
            Key::Float(fl) => write!(f, "{}", fl),
            Key::Bool(b) => write!(f, "{}", b),
            Key::Uint(u) => write!(f, "{}", u),
        }
    }
}

impl BlockfileKey {
    pub(crate) fn new(prefix: String, key: Key) -> Self {
        BlockfileKey { prefix, key }
    }
}

impl Debug for BlockfileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlockfileKey(prefix: {}, key: {})",
            self.prefix, self.key
        )
    }
}

impl Hash for BlockfileKey {
    // Hash is only used for the HashMap implementation, which is a test/reference implementation
    // Therefore this hash implementation is not used in production and allowed to be
    // hacky
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
    }
}

impl PartialEq for BlockfileKey {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl PartialOrd for BlockfileKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.prefix == other.prefix {
            self.key.partial_cmp(&other.key)
        } else {
            self.prefix.partial_cmp(&other.prefix)
        }
    }
}

impl Eq for BlockfileKey {}

impl Ord for BlockfileKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.prefix == other.prefix {
            match self.key {
                Key::String(ref s1) => match &other.key {
                    Key::String(s2) => s1.cmp(s2),
                    _ => panic!("Cannot compare string to float, bool, or uint"),
                },
                Key::Float(f1) => match &other.key {
                    Key::Float(f2) => f1.partial_cmp(f2).unwrap(),
                    _ => panic!("Cannot compare float to string, bool, or uint"),
                },
                Key::Bool(b1) => match &other.key {
                    Key::Bool(b2) => b1.cmp(b2),
                    _ => panic!("Cannot compare bool to string, float, or uint"),
                },
                Key::Uint(u1) => match &other.key {
                    Key::Uint(u2) => u1.cmp(u2),
                    _ => panic!("Cannot compare uint to string, float, or bool"),
                },
            }
        } else {
            self.prefix.cmp(&other.prefix)
        }
    }
}

// ===== Value Types =====

#[derive(Debug)]
pub(crate) enum Value {
    Int32ArrayValue(Int32Array),
    PositionalPostingListValue(PositionalPostingList),
    StringValue(String),
    IntValue(i32),
    UintValue(u32),
    RoaringBitmapValue(RoaringBitmap),
    EmbeddingRecordValue(EmbeddingRecord),
}

impl Clone for Value {
    fn clone(&self) -> Self {
        // TODO: make this correct for all types
        match self {
            Value::Int32ArrayValue(arr) => {
                // An arrow array, if nested in a larger structure, when cloned may clone the entire larger buffer.
                // This leads to a large memory overhead and also breaks our sizing assumptions. In order to work around this,
                // we have to manuallly create a new array and copy the data over.

                // Note that we use a vector here to avoid the overhead of the builder. The from() method for primitive
                // types uses unsafe code to wrap the vecs underlying buffer in an arrow array.

                // There are more performant ways to do this, but this is the most straightforward.
                let mut new_vec = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    new_vec.push(arr.value(i));
                }
                let new_arr = Int32Array::from(new_vec);
                Value::Int32ArrayValue(new_arr)
            }
            Value::PositionalPostingListValue(list) => {
                Value::PositionalPostingListValue(list.clone())
            }
            Value::EmbeddingRecordValue(record) => Value::EmbeddingRecordValue(record.clone()),
            Value::StringValue(s) => Value::StringValue(s.clone()),
            Value::RoaringBitmapValue(bitmap) => Value::RoaringBitmapValue(bitmap.clone()),
            Value::IntValue(i) => Value::IntValue(*i),
            Value::UintValue(u) => Value::UintValue(*u),
        }
    }
}

impl Value {
    pub(crate) fn get_size(&self) -> usize {
        match self {
            Value::Int32ArrayValue(arr) => arr.get_buffer_memory_size(),
            Value::PositionalPostingListValue(list) => {
                unimplemented!("Size of positional posting list")
            }
            Value::EmbeddingRecordValue(record) => {
                let user_id_size = record.id.len();
                let embedding_size = match &record.embedding {
                    Some(embedding) => embedding.len(),
                    None => 0,
                };
                let metadata_size = match &record.metadata {
                    Some(metadata) => {
                        let as_proto: chroma_proto::UpdateMetadata = metadata.clone().into();
                        as_proto.encoded_len()
                    }
                    None => 0,
                };
                let document_size = match record.get_document() {
                    Some(document) => document.len(),
                    None => 0,
                };
                // user_id_size + embedding_size + metadata_size + document_size
                // just uid metadata and document for now
                user_id_size + metadata_size + document_size
            }
            Value::StringValue(s) => s.len(),
            Value::RoaringBitmapValue(bitmap) => bitmap.serialized_size(),
            Value::IntValue(_) | Value::UintValue(_) => 4,
        }
    }
}

impl From<&Value> for ValueType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Int32ArrayValue(_) => ValueType::Int32Array,
            Value::PositionalPostingListValue(_) => ValueType::PositionalPostingList,
            Value::RoaringBitmapValue(_) => ValueType::RoaringBitmap,
            Value::EmbeddingRecordValue(_) => ValueType::EmbeddingRecord,
            Value::StringValue(_) => ValueType::String,
            Value::IntValue(_) => ValueType::Int,
            Value::UintValue(_) => ValueType::Uint,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ValueType {
    Int32Array,
    PositionalPostingList,
    EmbeddingRecord,
    RoaringBitmap,
    String,
    Int,
    Uint,
}

pub(crate) trait Blockfile: BlockfileClone {
    // ===== Transaction methods =====
    fn begin_transaction(&mut self) -> Result<(), Box<BlockfileError>>;

    fn commit_transaction(&mut self) -> Result<(), Box<BlockfileError>>;

    // ===== Data methods =====
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<BlockfileError>>;
    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>>;

    fn set(&mut self, key: BlockfileKey, value: Value) -> Result<(), Box<BlockfileError>>;
    fn delete(&mut self, key: BlockfileKey) -> Result<(), Box<BlockfileError>>;

    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>>;

    fn get_lt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>>;

    fn get_gte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>>;

    fn get_lte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>>;
}

pub(crate) trait BlockfileClone {
    fn clone_box(&self) -> Box<dyn Blockfile>;
}

impl<T> BlockfileClone for T
where
    T: 'static + Blockfile + Clone,
{
    fn clone_box(&self) -> Box<dyn Blockfile> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Blockfile> {
    fn clone(&self) -> Box<dyn Blockfile> {
        self.clone_box()
    }
}

#[derive(Clone)]
pub(crate) struct HashMapBlockfile {
    map: Arc<RwLock<HashMap<BlockfileKey, Value>>>,
}

impl HashMapBlockfile {
    pub(super) fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Blockfile for HashMapBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<BlockfileError>> {
        match self.map.read().get(&key) {
            Some(value) => Ok(value.clone()),
            None => Err(Box::new(BlockfileError::NotFoundError)),
        }
    }

    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>> {
        let mut result = Vec::new();
        for (key, value) in self.map.read().iter() {
            if key.prefix == prefix {
                result.push((key.clone(), value.clone()));
            }
        }
        Ok(result)
    }

    fn set(&mut self, key: BlockfileKey, value: Value) -> Result<(), Box<BlockfileError>> {
        self.map.write().insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: BlockfileKey) -> Result<(), Box<BlockfileError>> {
        match self.map.write().remove(&key) {
            Some(_) => Ok(()),
            None => Err(Box::new(BlockfileError::NotFoundError)),
        }
    }

    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.read().iter() {
            if k.prefix == prefix && k.key > key {
                result.push((k.clone(), v.clone()));
            }
        }
        Ok(result)
    }

    fn get_gte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.read().iter() {
            if k.prefix == prefix && k.key >= key {
                result.push((k.clone(), v.clone()));
            }
        }
        Ok(result)
    }

    fn get_lt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.read().iter() {
            if k.prefix == prefix && k.key < key {
                result.push((k.clone(), v.clone()));
            }
        }
        Ok(result)
    }

    fn get_lte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<BlockfileError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.read().iter() {
            if k.prefix == prefix && k.key <= key {
                result.push((k.clone(), v.clone()));
            }
        }
        Ok(result)
    }

    fn begin_transaction(&mut self) -> Result<(), Box<BlockfileError>> {
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<BlockfileError>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::positional_posting_list_value::PositionalPostingListBuilder;
    use arrow::array::Array;

    #[test]
    fn test_blockfile_set_get() {
        let mut blockfile = HashMapBlockfile::new();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let _res = blockfile
            .set(
                key.clone(),
                Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3])),
            )
            .unwrap();
        let value = blockfile.get(key);
        // downcast to string
        match value.unwrap() {
            Value::Int32ArrayValue(arr) => assert_eq!(arr, Int32Array::from(vec![1, 2, 3])),
            _ => panic!("Value is not a string"),
        }
    }

    #[test]
    fn test_blockfile_get_by_prefix() {
        let mut blockfile = HashMapBlockfile::new();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key2".to_string()),
        };
        let _res = blockfile
            .set(
                key1.clone(),
                Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3])),
            )
            .unwrap();
        let _res = blockfile
            .set(
                key2.clone(),
                Value::Int32ArrayValue(Int32Array::from(vec![4, 5, 6])),
            )
            .unwrap();
        let values = blockfile.get_by_prefix("text_prefix".to_string()).unwrap();
        assert_eq!(values.len(), 2);
        // May return values in any order
        match &values[0].1 {
            Value::Int32ArrayValue(arr) => assert!(
                arr == &Int32Array::from(vec![1, 2, 3]) || arr == &Int32Array::from(vec![4, 5, 6])
            ),
            _ => panic!("Value is not a string"),
        }
        match &values[1].1 {
            Value::Int32ArrayValue(arr) => assert!(
                arr == &Int32Array::from(vec![1, 2, 3]) || arr == &Int32Array::from(vec![4, 5, 6])
            ),
            _ => panic!("Value is not a string"),
        }
    }

    #[test]
    fn test_bool_key() {
        let mut blockfile = HashMapBlockfile::new();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::Bool(true),
        };
        let _res = blockfile.set(
            key.clone(),
            Value::Int32ArrayValue(Int32Array::from(vec![1])),
        );
        let value = blockfile.get(key).unwrap();
        match value {
            Value::Int32ArrayValue(arr) => assert_eq!(arr, Int32Array::from(vec![1])),
            _ => panic!("Value is not an arrow int32 array"),
        }
    }

    #[test]
    fn test_storing_arrow_in_blockfile() {
        let mut blockfile = HashMapBlockfile::new();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let array = Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3]));
        let _res = blockfile.set(key.clone(), array).unwrap();
        let value = blockfile.get(key).unwrap();
        match value {
            Value::Int32ArrayValue(arr) => assert_eq!(arr, Int32Array::from(vec![1, 2, 3])),
            _ => panic!("Value is not an arrow int32 array"),
        }
    }

    #[test]
    fn test_blockfile_get_gt() {
        let mut blockfile = HashMapBlockfile::new();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key2".to_string()),
        };
        let key3 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key3".to_string()),
        };
        let _res = blockfile.set(
            key1.clone(),
            Value::Int32ArrayValue(Int32Array::from(vec![1])),
        );
        let _res = blockfile.set(
            key2.clone(),
            Value::Int32ArrayValue(Int32Array::from(vec![2])),
        );
        let _res = blockfile.set(
            key3.clone(),
            Value::Int32ArrayValue(Int32Array::from(vec![3])),
        );
        let values = blockfile
            .get_gt("text_prefix".to_string(), Key::String("key1".to_string()))
            .unwrap();
        assert_eq!(values.len(), 2);
        match &values[0].0.key {
            Key::String(s) => assert!(s == "key2" || s == "key3"),
            _ => panic!("Key is not a string"),
        }
        match &values[1].0.key {
            Key::String(s) => assert!(s == "key2" || s == "key3"),
            _ => panic!("Key is not a string"),
        }
    }

    #[test]
    fn test_learning_arrow_struct() {
        let mut builder = PositionalPostingListBuilder::new();
        let _res = builder.add_doc_id_and_positions(1, vec![0]);
        let _res = builder.add_doc_id_and_positions(2, vec![0, 1]);
        let _res = builder.add_doc_id_and_positions(3, vec![0, 1, 2]);
        let list_term_1 = builder.build();

        // Example of how to use the struct array, which is one value for a term
        let mut blockfile = HashMapBlockfile::new();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("term1".to_string()),
        };
        let _res = blockfile
            .set(key.clone(), Value::PositionalPostingListValue(list_term_1))
            .unwrap();
        let posting_list = blockfile.get(key).unwrap();
        let posting_list = match posting_list {
            Value::PositionalPostingListValue(arr) => arr,
            _ => panic!("Value is not an arrow struct array"),
        };

        let ids = posting_list.get_doc_ids();
        let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        // find index of target id
        let target_id = 2;

        // imagine this is binary search instead of linear
        for i in 0..ids.len() {
            if ids.is_null(i) {
                continue;
            }
            if ids.value(i) == target_id {
                let pos_list = posting_list.get_positions_for_doc_id(target_id).unwrap();
                let pos_list = pos_list.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(pos_list.len(), 2);
                assert_eq!(pos_list.value(0), 0);
                assert_eq!(pos_list.value(1), 1);
                break;
            }
        }
    }

    #[test]
    fn test_roaring_bitmap_example() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(3);
        let mut blockfile = HashMapBlockfile::new();
        let key = BlockfileKey::new(
            "text_prefix".to_string(),
            Key::String("bitmap1".to_string()),
        );
        let _res = blockfile
            .set(key.clone(), Value::RoaringBitmapValue(bitmap))
            .unwrap();
        let value = blockfile.get(key).unwrap();
        match value {
            Value::RoaringBitmapValue(bitmap) => {
                assert!(bitmap.contains(1));
                assert!(bitmap.contains(2));
                assert!(bitmap.contains(3));
            }
            _ => panic!("Value is not a roaring bitmap"),
        }
    }
}
