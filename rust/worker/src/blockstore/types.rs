use super::memory::key::KeyWrapper;
use super::memory::reader_writer::{HashMapBlockfileReader, HashMapBlockfileWriter};
use super::memory::storage::{Readable, Writeable};
use crate::errors::{ChromaError, ErrorCodes};
use crate::segment::DataRecord;
use arrow::array::{Array, Int32Array};
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
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
}

impl ChromaError for BlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockfileError::NotFoundError
            | BlockfileError::InvalidKeyType
            | BlockfileError::InvalidValueType => ErrorCodes::InvalidArgument,
            BlockfileError::TransactionInProgress | BlockfileError::TransactionNotInProgress => {
                ErrorCodes::FailedPrecondition
            }
        }
    }
}

// ===== Key Types =====
#[derive(Clone)]
pub(crate) struct BlockfileKey<K: Key> {
    pub(crate) prefix: String,
    pub(crate) key: K,
}

pub(crate) trait Key: PartialEq + Eq + Debug + Display {
    fn get_size(&self) -> usize;
}

impl Key for String {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Key for bool {
    fn get_size(&self) -> usize {
        1
    }
}

impl<K: Key> BlockfileKey<K> {
    pub(super) fn get_size(&self) -> usize {
        self.get_prefix_size() + self.key.get_size()
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.prefix.len()
    }
}

impl<K: Key> BlockfileKey<K> {
    pub(crate) fn new(prefix: String, key: K) -> Self {
        BlockfileKey { prefix, key }
    }
}

impl<K: Key + Debug + Display> Debug for BlockfileKey<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlockfileKey(prefix: {}, key: {})",
            self.prefix, self.key
        )
    }
}

impl<K: Key> Hash for BlockfileKey<K> {
    // Hash is only used for the HashMap implementation, which is a test/reference implementation
    // Therefore this hash implementation is not used in production and allowed to be
    // hacky
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
    }
}

impl<K: Key> PartialEq for BlockfileKey<K> {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl<K: Key + PartialOrd> PartialOrd for BlockfileKey<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.prefix == other.prefix {
            self.key.partial_cmp(&other.key)
        } else {
            self.prefix.partial_cmp(&other.prefix)
        }
    }
}

impl<K: Key + Eq> Eq for BlockfileKey<K> {}

impl<K: Key + Ord> Ord for BlockfileKey<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.prefix == other.prefix {
            self.key.cmp(&other.key)
        } else {
            self.prefix.cmp(&other.prefix)
        }
    }
}

// ===== Value Types =====

// impl<'a> Clone for Value<'a> {
//     fn clone(&self) -> Self {
//         // TODO: make this correct for all types
//         match self {
//             Value::Int32ArrayValue(arr) => {
//                 // An arrow array, if nested in a larger structure, when cloned may clone the entire larger buffer.
//                 // This leads to a large memory overhead and also breaks our sizing assumptions. In order to work around this,
//                 // we have to manuallly create a new array and copy the data over.

//                 // Note that we use a vector here to avoid the overhead of the builder. The from() method for primitive
//                 // types uses unsafe code to wrap the vecs underlying buffer in an arrow array.

//                 // There are more performant ways to do this, but this is the most straightforward.
//                 let mut new_vec = Vec::with_capacity(arr.len());
//                 for i in 0..arr.len() {
//                     new_vec.push(arr.value(i));
//                 }
//                 let new_arr = Int32Array::from(new_vec);
//                 Value::Int32ArrayValue(new_arr)
//             }
//             Value::PositionalPostingListValue(list) => {
//                 Value::PositionalPostingListValue(list.clone())
//             }
//             Value::StringValue(s) => Value::StringValue(s.clone()),
//             Value::RoaringBitmapValue(bitmap) => Value::RoaringBitmapValue(bitmap.clone()),
//             Value::IntValue(i) => Value::IntValue(*i),
//             Value::UintValue(u) => Value::UintValue(*u),
//             Value::DataRecordValue(record) => Value::DataRecordValue(record.clone()),
//         }
//     }
// }

// impl Value<'_> {
//     pub(crate) fn get_size(&self) -> usize {
//         match self {
//             Value::Int32ArrayValue(arr) => arr.get_buffer_memory_size(),
//             Value::PositionalPostingListValue(list) => {
//                 unimplemented!("Size of positional posting list")
//             }
//             Value::StringValue(s) => s.len(),
//             Value::RoaringBitmapValue(bitmap) => bitmap.serialized_size(),
//             Value::IntValue(_) | Value::UintValue(_) => 4,
//             Value::DataRecordValue(record) => record.get_size(),
//         }
//     }
// }

pub(crate) trait Value: Clone {
    fn get_size(&self) -> usize;
}

impl Value for Int32Array {
    fn get_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl Value for String {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for &String {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl<'a> Value for DataRecord<'a> {
    fn get_size(&self) -> usize {
        self.get_size()
    }
}

pub(crate) enum BlockfileWriter<K: Key, V: Value> {
    HashMapBlockfileWriter(HashMapBlockfileWriter<K, V>),
}

impl<K: Key + Into<KeyWrapper>, V: Value + Writeable> BlockfileWriter<K, V> {
    pub(crate) fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::HashMapBlockfileWriter(writer) => writer.begin_transaction(),
        }
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::HashMapBlockfileWriter(writer) => writer.commit_transaction(),
        }
    }

    pub(crate) fn set(&self, prefix: &str, key: K, value: &V) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::HashMapBlockfileWriter(writer) => writer.set(prefix, key, value),
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::HashMapBlockfileWriter(writer) => writer.id(),
        }
    }
}

pub(crate) enum BlockfileReader<K: Key, V: Value> {
    HashMapBlockfileReader(HashMapBlockfileReader<K, V>),
}

impl<'referred_data, K: Key + Into<KeyWrapper>, V: Value + Readable<'referred_data>>
    BlockfileReader<K, V>
{
    pub(crate) fn get(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<V, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get(prefix, key),
        }
    }

    pub(crate) fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get_by_prefix(prefix),
        }
    }

    pub(crate) fn get_gt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get_gt(prefix, key),
        }
    }

    pub(crate) fn get_lt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get_lt(prefix, key),
        }
    }

    pub(crate) fn get_gte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get_gte(prefix, key),
        }
    }

    pub(crate) fn get_lte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.get_lte(prefix, key),
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileReader::HashMapBlockfileReader(reader) => reader.id(),
        }
    }
}
