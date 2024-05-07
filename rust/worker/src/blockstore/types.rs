use super::arrow::blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter};
use super::arrow::flusher::ArrowBlockfileFlusher;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::key::KeyWrapper;
use super::memory::reader_writer::{HashMapBlockfileReader, MemoryBlockfileWriter};
use super::memory::storage::{Readable, Writeable};
use crate::errors::{ChromaError, ErrorCodes};
use crate::segment::DataRecord;
use arrow::array::{Array, Int32Array};
use roaring::RoaringBitmap;
use std::collections::HashMap;
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
pub(crate) trait Key: PartialEq + Debug + Display + Into<KeyWrapper> + Clone {
    fn get_size(&self) -> usize;
}

impl Key for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Key for f32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Key for bool {
    fn get_size(&self) -> usize {
        1
    }
}

impl Key for u32 {
    fn get_size(&self) -> usize {
        4
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

// TODO: Maybe make writeable and readable traits'
// TODO: we don't need this get size
impl Value for Int32Array {
    fn get_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl Value for &Int32Array {
    fn get_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl Value for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for u32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Value for RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl Value for &RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl<'a> Value for DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

impl<'a> Value for &DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

#[derive(Clone)]
pub(crate) enum BlockfileWriter {
    MemoryBlockfileWriter(MemoryBlockfileWriter),
    ArrowBlockfileWriter(ArrowBlockfileWriter),
}

impl BlockfileWriter {
    pub(crate) fn commit<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        self,
    ) -> Result<BlockfileFlusher, Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => match writer.commit() {
                Ok(_) => Ok(BlockfileFlusher::MemoryBlockfileFlusher(())),
                Err(e) => Err(e),
            },
            BlockfileWriter::ArrowBlockfileWriter(writer) => match writer.commit::<K, V>() {
                Ok(flusher) => Ok(BlockfileFlusher::ArrowBlockfileFlusher(flusher)),
                Err(e) => Err(e),
            },
        }
    }

    pub(crate) async fn set<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.set(prefix, key, value),
            BlockfileWriter::ArrowBlockfileWriter(writer) => writer.set(prefix, key, value).await,
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowBlockfileWriter(writer) => writer.id(),
        }
    }
}

pub(crate) enum BlockfileFlusher {
    MemoryBlockfileFlusher(()),
    ArrowBlockfileFlusher(ArrowBlockfileFlusher),
}

impl BlockfileFlusher {
    pub(crate) async fn flush<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileFlusher::MemoryBlockfileFlusher(_) => Ok(()),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.flush::<K, V>().await,
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        match self {
            // TODO: should memory blockfiles have ids?
            BlockfileFlusher::MemoryBlockfileFlusher(_) => uuid::Uuid::nil(),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.id(),
        }
    }
}

pub(crate) enum BlockfileReader<
    'me,
    K: Key + ArrowReadableKey<'me>,
    V: Value + ArrowReadableValue<'me>,
> {
    MemoryBlockfileReader(HashMapBlockfileReader<K, V>),
    ArrowBlockfileReader(ArrowBlockfileReader<'me, K, V>),
}

impl<
        'referred_data,
        K: Key
            + Into<KeyWrapper>
            + From<&'referred_data KeyWrapper>
            + ArrowReadableKey<'referred_data>,
        V: Value + Readable<'referred_data> + ArrowReadableValue<'referred_data>,
    > BlockfileReader<'referred_data, K, V>
{
    pub(crate) async fn get(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<V, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get(prefix, key).await,
        }
    }

    // TODO: make prefix &str
    pub(crate) fn get_by_prefix(
        &'referred_data self,
        prefix: &'referred_data str,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_by_prefix(prefix),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_gt(
        &'referred_data self,
        prefix: &'referred_data str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_lt(
        &'referred_data self,
        prefix: &'referred_data str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_gte(
        &'referred_data self,
        prefix: &'referred_data str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_lte(
        &'referred_data self,
        prefix: &'referred_data str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.id(),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.id(),
        }
    }
}
