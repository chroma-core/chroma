use super::arrow::blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter};
use super::arrow::flusher::ArrowBlockfileFlusher;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::key::KeyWrapper;
use super::memory::reader_writer::{
    MemoryBlockfileFlusher, MemoryBlockfileReader, MemoryBlockfileWriter,
};
use super::memory::storage::{Readable, Writeable};
use crate::blockstore::positional_posting_list_value::PositionalPostingList;
use crate::errors::{ChromaError, ErrorCodes};
use crate::segment::DataRecord;
use arrow::array::{Array, Int32Array};
use futures::{Stream, StreamExt};
use roaring::RoaringBitmap;
use std::fmt::{Debug, Display};
use std::pin::Pin;
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
    #[error("Block not found")]
    BlockNotFound,
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
            BlockfileError::BlockNotFound => ErrorCodes::Internal,
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

impl Value for PositionalPostingList {
    fn get_size(&self) -> usize {
        return self.size_in_bytes();
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
                Ok(flusher) => Ok(BlockfileFlusher::MemoryBlockfileFlusher(flusher)),
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

    pub(crate) async fn delete<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.delete::<K, V>(prefix, key),
            BlockfileWriter::ArrowBlockfileWriter(writer) => {
                writer.delete::<K, V>(prefix, key).await
            }
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
    MemoryBlockfileFlusher(MemoryBlockfileFlusher),
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
            BlockfileFlusher::MemoryBlockfileFlusher(flusher) => flusher.id(),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.id(),
        }
    }
}

pub(crate) enum BlockfileReader<
    'me,
    K: Key + ArrowReadableKey<'me>,
    V: Value + ArrowReadableValue<'me>,
> {
    MemoryBlockfileReader(MemoryBlockfileReader<K, V>),
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

    pub(crate) async fn contains(&'referred_data self, prefix: &str, key: K) -> bool {
        match self {
            BlockfileReader::ArrowBlockfileReader(reader) => reader.contains(prefix, key).await,
            BlockfileReader::MemoryBlockfileReader(reader) => reader.contains(prefix, key),
        }
    }

    pub(crate) async fn count(&'referred_data self) -> Result<usize, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.count(),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                let count = reader.count().await;
                match count {
                    Ok(c) => {
                        return Ok(c);
                    }
                    Err(_) => {
                        return Err(Box::new(BlockfileError::BlockNotFound));
                    }
                }
            }
        }
    }

    // TODO: make prefix &str
    pub(crate) fn get_by_prefix(
        &'referred_data self,
        prefix: &str,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_by_prefix(prefix),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_gt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_lt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_gte(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => todo!(),
        }
    }

    pub(crate) fn get_lte(
        &'referred_data self,
        prefix: &str,
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

    pub(crate) fn iter(
        &'referred_data self,
    ) -> Pin<Box<dyn Stream<Item = Result<(&'referred_data str, K, V), ()>> + 'referred_data>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.iter(),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.iter(),
        }
    }
}
