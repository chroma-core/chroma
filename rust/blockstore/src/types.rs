use super::arrow::blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter};
use super::arrow::flusher::ArrowBlockfileFlusher;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::key::{InvalidKeyConversion, KeyWrapper};
use super::memory::reader_writer::{
    MemoryBlockfileFlusher, MemoryBlockfileReader, MemoryBlockfileWriter,
};
use super::memory::storage::{Readable, Writeable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::DataRecord;
use futures::{Stream, StreamExt};
use roaring::RoaringBitmap;
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::ops::RangeBounds;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlockfileError {
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
pub trait Key: PartialEq + Debug + Display + Into<KeyWrapper> + Clone {
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

pub trait Value: Clone {
    fn get_size(&self) -> usize;
}

impl Value for Vec<u32> {
    fn get_size(&self) -> usize {
        self.len() * size_of::<u32>()
    }
}

impl Value for &[u32] {
    fn get_size(&self) -> usize {
        std::mem::size_of_val(*self)
    }
}

impl Value for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for String {
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
pub enum BlockfileWriter {
    MemoryBlockfileWriter(MemoryBlockfileWriter),
    ArrowBlockfileWriter(ArrowBlockfileWriter),
}

impl BlockfileWriter {
    pub async fn commit<
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
            BlockfileWriter::ArrowBlockfileWriter(writer) => match writer.commit::<K, V>().await {
                Ok(flusher) => Ok(BlockfileFlusher::ArrowBlockfileFlusher(flusher)),
                Err(e) => Err(e),
            },
        }
    }

    pub async fn set<
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

    pub async fn delete<
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

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowBlockfileWriter(writer) => writer.id(),
        }
    }
}

pub enum BlockfileFlusher {
    MemoryBlockfileFlusher(MemoryBlockfileFlusher),
    ArrowBlockfileFlusher(ArrowBlockfileFlusher),
}

impl BlockfileFlusher {
    pub async fn flush<
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

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileFlusher::MemoryBlockfileFlusher(flusher) => flusher.id(),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.id(),
        }
    }
}

#[derive(Clone)]
pub enum BlockfileReader<
    'me,
    K: Key + Into<KeyWrapper> + ArrowReadableKey<'me>,
    V: Value + ArrowReadableValue<'me>,
> {
    MemoryBlockfileReader(MemoryBlockfileReader<K, V>),
    ArrowBlockfileReader(ArrowBlockfileReader<'me, K, V>),
}

impl<
        'referred_data,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'referred_data KeyWrapper, Error = InvalidKeyConversion>
            + ArrowReadableKey<'referred_data>,
        V: Value + Readable<'referred_data> + ArrowReadableValue<'referred_data>,
    > BlockfileReader<'referred_data, K, V>
{
    pub async fn get(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<V, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get(prefix, key).await,
        }
    }

    pub async fn contains(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<bool, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::ArrowBlockfileReader(reader) => reader.contains(prefix, key).await,
            BlockfileReader::MemoryBlockfileReader(reader) => Ok(reader.contains(prefix, key)),
        }
    }

    pub async fn count(&'referred_data self) -> Result<usize, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.count(),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                let count = reader.count().await;
                match count {
                    Ok(c) => Ok(c),
                    Err(_) => Err(Box::new(BlockfileError::BlockNotFound)),
                }
            }
        }
    }

    pub fn get_range_stream<'prefix, PrefixRange, KeyRange>(
        &'referred_data self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> impl Stream<Item = Result<(K, V), Box<dyn ChromaError>>> + 'referred_data + Send
    where
        PrefixRange: RangeBounds<&'prefix str> + Clone + Send + 'referred_data,
        KeyRange: RangeBounds<K> + Clone + Send + 'referred_data,
        K: Sync + Send,
        V: Sync + Send,
    {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => {
                match reader.get_range_iter(prefix_range, key_range) {
                    Ok(r) => futures::stream::iter(r.map(Ok)).boxed(),
                    Err(e) => futures::stream::iter(vec![Err(e)]).boxed(),
                }
            }

            BlockfileReader::ArrowBlockfileReader(reader) => {
                reader.get_range_stream(prefix_range, key_range).boxed()
            }
        }
    }

    pub async fn get_range<'prefix, PrefixRange, KeyRange>(
        &'referred_data self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>>
    where
        PrefixRange: RangeBounds<&'prefix str> + Clone + 'referred_data,
        KeyRange: RangeBounds<K> + Clone + 'referred_data,
    {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader
                .get_range_iter(prefix_range, key_range)
                .map(|i| i.collect()),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                reader.get_range(prefix_range, key_range).await
            }
        }
    }

    pub async fn get_at_index(
        &'referred_data self,
        index: usize,
    ) -> Result<(&str, K, V), Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_at_index(index),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_at_index(index).await,
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.id(),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.id(),
        }
    }

    pub async fn load_blocks_for_keys(&self, prefixes: &[&str], keys: &[K]) {
        match self {
            BlockfileReader::MemoryBlockfileReader(_reader) => unimplemented!(),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                reader.load_blocks_for_keys(prefixes, keys).await
            }
        }
    }
}
