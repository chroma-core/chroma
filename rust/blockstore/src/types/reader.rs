use crate::arrow::blockfile::ArrowBlockfileReader;
use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use crate::key::{InvalidKeyConversion, KeyWrapper};
use crate::memory::reader_writer::MemoryBlockfileReader;
use crate::memory::storage::Readable;
use chroma_error::ChromaError;

use super::{BlockfileError, Key, Value};

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

    pub async fn get_by_prefix(
        &'referred_data self,
        prefix: &str,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_by_prefix(prefix),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_by_prefix(prefix).await,
        }
    }

    pub async fn get_gt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_gt(prefix, key).await,
        }
    }

    pub async fn get_lt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_lt(prefix, key).await,
        }
    }

    pub async fn get_gte(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_gte(prefix, key).await,
        }
    }

    pub async fn get_lte(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_lte(prefix, key).await,
        }
    }

    pub async fn get_at_index(
        &'referred_data self,
        index: usize,
    ) -> Result<(&'referred_data str, K, V), Box<dyn ChromaError>> {
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
