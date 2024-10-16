use super::{BlockfileError, Key, Value};
use crate::arrow::blockfile::ArrowBlockfileReader;
use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use crate::key::{InvalidKeyConversion, KeyWrapper};
use crate::memory::reader_writer::MemoryBlockfileReader;
use crate::memory::storage::Readable;
use chroma_error::ChromaError;
use futures::{Stream, StreamExt};
use std::ops::RangeBounds;

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
