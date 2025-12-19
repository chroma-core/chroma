use super::{BlockfileFlusher, Key, Value};
use crate::arrow::blockfile::ArrowUnorderedBlockfileWriter;
use crate::arrow::ordered_blockfile_writer::ArrowOrderedBlockfileWriter;
use crate::arrow::types::{ArrowWriteableKey, ArrowWriteableValue};
use crate::dashmap::reader_writer::DashMapBlockfileWriter;
use crate::dashmap::storage::{PreparedValueFromStoredValue, ToStoredValue};
use crate::key::KeyWrapper;
use crate::memory::reader_writer::MemoryBlockfileWriter;
use crate::memory::storage::Writeable;
use chroma_error::ChromaError;

#[derive(Clone)]
pub enum BlockfileWriter {
    MemoryBlockfileWriter(MemoryBlockfileWriter),
    ArrowOrderedBlockfileWriter(ArrowOrderedBlockfileWriter),
    ArrowUnorderedBlockfileWriter(ArrowUnorderedBlockfileWriter),
    DashMapBlockfileWriter(DashMapBlockfileWriter),
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
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                match writer.commit::<K, V>().await {
                    Ok(flusher) => Ok(BlockfileFlusher::ArrowBlockfileFlusher(flusher)),
                    Err(e) => Err(e),
                }
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => {
                match writer.commit::<K, V>().await {
                    Ok(flusher) => Ok(BlockfileFlusher::ArrowBlockfileFlusher(flusher)),
                    Err(e) => Err(e),
                }
            }
            BlockfileWriter::DashMapBlockfileWriter(writer) => match writer.commit() {
                Ok(flusher) => Ok(BlockfileFlusher::DashMapBlockfileFlusher(flusher)),
                Err(e) => Err(e),
            },
        }
    }

    pub async fn set<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue + ToStoredValue,
    >(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.set(prefix, key, value),
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.set(prefix, key, value).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => {
                writer.set(prefix, key, value).await
            }
            BlockfileWriter::DashMapBlockfileWriter(writer) => writer.set(prefix, key, value),
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
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.delete::<K, V>(prefix, key).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => {
                writer.delete::<K, V>(prefix, key).await
            }
            BlockfileWriter::DashMapBlockfileWriter(writer) => writer.delete::<K, V>(prefix, key),
        }
    }

    pub async fn get_owned<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<Option<V::PreparedValue>, Box<dyn ChromaError>>
    where
        V::PreparedValue: PreparedValueFromStoredValue,
    {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(_) => todo!(),
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.get_owned::<K, V>(prefix, key).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(_) => todo!(),
            BlockfileWriter::DashMapBlockfileWriter(writer) => {
                writer.get_owned::<K, V::PreparedValue>(prefix, key)
            }
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::DashMapBlockfileWriter(writer) => writer.id(),
        }
    }
}
