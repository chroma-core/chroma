use crate::arrow::blockfile::ArrowBlockfileWriter;
use crate::key::KeyWrapper;
use crate::memory::reader_writer::MemoryBlockfileWriter;
use chroma_error::ChromaError;

use crate::arrow::types::{ArrowWriteableKey, ArrowWriteableValue};
use crate::memory::storage::Writeable;

use super::{BlockfileFlusher, Key, Value};

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
