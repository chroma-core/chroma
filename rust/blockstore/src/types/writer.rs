use super::{BlockfileFlusher, Key, Value};
use crate::arrow::blockfile::ArrowUnorderedBlockfileWriter;
use crate::arrow::ordered_blockfile_writer::ArrowOrderedBlockfileWriter;
use crate::arrow::types::{ArrowWriteableKey, ArrowWriteableValue};
use crate::key::KeyWrapper;
use crate::memory::reader_writer::MemoryBlockfileWriter;
use crate::memory::storage::Writeable;
use chroma_error::ChromaError;

#[derive(Clone)]
pub enum BlockfileWriter {
    MemoryBlockfileWriter(MemoryBlockfileWriter),
    ArrowOrderedBlockfileWriter(ArrowOrderedBlockfileWriter),
    ArrowUnorderedBlockfileWriter(ArrowUnorderedBlockfileWriter),
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
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.set(prefix, key, value).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => {
                writer.set(prefix, key, value).await
            }
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
        }
    }

    pub async fn get_owned<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<Option<V::PreparedValue>, Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(_) => todo!(),
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.get_owned::<K, V>(prefix, key).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(_) => todo!(),
        }
    }

    /// Get a value from the blockfile without copying.
    /// The closure `f` is called with a reference to the stored value if it exists.
    /// This allows zero-copy access to stored data.
    ///
    /// Note: This method currently only supports ArrowUnorderedBlockfileWriter.
    /// For other writer types, use `get_owned` instead.
    pub async fn get<K, V, R>(
        &self,
        prefix: &str,
        key: K,
        f: impl FnOnce(Option<&V>) -> R,
    ) -> Result<R, Box<dyn ChromaError>>
    where
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(_) => {
                todo!("get() not implemented for MemoryBlockfileWriter")
            }
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => {
                writer.get::<K, V, R>(prefix, key, f).await
            }
            BlockfileWriter::ArrowOrderedBlockfileWriter(_) => {
                todo!("get() not implemented for ArrowOrderedBlockfileWriter")
            }
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => writer.id(),
        }
    }
}
