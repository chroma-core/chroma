use crate::arrow::flusher::ArrowBlockfileFlusher;
use crate::arrow::types::{ArrowWriteableKey, ArrowWriteableValue};
use crate::key::KeyWrapper;
use crate::memory::reader_writer::MemoryBlockfileFlusher;
use crate::memory::storage::Writeable;
use chroma_error::ChromaError;

use super::{Key, Value};

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
