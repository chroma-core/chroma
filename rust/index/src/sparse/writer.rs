use std::collections::{BTreeMap, HashMap};

use chroma_blockstore::BlockfileWriter;
use chroma_error::ChromaError;
use thiserror::Error;

use crate::sparse::{
    reader::{SparseReader, SparseReaderError},
    types::encode_u32,
};

#[derive(Clone, Default)]
pub struct SparseDelta {
    dimension_value_updates: HashMap<u32, HashMap<u32, Option<f32>>>,
}

impl SparseDelta {
    pub fn create(&mut self, offset: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {
        for (dimension_id, value) in sparse_vector {
            self.dimension_value_updates
                .entry(dimension_id)
                .or_default()
                .entry(offset)
                .insert_entry(Some(value));
        }
    }

    pub fn delete(&mut self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        for dimension_id in sparse_indices {
            self.dimension_value_updates
                .entry(dimension_id)
                .or_default()
                .entry(offset)
                .insert_entry(None);
        }
    }
}

#[derive(Debug, Error)]
pub enum SparseWriterError {
    #[error(transparent)]
    Blockfile(#[from] Box<dyn ChromaError>),
    #[error(transparent)]
    Reader(#[from] SparseReaderError),
}
pub struct SparseWriter<'me> {
    block_size: u32,
    max_writer: BlockfileWriter,
    offset_value_writer: BlockfileWriter,
    reader: Option<SparseReader<'me>>,
}

impl<'me> SparseWriter<'me> {
    pub async fn write_delta(&self, delta: SparseDelta) -> Result<(), SparseWriterError> {
        for (dimension_id, updates) in delta.dimension_value_updates {
            let encoded_dimension = encode_u32(dimension_id);
            let blocks = match self.reader.as_ref() {
                Some(reader) => reader.get_blocks(dimension_id).await?,
                None => Vec::new(),
            }
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        }
        Ok(())
    }
}
