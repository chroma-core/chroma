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
                .insert(offset, Some(value));
        }
    }

    pub fn delete(&mut self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        for dimension_id in sparse_indices {
            self.dimension_value_updates
                .entry(dimension_id)
                .or_default()
                .insert(offset, None);
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
            let (commited_blocks, mut offset_values) = match self.reader.as_ref() {
                Some(reader) => {
                    let blocks = reader.get_blocks(dimension_id).await?.into_iter().collect();
                    let offset_values = reader
                        .get_offset_values(dimension_id, ..)
                        .await?
                        .into_iter()
                        .collect();
                    (blocks, offset_values)
                }
                None => (HashMap::new(), BTreeMap::new()),
            };
            for &offset in commited_blocks.keys() {
                self.max_writer
                    .delete::<_, f32>(&encoded_dimension, offset)
                    .await?;
            }
            for (offset, update) in updates {
                match update {
                    Some(value) => {
                        self.offset_value_writer
                            .set(&encoded_dimension, offset, value)
                            .await?;
                        offset_values.insert(offset, value);
                    }
                    None => {
                        self.offset_value_writer
                            .delete::<_, f32>(&encoded_dimension, offset)
                            .await?;
                        offset_values.remove(&offset);
                    }
                };
            }
            let offset_value_vec = offset_values.into_iter().collect::<Vec<_>>();
            for block in offset_value_vec.chunks(self.block_size as usize) {
                let (min_offset, max_value) = block.iter().fold(
                    (u32::MAX, f32::MIN),
                    |(min_offset, max_value), (offset, value)| {
                        (min_offset.min(*offset), max_value.max(*value))
                    },
                );
                self.max_writer
                    .set(&encoded_dimension, min_offset, max_value)
                    .await?;
            }
        }
        Ok(())
    }
}
