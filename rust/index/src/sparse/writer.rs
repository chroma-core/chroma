use std::collections::{BTreeMap, HashMap};

use chroma_blockstore::{BlockfileFlusher, BlockfileWriter};
use chroma_error::ChromaError;
use thiserror::Error;

use crate::sparse::{
    reader::{SparseReader, SparseReaderError},
    types::{encode_u32, DIMENSION_PREFIX},
};

#[derive(Default)]
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

pub struct SparseFlusher {
    max_flusher: BlockfileFlusher,
    offset_value_flusher: BlockfileFlusher,
}

impl SparseFlusher {
    pub async fn flush(self) -> Result<(), SparseWriterError> {
        self.max_flusher.flush::<u32, f32>().await?;
        self.offset_value_flusher.flush::<u32, f32>().await?;
        Ok(())
    }
}

pub struct SparseWriter<'me> {
    block_size: u32,
    max_writer: BlockfileWriter,
    offset_value_writer: BlockfileWriter,
    reader: Option<SparseReader<'me>>,
}

impl<'me> SparseWriter<'me> {
    pub fn new(
        block_size: u32,
        max_writer: BlockfileWriter,
        offset_value_writer: BlockfileWriter,
        reader: Option<SparseReader<'me>>,
    ) -> Self {
        Self {
            block_size,
            max_writer,
            offset_value_writer,
            reader,
        }
    }

    pub async fn commit(self) -> Result<SparseFlusher, SparseWriterError> {
        Ok(SparseFlusher {
            max_flusher: self.max_writer.commit::<u32, f32>().await?,
            offset_value_flusher: self.offset_value_writer.commit::<u32, f32>().await?,
        })
    }

    pub async fn write_delta(&self, delta: SparseDelta) -> Result<(), SparseWriterError> {
        for (dimension_id, updates) in delta.dimension_value_updates {
            let encoded_dimension = encode_u32(dimension_id);
            let (commited_blocks, mut offset_values) = match self.reader.as_ref() {
                Some(reader) => {
                    let blocks = reader.get_blocks(&encoded_dimension).await?.collect();
                    let offset_values = reader
                        .get_offset_values(&encoded_dimension, ..)
                        .await?
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
            if offset_value_vec.is_empty() {
                self.max_writer
                    .delete::<_, f32>(DIMENSION_PREFIX, dimension_id)
                    .await?;
            } else {
                let mut dimension_max = f32::MIN;
                for block in offset_value_vec.chunks(self.block_size as usize) {
                    let (min_offset, max_value) = block.iter().fold(
                        (u32::MAX, f32::MIN),
                        |(min_offset, max_value), (offset, value)| {
                            (min_offset.min(*offset), max_value.max(*value))
                        },
                    );
                    dimension_max = dimension_max.max(max_value);
                    self.max_writer
                        .set(&encoded_dimension, min_offset, max_value)
                        .await?;
                }
                self.max_writer
                    .set(DIMENSION_PREFIX, dimension_id, dimension_max)
                    .await?;
            }
        }
        Ok(())
    }
}
