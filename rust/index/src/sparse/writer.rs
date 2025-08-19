use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use chroma_blockstore::{BlockfileFlusher, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::sparse::{
    reader::{SparseReader, SparseReaderError},
    types::{encode_u32, DIMENSION_PREFIX},
};

#[derive(Debug, Error)]
pub enum SparseWriterError {
    #[error(transparent)]
    Blockfile(#[from] Box<dyn ChromaError>),
    #[error(transparent)]
    Reader(#[from] SparseReaderError),
}

impl ChromaError for SparseWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            SparseWriterError::Blockfile(err) => err.code(),
            SparseWriterError::Reader(err) => err.code(),
        }
    }
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

    pub fn max_id(&self) -> Uuid {
        self.max_flusher.id()
    }

    pub fn offset_value_id(&self) -> Uuid {
        self.offset_value_flusher.id()
    }
}

#[derive(Clone)]
pub struct SparseWriter<'me> {
    block_size: u32,
    #[allow(clippy::type_complexity)]
    delta: Arc<Mutex<HashMap<u32, HashMap<u32, Option<f32>>>>>,
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
            delta: Default::default(),
            max_writer,
            offset_value_writer,
            reader,
        }
    }

    pub async fn set(&self, offset: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {
        let mut delta_guard = self.delta.lock().await;
        for (dimension_id, value) in sparse_vector {
            delta_guard
                .entry(dimension_id)
                .or_default()
                .insert(offset, Some(value));
        }
    }

    pub async fn delete(&self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        let mut delta_guard = self.delta.lock().await;
        for dimension_id in sparse_indices {
            delta_guard
                .entry(dimension_id)
                .or_default()
                .insert(offset, None);
        }
    }

    pub async fn commit(self) -> Result<SparseFlusher, SparseWriterError> {
        let mut delta_guard = self.delta.lock().await;
        for (dimension_id, updates) in delta_guard.drain() {
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
                    let (max_offset, max_value) = block.iter().fold(
                        (u32::MIN, f32::MIN),
                        |(max_offset, max_value), (offset, value)| {
                            (max_offset.max(*offset), max_value.max(*value))
                        },
                    );
                    dimension_max = dimension_max.max(max_value);
                    self.max_writer
                        .set(&encoded_dimension, max_offset + 1, max_value)
                        .await?;
                }
                self.max_writer
                    .set(DIMENSION_PREFIX, dimension_id, dimension_max)
                    .await?;
            }
        }
        Ok(SparseFlusher {
            max_flusher: self.max_writer.commit::<u32, f32>().await?,
            offset_value_flusher: self.offset_value_writer.commit::<u32, f32>().await?,
        })
    }
}
