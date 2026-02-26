use std::{collections::HashMap, sync::Arc};

use chroma_blockstore::{BlockfileFlusher, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use dashmap::DashMap;
use thiserror::Error;
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
    // NOTE: `delta` hold all writes to the writer until commit
    // Structure: dimension_id -> offset_id -> delete/upsert value
    delta: Arc<DashMap<u32, DashMap<u32, Option<f32>>>>,
    max_writer: BlockfileWriter,
    offset_value_writer: BlockfileWriter,
    old_reader: Option<SparseReader<'me>>,
}

impl<'me> SparseWriter<'me> {
    pub fn new(
        block_size: u32,
        max_writer: BlockfileWriter,
        offset_value_writer: BlockfileWriter,
        old_reader: Option<SparseReader<'me>>,
    ) -> Self {
        Self {
            block_size,
            delta: Default::default(),
            max_writer,
            offset_value_writer,
            old_reader,
        }
    }

    pub async fn set(&self, offset: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {
        for (dimension_id, value) in sparse_vector {
            self.delta
                .entry(dimension_id)
                .or_default()
                .insert(offset, Some(value));
        }
    }

    pub async fn delete(&self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        for dimension_id in sparse_indices {
            self.delta
                .entry(dimension_id)
                .or_default()
                .insert(offset, None);
        }
    }

    pub async fn commit(self) -> Result<SparseFlusher, SparseWriterError> {
        // Sort dimension by encoding so that we process them in order
        let mut encoded_dimensions = self
            .delta
            .iter()
            .map(|entry| {
                let dimension_id = *entry.key();
                (encode_u32(dimension_id), dimension_id)
            })
            .collect::<Vec<_>>();
        encoded_dimensions.push((DIMENSION_PREFIX.to_string(), u32::MAX));
        encoded_dimensions.sort_unstable();

        let mut block_maxes = HashMap::with_capacity(encoded_dimensions.len());
        let mut dimension_maxes = match self.old_reader.as_ref() {
            Some(reader) => reader.get_dimension_max().await?,
            None => HashMap::new(),
        };

        for (encoded_dimension, dimension_id) in &encoded_dimensions {
            if encoded_dimension == DIMENSION_PREFIX {
                continue;
            }

            let Some((_, offset_updates)) = self.delta.remove(dimension_id) else {
                continue;
            };
            let mut offset_update_vec = offset_updates.into_iter().collect::<Vec<_>>();
            offset_update_vec.sort_unstable_by_key(|(offset, _)| *offset);

            let mut offset_values = match self.old_reader.as_ref() {
                Some(reader) => reader.get_offset_values(encoded_dimension).await?.collect(),
                None => HashMap::new(),
            };

            for (offset, update) in offset_update_vec {
                match update {
                    Some(value) => {
                        self.offset_value_writer
                            .set(encoded_dimension, offset, value)
                            .await?;
                        offset_values.insert(offset, value);
                    }
                    None => {
                        self.offset_value_writer
                            .delete::<_, f32>(encoded_dimension, offset)
                            .await?;
                        offset_values.remove(&offset);
                    }
                }
            }

            if offset_values.is_empty() {
                dimension_maxes.remove(dimension_id);
            } else {
                let mut block_max =
                    Vec::with_capacity(offset_values.len() / self.block_size as usize);
                let mut dimension_max = f32::MIN;
                let mut offset_value_vec = offset_values.into_iter().collect::<Vec<_>>();
                offset_value_vec.sort_unstable_by_key(|(offset, _)| *offset);

                for block in offset_value_vec.chunks(self.block_size as usize) {
                    let (max_offset, max_value) = block.iter().fold(
                        (u32::MIN, f32::MIN),
                        |(max_offset, max_value), (offset, value)| {
                            (max_offset.max(*offset), max_value.max(*value))
                        },
                    );
                    block_max.push((max_offset + 1, max_value));
                    dimension_max = dimension_max.max(max_value);
                }

                block_maxes.insert(*dimension_id, block_max);
                dimension_maxes.insert(*dimension_id, dimension_max);
            }
        }

        for (encoded_dimension, dimension_id) in encoded_dimensions {
            if encoded_dimension == DIMENSION_PREFIX {
                let mut dimension_max_vec = dimension_maxes.drain().collect::<Vec<_>>();
                dimension_max_vec.sort_unstable_by_key(|(dimension_id, _)| *dimension_id);
                for (dimension_max_id, value) in dimension_max_vec {
                    self.max_writer
                        .set(DIMENSION_PREFIX, dimension_max_id, value)
                        .await?;
                }
                continue;
            }

            let Some(block_max) = block_maxes.remove(&dimension_id) else {
                continue;
            };
            for (offset, value) in block_max {
                self.max_writer
                    .set(&encoded_dimension, offset, value)
                    .await?;
            }
        }

        Ok(SparseFlusher {
            max_flusher: self.max_writer.commit::<u32, f32>().await?,
            offset_value_flusher: self.offset_value_writer.commit::<u32, f32>().await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparse::reader::SparseReader;
    use chroma_blockstore::{
        arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider,
        BlockfileWriterOptions,
    };

    #[tokio::test]
    async fn test_writer_crud_operations() {
        let (_temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

        // Setup writers
        let max_writer = provider
            .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()).ordered_mutations())
            .await
            .unwrap();
        let offset_value_writer = provider
            .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()).ordered_mutations())
            .await
            .unwrap();

        let writer = SparseWriter::new(64, max_writer, offset_value_writer, None);

        // CREATE: Add multiple vectors including edge cases
        // Normal vector
        writer.set(0, vec![(1, 0.5), (5, 0.8), (10, 0.3)]).await;

        // Empty vector (edge case)
        writer.set(1, vec![]).await;

        // Single dimension vector (edge case)
        writer.set(2, vec![(100, 1.0)]).await;

        // Large dimension IDs (edge case)
        writer
            .set(3, vec![(u32::MAX - 1, 0.1), (u32::MAX, 0.9)])
            .await;

        // Vector with zero values (edge case)
        writer.set(4, vec![(20, 0.0), (21, 0.5)]).await;

        // Dense vector spanning multiple blocks
        let dense_vector: Vec<(u32, f32)> = (0..200).map(|i| (i, 0.1 * (i as f32))).collect();
        writer.set(5, dense_vector.clone()).await;

        // UPDATE: Overwrite existing vector
        writer.set(0, vec![(1, 0.9), (2, 0.7)]).await;

        // DELETE: Remove a vector
        writer.delete(2, vec![100]).await;

        // Commit and verify blockfile contents
        let flusher = Box::pin(writer.commit()).await.unwrap();
        let max_id = flusher.max_id();
        let offset_value_id = flusher.offset_value_id();
        Box::pin(flusher.flush()).await.unwrap();

        // Create readers to verify final state
        let max_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(max_id, "".to_string()))
            .await
            .unwrap();
        let offset_value_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(offset_value_id, "".to_string()))
            .await
            .unwrap();

        // Verify vector at offset 0 was updated
        let dim1_encoded = encode_u32(1);
        assert_eq!(
            offset_value_reader.get(&dim1_encoded, 0).await.unwrap(),
            Some(0.9)
        );
        let dim2_encoded = encode_u32(2);
        assert_eq!(
            offset_value_reader.get(&dim2_encoded, 0).await.unwrap(),
            Some(0.7)
        );
        // Dimension 5 should still exist in offset 0 (not overwritten)
        let dim5_encoded = encode_u32(5);
        assert_eq!(
            offset_value_reader.get(&dim5_encoded, 0).await.unwrap(),
            Some(0.8)
        );
        // Dimension 10 should still exist in offset 0 (not overwritten)
        let dim10_encoded = encode_u32(10);
        assert_eq!(
            offset_value_reader.get(&dim10_encoded, 0).await.unwrap(),
            Some(0.3)
        );

        // Verify vector at offset 2 was deleted
        let dim100_encoded = encode_u32(100);
        assert_eq!(
            offset_value_reader.get(&dim100_encoded, 2).await.unwrap(),
            None
        );

        // Verify empty vector at offset 1 (should have no entries)
        // Empty vectors don't create any entries in the blockfiles

        // Verify large dimension IDs work correctly
        let dim_max_minus_1_encoded = encode_u32(u32::MAX - 1);
        let dim_max_encoded = encode_u32(u32::MAX);
        assert_eq!(
            offset_value_reader
                .get(&dim_max_minus_1_encoded, 3)
                .await
                .unwrap(),
            Some(0.1)
        );
        assert_eq!(
            offset_value_reader.get(&dim_max_encoded, 3).await.unwrap(),
            Some(0.9)
        );

        // Verify zero values are stored
        let dim20_encoded = encode_u32(20);
        let dim21_encoded = encode_u32(21);
        assert_eq!(
            offset_value_reader.get(&dim20_encoded, 4).await.unwrap(),
            Some(0.0)
        );
        assert_eq!(
            offset_value_reader.get(&dim21_encoded, 4).await.unwrap(),
            Some(0.5)
        );

        // Verify dimension max values are correct
        assert!(max_reader.get(DIMENSION_PREFIX, 1).await.unwrap().unwrap() > 0.0);
        assert!(max_reader.get(DIMENSION_PREFIX, 2).await.unwrap().unwrap() > 0.0);

        // Verify block max values exist for dimensions with data
        // For the dense vector (offset 5), check dimension 50
        // Since we only have one vector at offset 5 for dimension 50, the block max key would be offset+1 = 6
        let dim50_encoded = encode_u32(50);
        let block_max_value = max_reader
            .get(&dim50_encoded, 6) // offset 5 + 1
            .await
            .unwrap();
        assert!(
            block_max_value.is_some(),
            "Block max should exist for dimension 50"
        );
        // The value should be 0.1 * 50 = 5.0
        assert_eq!(block_max_value.unwrap(), 5.0);

        // Verify dimension 50 exists at offset 5 in the original data
        let dim50_encoded = encode_u32(50);
        assert_eq!(
            offset_value_reader.get(&dim50_encoded, 5).await.unwrap(),
            Some(5.0), // 0.1 * 50
            "Dimension 50 should exist at offset 5 in original data"
        );

        // Test with old_reader (incremental update)
        // Max writer is not forked (fully rewritten each commit).
        // Offset-value writer is forked so old entries are preserved.
        let max_writer2 = provider
            .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()).ordered_mutations())
            .await
            .unwrap();
        let offset_value_writer2 = provider
            .write::<u32, f32>(
                BlockfileWriterOptions::new("".to_string())
                    .ordered_mutations()
                    .fork(offset_value_id),
            )
            .await
            .unwrap();

        let reader = SparseReader::new(max_reader, offset_value_reader);
        let writer2 = SparseWriter::new(64, max_writer2, offset_value_writer2, Some(reader));

        // Add new vectors
        writer2.set(10, vec![(1, 0.2), (50, 0.6)]).await;
        writer2.set(11, vec![(1, 0.3), (100, 0.7)]).await;

        let flusher2 = Box::pin(writer2.commit()).await.unwrap();
        let max_id2 = flusher2.max_id();
        let offset_value_id2 = flusher2.offset_value_id();
        Box::pin(flusher2.flush()).await.unwrap();

        let final_max_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(max_id2, "".to_string()))
            .await
            .unwrap();
        let final_offset_value_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(
                offset_value_id2,
                "".to_string(),
            ))
            .await
            .unwrap();

        // New values should be present
        assert_eq!(
            final_offset_value_reader
                .get(&dim1_encoded, 10)
                .await
                .unwrap(),
            Some(0.2),
            "New value for dimension 1 at offset 10"
        );
        assert_eq!(
            final_offset_value_reader
                .get(&dim1_encoded, 11)
                .await
                .unwrap(),
            Some(0.3),
            "New value for dimension 1 at offset 11"
        );
        assert_eq!(
            final_offset_value_reader
                .get(&dim50_encoded, 10)
                .await
                .unwrap(),
            Some(0.6),
            "New value for dimension 50 at offset 10"
        );
        assert_eq!(
            final_offset_value_reader
                .get(&dim100_encoded, 11)
                .await
                .unwrap(),
            Some(0.7),
            "New value for dimension 100 at offset 11"
        );

        // Old values should still be accessible (offset-value writer was forked)
        assert_eq!(
            final_offset_value_reader
                .get(&dim1_encoded, 0)
                .await
                .unwrap(),
            Some(0.9),
            "Old value for dimension 1 at offset 0 preserved via fork"
        );
        assert_eq!(
            final_offset_value_reader
                .get(&dim50_encoded, 5)
                .await
                .unwrap(),
            Some(5.0),
            "Old value for dimension 50 at offset 5 preserved via fork"
        );

        // Max blockfile should reflect merged state (old + new dimensions)
        // Dimension 1 has offsets 0 (0.9), 10 (0.2), 11 (0.3) -> dimension max = 0.9
        assert_eq!(
            final_max_reader.get(DIMENSION_PREFIX, 1).await.unwrap(),
            Some(0.9),
            "Dimension 1 max should reflect all offsets including old"
        );
        // Dimension 50 has offsets 5 (5.0), 10 (0.6) -> dimension max = 5.0
        assert_eq!(
            final_max_reader.get(DIMENSION_PREFIX, 50).await.unwrap(),
            Some(5.0),
            "Dimension 50 max should reflect all offsets including old"
        );
    }
}
