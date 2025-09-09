use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fmt,
    ops::RangeBounds,
};

use chroma_blockstore::BlockfileReader;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::SignedRoaringBitmap;
use thiserror::Error;

use crate::sparse::types::{encode_u32, DIMENSION_PREFIX};

struct Cursor<B, D> {
    block_iterator: B,
    block_next_offset: u32,
    block_upper_bound: f32,
    dimension_iterator: D,
    dimension_upper_bound: f32,
    offset: u32,
    query: f32,
    value: f32,
}

impl<B, D> fmt::Debug for Cursor<B, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor")
            .field("block_next_offset", &self.block_next_offset)
            .field("block_upper_bound", &self.block_upper_bound)
            .field("dimension_upper_bound", &self.dimension_upper_bound)
            .field("offset", &self.offset)
            .field("query", &self.query)
            .field("value", &self.value)
            .finish()
    }
}

impl<B, D> Eq for Cursor<B, D> {}

impl<B, D> Ord for Cursor<B, D> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl<B, D> PartialEq for Cursor<B, D> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl<B, D> PartialOrd for Cursor<B, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, PartialEq)]
pub struct Score {
    pub score: f32,
    pub offset: u32,
}

impl Eq for Score {}

// Reverse order by score for a min heap
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then(self.offset.cmp(&other.offset))
            .reverse()
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error)]
pub enum SparseReaderError {
    #[error(transparent)]
    Blockfile(#[from] Box<dyn ChromaError>),
}

impl ChromaError for SparseReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            SparseReaderError::Blockfile(err) => err.code(),
        }
    }
}

#[derive(Clone)]
pub struct SparseReader<'me> {
    max_reader: BlockfileReader<'me, u32, f32>,
    offset_value_reader: BlockfileReader<'me, u32, f32>,
}

impl<'me> SparseReader<'me> {
    pub fn new(
        max_reader: BlockfileReader<'me, u32, f32>,
        offset_value_reader: BlockfileReader<'me, u32, f32>,
    ) -> Self {
        Self {
            max_reader,
            offset_value_reader,
        }
    }

    pub async fn get_dimension_max(&self) -> Result<HashMap<u32, f32>, SparseReaderError> {
        Ok(self
            .max_reader
            .get_range(DIMENSION_PREFIX..=DIMENSION_PREFIX, ..)
            .await?
            .map(|(_, dimension_id, max)| (dimension_id, max))
            .collect())
    }

    pub async fn get_blocks(
        &'me self,
        encoded_dimension_id: &'me str,
    ) -> Result<impl Iterator<Item = (u32, f32)> + 'me, SparseReaderError> {
        Ok(self
            .max_reader
            .get_range(encoded_dimension_id..=encoded_dimension_id, ..)
            .await?
            .map(|(_, dimension_id, max)| (dimension_id, max)))
    }

    pub async fn get_offset_values(
        &'me self,
        encoded_dimension_id: &'me str,
        offset_range: impl RangeBounds<u32> + Clone + Send + 'me,
    ) -> Result<impl Iterator<Item = (u32, f32)> + 'me, SparseReaderError> {
        Ok(self
            .offset_value_reader
            .get_range(encoded_dimension_id..=encoded_dimension_id, offset_range)
            .await?
            .map(|(_, offset, value)| (offset, value)))
    }

    pub async fn wand(
        &self,
        query_vector: impl IntoIterator<Item = (u32, f32)>,
        k: u32,
        mask: SignedRoaringBitmap,
    ) -> Result<Vec<Score>, SparseReaderError> {
        let collected_query = query_vector
            .into_iter()
            .map(|(dimension_id, query)| (dimension_id, encode_u32(dimension_id), query))
            .collect::<Vec<_>>();
        let dimension_count = collected_query.len();
        let all_dimension_max = self.get_dimension_max().await?;

        let mut cursors = Vec::with_capacity(dimension_count);
        for (dimension_id, encoded_dimension_id, query) in &collected_query {
            let Some(dimension_max) = all_dimension_max.get(dimension_id) else {
                continue;
            };
            let mut dimension_iterator = self.get_offset_values(encoded_dimension_id, ..).await?;
            let Some((offset, value)) = dimension_iterator
                .by_ref()
                .find(|&(offset, _)| mask.contains(offset))
            else {
                continue;
            };
            let mut block_iterator = self.get_blocks(encoded_dimension_id).await?.peekable();
            let Some((block_next_offset, block_max)) = block_iterator
                .by_ref()
                .find(|&(block_next_offset, _)| offset < block_next_offset)
            else {
                continue;
            };
            cursors.push(Cursor {
                block_iterator,
                block_next_offset,
                block_upper_bound: query * block_max,
                dimension_iterator,
                dimension_upper_bound: query * dimension_max,
                offset,
                query: *query,
                value,
            })
        }

        cursors.sort_unstable();

        let Some(mut first_unchecked_offset) = cursors.first().map(|cursor| cursor.offset) else {
            return Ok(Vec::new());
        };

        let mut threshold = f32::MIN;
        let mut top_scores = BinaryHeap::with_capacity(k as usize);

        loop {
            let mut accumulated_dimension_upper_bound = 0.0;
            let mut following_cursor_offset = u32::MAX;
            let mut pivot_cursor_index = None;

            for (cursor_index, cursor) in cursors.iter().enumerate() {
                if pivot_cursor_index.is_some() {
                    following_cursor_offset = cursor.offset;
                    break;
                }
                accumulated_dimension_upper_bound += cursor.dimension_upper_bound;
                if threshold < accumulated_dimension_upper_bound {
                    pivot_cursor_index = Some(cursor_index);
                }
            }

            let Some(pivot_cursor_index) = pivot_cursor_index else {
                break;
            };

            let pivot_offset = cursors[pivot_cursor_index].offset;

            let (accumulated_block_upper_bound, min_block_next_offset) = cursors
                [..=pivot_cursor_index]
                .iter_mut()
                .filter_map(|cursor| {
                    if cursor.block_next_offset <= pivot_offset {
                        let (block_next_offset, block_max) = cursor
                            .block_iterator
                            .by_ref()
                            .find(|&(block_next_offset, _)| pivot_offset < block_next_offset)?;
                        cursor.block_next_offset = block_next_offset;
                        cursor.block_upper_bound = cursor.query * block_max;
                    }
                    Some((cursor.block_upper_bound, cursor.block_next_offset))
                })
                .fold(
                    (0.0, following_cursor_offset),
                    |(accumulated_block_upper_bound, min_block_next_offset),
                     (block_upper_bound, block_next_offset)| {
                        (
                            accumulated_block_upper_bound + block_upper_bound,
                            min_block_next_offset.min(block_next_offset),
                        )
                    },
                );

            let offset_cutoff = if accumulated_block_upper_bound < threshold
                && pivot_offset < min_block_next_offset
            {
                min_block_next_offset
            } else if pivot_offset < first_unchecked_offset {
                first_unchecked_offset
            } else if pivot_offset <= cursors[0].offset {
                let score = cursors
                    .iter()
                    .take_while(|cursor| cursor.offset <= pivot_offset)
                    .map(|cursor| cursor.query * cursor.value)
                    .sum();
                if (top_scores.len() as u32) < k {
                    top_scores.push(Score {
                        score,
                        offset: pivot_offset,
                    });
                } else if top_scores
                    .peek()
                    .map(|score| score.score)
                    .unwrap_or(f32::MIN)
                    < score
                {
                    top_scores.pop();
                    top_scores.push(Score {
                        score,
                        offset: pivot_offset,
                    });
                    threshold = top_scores
                        .peek()
                        .map(|score| score.score)
                        .unwrap_or_default();
                }
                first_unchecked_offset = pivot_offset + 1;
                first_unchecked_offset
            } else {
                pivot_offset
            };

            let mut cursor_index = 0;
            while cursor_index < cursors.len().min(pivot_cursor_index + 1) {
                let cursor = &mut cursors[cursor_index];
                if cursor.offset < offset_cutoff {
                    if let Some((offset, value)) = cursor
                        .dimension_iterator
                        .by_ref()
                        .find(|&(offset, _)| offset_cutoff <= offset && mask.contains(offset))
                    {
                        cursor.offset = offset;
                        cursor.value = value;
                    } else {
                        cursors.swap_remove(cursor_index);
                    }
                }
                cursor_index += 1;
            }
            cursors.sort_unstable();
        }

        Ok(top_scores.into_sorted_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparse::writer::SparseWriter;
    use chroma_blockstore::{
        arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider,
        BlockfileWriterOptions,
    };
    use chroma_types::SignedRoaringBitmap;

    async fn setup_reader_with_data(vectors: Vec<(u32, Vec<(u32, f32)>)>) -> SparseReader<'static> {
        let provider = BlockfileProvider::new_memory();

        let max_writer = provider
            .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()))
            .await
            .unwrap();
        let offset_value_writer = provider
            .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()))
            .await
            .unwrap();

        let writer = SparseWriter::new(64, max_writer, offset_value_writer, None);

        // Write all vectors
        for (offset, vector) in vectors {
            writer.set(offset, vector).await;
        }

        let flusher = writer.commit().await.unwrap();
        let max_id = flusher.max_id();
        let offset_value_id = flusher.offset_value_id();
        flusher.flush().await.unwrap();

        // Create and return reader
        let max_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(max_id, "".to_string()))
            .await
            .unwrap();
        let offset_value_reader = provider
            .read::<u32, f32>(BlockfileReaderOptions::new(offset_value_id, "".to_string()))
            .await
            .unwrap();

        SparseReader::new(max_reader, offset_value_reader)
    }

    #[tokio::test]
    async fn test_reader_wand_query_correctness() {
        // Setup data with known scores
        let vectors = vec![
            (0, vec![(0, 1.0), (1, 1.0), (2, 0.5)]), // dot product with query: 2.0
            (1, vec![(0, 0.5), (3, 1.0)]),           // dot product with query: 0.5
            (2, vec![(1, 0.5), (2, 1.0), (3, 0.5)]), // dot product with query: 1.0
            (3, vec![(0, 0.8), (1, 0.8)]),           // dot product with query: 1.6
            (4, vec![(4, 1.0), (5, 1.0)]),           // dot product with query: 0.0 (no overlap)
        ];

        let reader = setup_reader_with_data(vectors).await;

        // Test 1: Basic top-k query
        let query = vec![(0, 1.0), (1, 1.0)];
        let results = reader
            .wand(query.clone(), 3, SignedRoaringBitmap::full())
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].offset, 0); // offset 0, score 2.0
        assert_eq!(results[1].offset, 3); // offset 3, score 1.6
        assert_eq!(results[2].offset, 2); // offset 2, score 1.0

        // Verify scores are in descending order
        assert!(results[0].score >= results[1].score);
        assert!(results[1].score >= results[2].score);

        // Test 2: Query with k > num_documents
        let results = reader
            .wand(query.clone(), 10, SignedRoaringBitmap::full())
            .await
            .unwrap();
        assert_eq!(results.len(), 4); // Only 4 docs have non-zero scores

        // Test 3: Empty query (edge case)
        let results = reader
            .wand(vec![], 5, SignedRoaringBitmap::full())
            .await
            .unwrap();
        assert_eq!(results.len(), 0);

        // Test 4: Single dimension query (edge case)
        let results = reader
            .wand(vec![(0, 1.0)], 2, SignedRoaringBitmap::full())
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        // Test 5: No matching dimensions
        let results = reader
            .wand(vec![(99, 1.0)], 5, SignedRoaringBitmap::full())
            .await
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_reader_large_dataset() {
        // Test with dataset spanning multiple blocks
        let mut vectors = Vec::new();

        // Create 1000 vectors with varying sparsity
        for i in 0..1000 {
            let dims: Vec<(u32, f32)> = ((i % 10)..(i % 10 + 5))
                .map(|d| (d, (i as f32) * 0.001))
                .collect();
            vectors.push((i, dims));
        }

        let reader = setup_reader_with_data(vectors).await;

        // Query and verify we get top-k
        let query = vec![(0, 1.0), (1, 1.0), (2, 1.0)];
        let results = reader
            .wand(query, 10, SignedRoaringBitmap::full())
            .await
            .unwrap();

        assert_eq!(results.len(), 10);
        // Verify results are sorted by score
        for i in 1..results.len() {
            assert!(results[i - 1].score >= results[i].score);
        }
    }

    #[tokio::test]
    async fn test_reader_empty_index() {
        // Test querying empty index
        // Note: We need to write at least one vector and then delete it to create valid blockfiles
        let vectors = vec![(0, vec![(0, 1.0)])]; // Add one vector
        let reader = setup_reader_with_data(vectors).await;

        // Now test with a query that won't match
        let query = vec![(99, 1.0)]; // Query for dimension that doesn't exist
        let results = reader
            .wand(query, 5, SignedRoaringBitmap::full())
            .await
            .unwrap();

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_reader_tie_breaking() {
        // Vectors with identical scores
        let vectors = vec![
            (0, vec![(0, 1.0)]),
            (1, vec![(0, 1.0)]),
            (2, vec![(0, 1.0)]),
        ];

        let reader = setup_reader_with_data(vectors).await;

        let query = vec![(0, 1.0)];
        let results = reader
            .wand(query, 2, SignedRoaringBitmap::full())
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        // All have same score
        assert_eq!(results[0].score, results[1].score);
        // Verify consistent ordering (implementation dependent)
    }

    #[tokio::test]
    async fn test_wand_correctness_vs_exhaustive() {
        // Property test: WAND should return same top-k as exhaustive search
        let vectors = vec![
            (0, vec![(0, 0.5), (2, 0.3), (5, 0.8)]),
            (1, vec![(1, 0.7), (2, 0.2), (4, 0.9)]),
            (2, vec![(0, 0.3), (3, 0.6), (5, 0.4)]),
            (3, vec![(1, 0.8), (3, 0.5), (4, 0.2)]),
            (4, vec![(2, 0.9), (4, 0.3), (5, 0.7)]),
        ];

        let reader = setup_reader_with_data(vectors.clone()).await;

        let query = vec![(0, 0.4), (2, 0.6), (4, 0.5), (5, 0.3)];

        // Compute expected scores manually
        let mut expected_scores = vec![];
        for (offset, vector) in &vectors {
            let mut score = 0.0;
            for (q_dim, q_val) in &query {
                for (v_dim, v_val) in vector {
                    if q_dim == v_dim {
                        score += q_val * v_val;
                    }
                }
            }
            expected_scores.push((*offset, score));
        }
        expected_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // Get WAND results
        let wand_results = reader
            .wand(query, 3, SignedRoaringBitmap::full())
            .await
            .unwrap();

        // Verify WAND returns correct top-3
        for i in 0..3 {
            assert_eq!(wand_results[i].offset, expected_scores[i].0);
            assert!((wand_results[i].score - expected_scores[i].1).abs() < 1e-6);
        }
    }
}
