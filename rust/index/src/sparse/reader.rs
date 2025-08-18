use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fmt,
    ops::RangeBounds,
};

use chroma_blockstore::BlockfileReader;
use chroma_error::ChromaError;
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

#[derive(Debug)]
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
                .skip_while(|&(offset, _)| !mask.contains(offset))
                .next()
            else {
                continue;
            };
            let mut block_iterator = self.get_blocks(encoded_dimension_id).await?.peekable();
            let Some((block_next_offset, block_max)) = block_iterator
                .by_ref()
                .skip_while(|&(block_next_offset, _)| block_next_offset <= offset)
                .next()
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
        let mut top_scores = BinaryHeap::<Score>::with_capacity(k as usize);

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
                            .skip_while(|&(block_next_offset, _)| block_next_offset <= pivot_offset)
                            .next()?;
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
                        .skip_while(|&(offset, _)| offset < offset_cutoff || !mask.contains(offset))
                        .next()
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
