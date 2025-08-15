use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fmt,
    iter::Peekable,
    ops::RangeBounds,
};

use chroma_blockstore::BlockfileReader;
use chroma_error::ChromaError;
use thiserror::Error;

use crate::sparse::types::{encode_u32, DIMENSION_PREFIX};

struct Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
    block_iterator: Peekable<B>,
    block_upper_bound: f32,
    dimension_iterator: D,
    dimension_upper_bound: f32,
    offset: u32,
    query: f32,
    value: f32,
}

impl<B, D> fmt::Debug for Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor")
            .field("block_upper_bound", &self.block_upper_bound)
            .field("dimension_upper_bound", &self.dimension_upper_bound)
            .field("offset", &self.offset)
            .field("query", &self.query)
            .field("value", &self.value)
            .finish()
    }
}

impl<B, D> Eq for Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
}

impl<B, D> Ord for Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl<B, D> PartialEq for Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl<B, D> PartialOrd for Cursor<B, D>
where
    B: Iterator<Item = (u32, f32)>,
    D: Iterator<Item = (u32, f32)>,
{
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
    ) -> Result<(Vec<Score>, u32), SparseReaderError> {
        let collected_query = query_vector
            .into_iter()
            .map(|(dimension_id, query)| (dimension_id, encode_u32(dimension_id), query))
            .collect::<Vec<_>>();
        let dimension_count = collected_query.len();
        let all_dimension_max = self.get_dimension_max().await?;

        // NOTE: This is a temporary counter for debug purpose. Should be removed later
        let mut full_eval_counter = 0;

        let mut cursors = Vec::with_capacity(dimension_count);
        for (dimension_id, encoded_dimension_id, query) in &collected_query {
            if let Some(dimension_max) = all_dimension_max.get(dimension_id) {
                let mut block_iterator = self.get_blocks(encoded_dimension_id).await?.peekable();
                let Some(block_upper_bound) = block_iterator
                    .next()
                    .map(|(_, block_max)| query * block_max)
                else {
                    continue;
                };
                let dimension_upper_bound = query * dimension_max;
                let mut dimension_iterator =
                    self.get_offset_values(encoded_dimension_id, ..).await?;
                let Some((offset, value)) = dimension_iterator.next() else {
                    continue;
                };
                cursors.push(Cursor {
                    block_iterator,
                    block_upper_bound,
                    dimension_iterator,
                    dimension_upper_bound,
                    offset,
                    query: *query,
                    value,
                })
            }
        }

        cursors.sort_unstable();

        let Some(mut first_unchecked_offset) = cursors.first().map(|cursor| cursor.offset) else {
            return Ok((Vec::new(), 0));
        };

        let mut threshold = f32::MIN;
        let mut top_scores = BinaryHeap::<Score>::with_capacity(k as usize);

        loop {
            let mut accumulated_dimension_upper_bound = 0.0;
            let mut following_cursor_offset = u32::MAX;
            let mut peak_cursor_index = 0;
            let mut lag_cursor_index = 0;
            let mut pivot_cursor_index = None;

            for (cursor_index, cursor) in cursors.iter().enumerate() {
                if pivot_cursor_index.is_some() {
                    following_cursor_offset = cursor.offset;
                    break;
                }
                if cursors[peak_cursor_index].dimension_upper_bound < cursor.dimension_upper_bound {
                    if cursors[peak_cursor_index].offset < cursor.offset {
                        lag_cursor_index = peak_cursor_index;
                    }
                    peak_cursor_index = cursor_index;
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

            let (accumulated_block_upper_bound, min_block_cutoff) = cursors[..=pivot_cursor_index]
                .iter_mut()
                .map(|cursor| {
                    while let Some(&(next_block_first_offset, next_block_max)) =
                        cursor.block_iterator.peek()
                    {
                        if next_block_first_offset <= pivot_offset {
                            cursor.block_upper_bound = cursor.query * next_block_max;
                            cursor.block_iterator.next();
                        } else {
                            break;
                        }
                    }

                    let pivot_block_cutoff = cursor
                        .block_iterator
                        .peek()
                        .map(|&(next_block_first_offset, _)| next_block_first_offset)
                        .unwrap_or(u32::MAX);

                    (cursor.block_upper_bound, pivot_block_cutoff)
                })
                .fold(
                    (0.0, following_cursor_offset),
                    |(accumulated_block_upper_bound, min_block_cutoff),
                     (block_upper_bound, block_cutoff)| {
                        (
                            accumulated_block_upper_bound + block_upper_bound,
                            min_block_cutoff.min(block_cutoff),
                        )
                    },
                );

            let offset_cutoff =
                if accumulated_block_upper_bound < threshold && pivot_offset < min_block_cutoff {
                    min_block_cutoff
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
                    full_eval_counter += 1;
                    first_unchecked_offset = pivot_offset + 1;
                    first_unchecked_offset
                } else {
                    peak_cursor_index = lag_cursor_index;
                    pivot_offset
                };

            let mut exhausted = true;
            while let Some((offset, value)) = cursors[peak_cursor_index].dimension_iterator.next() {
                if offset_cutoff <= offset {
                    let rotate_cutoff_index =
                        cursors.partition_point(|cursor| cursor.offset < offset);
                    cursors[peak_cursor_index].offset = offset;
                    cursors[peak_cursor_index].value = value;
                    if peak_cursor_index < rotate_cutoff_index {
                        cursors[peak_cursor_index..rotate_cutoff_index].rotate_left(1);
                    }
                    exhausted = false;
                    break;
                }
            }
            if exhausted {
                cursors.remove(peak_cursor_index);
            }
        }

        Ok((top_scores.into_sorted_vec(), full_eval_counter))
    }
}
