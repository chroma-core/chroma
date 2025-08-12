use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

use chroma_blockstore::BlockfileReader;
use chroma_error::ChromaError;
use futures::{StreamExt, TryStreamExt};
use thiserror::Error;

use crate::sparse::types::{encode_u32, DIMENSION_PREFIX};

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Cursor {
    offset: u32,
    dimension_index: usize,
}

#[derive(Debug, PartialEq)]
pub struct Score {
    score: f32,
    offset: u32,
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
        Some(self.cmp(&other))
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
    pub async fn get_dimension_max(&self) -> Result<HashMap<u32, f32>, SparseReaderError> {
        Ok(self
            .max_reader
            .get_range_stream(DIMENSION_PREFIX..=DIMENSION_PREFIX, ..)
            .map(|result| result.map(|(_, dimension_id, max)| (dimension_id, max)))
            .try_collect::<HashMap<_, _>>()
            .await?)
    }

    pub async fn get_blocks(
        &self,
        dimension_id: u32,
    ) -> Result<Vec<(u32, f32)>, SparseReaderError> {
        let encoded_dimension = encode_u32(dimension_id);
        Ok(self
            .max_reader
            .get_range_stream(encoded_dimension.as_str()..=encoded_dimension.as_str(), ..)
            .map(|result| {
                result.map(|(_, block_first_offset, block_max)| (block_first_offset, block_max))
            })
            .try_collect::<Vec<_>>()
            .await?)
    }

    pub async fn lower_bound_offset_value(
        &self,
        dimension_id: u32,
        lower_bound: u32,
    ) -> Result<Option<(u32, f32)>, SparseReaderError> {
        let encoded_dimension = encode_u32(dimension_id);
        Ok(self
            .offset_value_reader
            .get_range_stream(
                encoded_dimension.clone().as_str()..=encoded_dimension.clone().as_str(),
                lower_bound..,
            )
            .try_next()
            .await?
            .map(|(_, offset, value)| (offset, value)))
    }

    pub async fn wand(
        &self,
        query_vector: impl IntoIterator<Item = (u32, f32)>,
        k: u32,
    ) -> Result<Vec<Score>, SparseReaderError> {
        let collected_query = query_vector.into_iter().collect::<Vec<_>>();
        let dimension_count = collected_query.len();
        let all_dimension_max = self.get_dimension_max().await?;

        let mut block_cursor = Vec::with_capacity(dimension_count);
        let mut block_statistic = Vec::with_capacity(dimension_count);
        let mut dimension_cursor = Vec::with_capacity(dimension_count);
        let mut dimension_id = Vec::with_capacity(dimension_count);
        let mut dimension_upper_bound = Vec::with_capacity(dimension_count);
        let mut dimension_score = Vec::with_capacity(dimension_count);
        let mut query_weight = Vec::with_capacity(dimension_count);

        for (id, weight) in collected_query {
            if let Some(dimension_max_value) = all_dimension_max.get(&id) {
                let blocks = self
                    .get_blocks(id)
                    .await?
                    .into_iter()
                    .map(|(offset, block_max_value)| (offset, weight * block_max_value))
                    .collect::<Vec<_>>();
                if let Some((offset, value)) = blocks.first().cloned() {
                    block_cursor.push(0);
                    block_statistic.push(blocks);
                    let dimension_index = dimension_cursor.len();
                    dimension_cursor.push(Cursor {
                        offset,
                        dimension_index,
                    });
                    dimension_id.push(id);
                    dimension_upper_bound.push(weight * dimension_max_value);
                    dimension_score.push(weight * value);
                    query_weight.push(weight);
                }
            }
        }

        dimension_cursor.sort_unstable();

        let Some(mut first_unchecked_offset) = dimension_cursor.first().map(|cursor| cursor.offset)
        else {
            return Ok(Vec::new());
        };

        let mut threshold = f32::MIN;
        let mut top_scores = BinaryHeap::<Score>::with_capacity(k as usize);

        loop {
            let mut accumulated_dimension_upper_bound = 0.0;
            let mut following_dimension_cursor_offset = u32::MAX;
            let mut peak_dimension_cursor_index = 0;
            let mut lag_dimension_cursor_index = 0;
            let mut pivot_dimension_cursor_index = None;

            for (
                cursor_index,
                &Cursor {
                    offset,
                    dimension_index,
                },
            ) in dimension_cursor.iter().enumerate()
            {
                if pivot_dimension_cursor_index.is_some() {
                    following_dimension_cursor_offset = offset;
                    break;
                }
                if dimension_upper_bound
                    [dimension_cursor[peak_dimension_cursor_index].dimension_index]
                    < dimension_upper_bound[dimension_index]
                {
                    if dimension_cursor[peak_dimension_cursor_index].offset < offset {
                        lag_dimension_cursor_index = peak_dimension_cursor_index;
                    }
                    peak_dimension_cursor_index = cursor_index;
                }
                accumulated_dimension_upper_bound += dimension_upper_bound[dimension_index];
                if threshold < accumulated_dimension_upper_bound {
                    pivot_dimension_cursor_index = Some(cursor_index);
                }
            }

            let Some(pivot_dimension_cursor_index) = pivot_dimension_cursor_index else {
                break;
            };

            let pivot_offset = dimension_cursor[pivot_dimension_cursor_index].offset;
            let (accumulated_block_upper_bound, min_block_cutoff) = dimension_cursor
                [..=pivot_dimension_cursor_index]
                .iter()
                .filter_map(
                    |&Cursor {
                         offset: _,
                         dimension_index,
                     }| {
                        let block_offset = loop {
                            match block_statistic[dimension_index]
                                .get(block_cursor[dimension_index] + 1)
                            {
                                Some(&(offset, _)) if offset <= pivot_offset => {
                                    block_cursor[dimension_index] += 1
                                }
                                _ => break block_cursor[dimension_index],
                            };
                        };

                        let &(_, pivot_block_upper_bound) =
                            block_statistic[dimension_index].get(block_offset)?;
                        let pivot_block_cutoff = block_statistic[dimension_index]
                            .get(block_offset + 1)
                            .map(|&(offset, _)| offset)
                            .unwrap_or(u32::MAX);
                        Some((pivot_block_upper_bound, pivot_block_cutoff))
                    },
                )
                .fold(
                    (0.0, following_dimension_cursor_offset),
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
                } else if pivot_offset <= dimension_cursor[0].offset {
                    let score = dimension_cursor[..=pivot_dimension_cursor_index]
                        .iter()
                        .map(
                            |&Cursor {
                                 offset: _,
                                 dimension_index,
                             }| dimension_score[dimension_index],
                        )
                        .sum();
                    if (top_scores.len() as u32) < k {
                        top_scores.push(Score {
                            score,
                            offset: pivot_offset,
                        });
                    } else if top_scores.peek().is_none()
                        || top_scores.peek().is_some_and(
                            |&Score {
                                 score: min_top_score,
                                 offset: _,
                             }| min_top_score < score,
                        )
                    {
                        top_scores.pop();
                        top_scores.push(Score {
                            score,
                            offset: pivot_offset,
                        });
                        threshold = top_scores
                            .peek()
                            .map(|&Score { score, offset: _ }| score)
                            .unwrap_or_default();
                    }
                    first_unchecked_offset = pivot_offset + 1;
                    first_unchecked_offset
                } else {
                    peak_dimension_cursor_index = lag_dimension_cursor_index;
                    pivot_offset
                };

            let next_offset = self
                .lower_bound_offset_value(
                    dimension_id[dimension_cursor[peak_dimension_cursor_index].dimension_index],
                    offset_cutoff,
                )
                .await?;

            if let Some((offset, value)) = next_offset {
                let rotate_cutoff_index =
                    dimension_cursor.partition_point(|cursor| cursor.offset < offset);
                let peak_dimension_index =
                    dimension_cursor[peak_dimension_cursor_index].dimension_index;
                dimension_score[peak_dimension_index] = query_weight[peak_dimension_index] * value;
                dimension_cursor[peak_dimension_cursor_index].offset = offset;
                if peak_dimension_cursor_index < rotate_cutoff_index {
                    dimension_cursor[peak_dimension_cursor_index..rotate_cutoff_index]
                        .rotate_left(1);
                }
            } else {
                dimension_cursor.remove(peak_dimension_cursor_index);
            }
        }

        Ok(top_scores.into_sorted_vec())
    }
}
