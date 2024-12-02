use std::{cmp::Ordering, collections::BinaryHeap};

use tonic::async_trait;

use crate::execution::operator::Operator;

use super::knn::RecordDistance;

#[derive(Clone, Debug)]
pub struct SpannKnnMergeOperator {
    pub k: u32,
}

#[derive(Debug)]
pub struct SpannKnnMergeInput {
    pub records: Vec<Vec<RecordDistance>>,
}

#[derive(Debug)]
pub struct SpannKnnMergeOutput {
    pub merged_records: Vec<RecordDistance>,
}

pub type SpannKnnMergeError = ();

struct RecordHeapEntry {
    distance: f32,
    offset_id: u32,
    array_index: usize,
}

impl Ord for RecordHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.total_cmp(&other.distance)
    }
}

impl PartialOrd for RecordHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RecordHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance.eq(&other.distance)
    }
}

impl Eq for RecordHeapEntry {}

#[async_trait]
impl Operator<SpannKnnMergeInput, SpannKnnMergeOutput> for SpannKnnMergeOperator {
    type Error = SpannKnnMergeError;

    async fn run(
        &self,
        input: &SpannKnnMergeInput,
    ) -> Result<SpannKnnMergeOutput, SpannKnnMergeError> {
        let mut pq = BinaryHeap::with_capacity(input.records.len());
        let mut indices = Vec::with_capacity(input.records.len());
        for (index, records) in input.records.iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            pq.push(RecordHeapEntry {
                distance: records[0].measure,
                offset_id: records[0].offset_id,
                array_index: index,
            });
            indices.push(0);
        }
        let mut count = 0;
        let mut result = Vec::with_capacity(self.k as usize);
        while let Some(v) = pq.pop() {
            if count == self.k {
                break;
            }
            result.push(RecordDistance {
                offset_id: v.offset_id,
                measure: v.distance,
            });
            count += 1;
            indices[v.array_index] += 1;
            if indices[v.array_index] < input.records[v.array_index].len() {
                pq.push(RecordHeapEntry {
                    distance: input.records[v.array_index][indices[v.array_index]].measure,
                    offset_id: input.records[v.array_index][indices[v.array_index]].offset_id,
                    array_index: v.array_index,
                });
            }
        }
        Ok(SpannKnnMergeOutput {
            merged_records: result,
        })
    }
}
