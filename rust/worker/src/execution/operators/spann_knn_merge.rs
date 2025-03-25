use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

use async_trait::async_trait;

use chroma_system::Operator;
use thiserror::Error;

use super::knn::RecordDistance;

#[derive(Clone, Debug)]
pub struct SpannKnnMergeOperator {
    pub k: u32,
}

#[derive(Debug)]
pub struct SpannKnnMergeInput {
    pub records: Vec<Vec<RecordDistance>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SpannKnnMergeOutput {
    pub merged_records: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
#[error("Spann knn merge error (unreachable)")]
pub struct SpannKnnMergeError;

#[derive(Debug)]
struct RecordHeapEntry {
    distance: f32,
    offset_id: u32,
    array_index: usize,
}

impl Ord for RecordHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.distance.total_cmp(&self.distance)
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
            indices.push(0);
            if records.is_empty() {
                continue;
            }
            pq.push(RecordHeapEntry {
                distance: records[0].measure,
                offset_id: records[0].offset_id,
                array_index: index,
            });
        }
        let mut count = 0;
        let mut result = Vec::with_capacity(self.k as usize);
        let mut unique_ids = HashSet::new();
        while let Some(v) = pq.pop() {
            if count == self.k {
                break;
            }
            if !unique_ids.contains(&v.offset_id) {
                unique_ids.insert(v.offset_id);
                result.push(RecordDistance {
                    offset_id: v.offset_id,
                    measure: v.distance,
                });
                count += 1;
            }
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

#[cfg(test)]
mod test {
    use crate::execution::operators::knn::RecordDistance;
    use crate::execution::operators::spann_knn_merge::{SpannKnnMergeInput, SpannKnnMergeOperator};
    use chroma_system::Operator;

    // Basic operator test.
    #[tokio::test]
    async fn test_spann_knn_merge_operator() {
        let input = SpannKnnMergeInput {
            records: vec![
                vec![
                    RecordDistance {
                        offset_id: 1,
                        measure: 0.1,
                    },
                    RecordDistance {
                        offset_id: 2,
                        measure: 0.5,
                    },
                    RecordDistance {
                        offset_id: 3,
                        measure: 1.0,
                    },
                ],
                vec![
                    RecordDistance {
                        offset_id: 4,
                        measure: 0.3,
                    },
                    RecordDistance {
                        offset_id: 5,
                        measure: 0.4,
                    },
                    RecordDistance {
                        offset_id: 6,
                        measure: 0.6,
                    },
                ],
                vec![
                    RecordDistance {
                        offset_id: 7,
                        measure: 0.7,
                    },
                    RecordDistance {
                        offset_id: 8,
                        measure: 0.8,
                    },
                    RecordDistance {
                        offset_id: 9,
                        measure: 0.9,
                    },
                ],
            ],
        };

        let operator = SpannKnnMergeOperator { k: 5 };
        let mut output = operator.run(&input).await.unwrap();

        assert_eq!(output.merged_records.len(), 5);
        output
            .merged_records
            .sort_by(|a, b| a.offset_id.partial_cmp(&b.offset_id).unwrap());
        assert_eq!(output.merged_records[0].offset_id, 1);
        assert_eq!(output.merged_records[1].offset_id, 2);
        assert_eq!(output.merged_records[2].offset_id, 4);
        assert_eq!(output.merged_records[3].offset_id, 5);
        assert_eq!(output.merged_records[4].offset_id, 6);
    }

    #[tokio::test]
    async fn test_non_duplicates() {
        let input = SpannKnnMergeInput {
            records: vec![
                vec![
                    RecordDistance {
                        offset_id: 1,
                        measure: 0.1,
                    },
                    RecordDistance {
                        offset_id: 2,
                        measure: 0.5,
                    },
                    RecordDistance {
                        offset_id: 5,
                        measure: 1.0,
                    },
                ],
                vec![
                    RecordDistance {
                        offset_id: 2,
                        measure: 0.5,
                    },
                    RecordDistance {
                        offset_id: 3,
                        measure: 0.6,
                    },
                    RecordDistance {
                        offset_id: 6,
                        measure: 0.7,
                    },
                ],
                vec![
                    RecordDistance {
                        offset_id: 3,
                        measure: 0.6,
                    },
                    RecordDistance {
                        offset_id: 5,
                        measure: 1.0,
                    },
                ],
            ],
        };

        let operator = SpannKnnMergeOperator { k: 5 };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.merged_records.len(), 5);
        // output is sorted by distance.
        assert_eq!(output.merged_records[0].offset_id, 1);
        assert_eq!(output.merged_records[1].offset_id, 2);
        assert_eq!(output.merged_records[2].offset_id, 3);
        assert_eq!(output.merged_records[3].offset_id, 6);
        assert_eq!(output.merged_records[4].offset_id, 5);
    }
}
