use std::cmp::Reverse;

use async_trait::async_trait;

use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{Merge, RecordMeasure};
use thiserror::Error;

/// The `KnnMerge` operator selects the records nearest to target from the two vectors of records
/// which are both sorted by distance in ascending order
///
/// # Inputs
/// - `batch_distances`: A batch of record vectors, each vector is sorted by distance in ascending order
///
/// # Outputs
/// - `record_distances`: The nearest records in either vectors, sorted by distance in ascending order
///
/// # Usage
/// It can be used to merge the nearest results from the log and the vector segment

#[derive(Debug)]
pub struct KnnMergeInput {
    pub batch_measures: Vec<Vec<RecordMeasure>>,
}

#[derive(Debug, Default)]
pub struct KnnMergeOutput {
    pub measures: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Knn merge error (unreachable)")]
pub struct KnnMergeError;

impl ChromaError for KnnMergeError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<KnnMergeInput, KnnMergeOutput> for Merge {
    type Error = KnnMergeError;

    async fn run(&self, input: &KnnMergeInput) -> Result<KnnMergeOutput, KnnMergeError> {
        // Reversing because similarity is in ascending order,
        // while Merge takes element in descending order
        let reversed_distances = input
            .batch_measures
            .iter()
            .map(|batch| batch.iter().map(|m| Reverse(m.clone())).collect())
            .collect();
        Ok(KnnMergeOutput {
            measures: self
                .merge(reversed_distances)
                .into_iter()
                .map(|Reverse(distance)| distance)
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_system::Operator;
    use chroma_types::operator::{Merge, RecordMeasure};

    use super::KnnMergeInput;

    /// The unit tests for `KnnMerge` operator uses the following test data
    /// It generates records where the distance to target is the same as value of offset
    /// - First: 4, 8, ..., 100
    /// - Second: 1, 3, ..., 99
    fn setup_knn_merge_input() -> KnnMergeInput {
        KnnMergeInput {
            batch_measures: vec![
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 3 == 1).then_some(RecordMeasure {
                            offset_id,
                            measure: offset_id as f32,
                        })
                    })
                    .collect(),
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 5 == 2).then_some(RecordMeasure {
                            offset_id,
                            measure: offset_id as f32,
                        })
                    })
                    .collect(),
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 7 == 3).then_some(RecordMeasure {
                            offset_id,
                            measure: offset_id as f32,
                        })
                    })
                    .collect(),
            ],
        }
    }

    #[tokio::test]
    async fn test_simple_merge() {
        let knn_merge_input = setup_knn_merge_input();

        let knn_merge_operator = Merge { k: 10 };

        let knn_merge_output = knn_merge_operator
            .run(&knn_merge_input)
            .await
            .expect("KnnMergeOperator should not fail");

        assert_eq!(
            knn_merge_output
                .measures
                .iter()
                .map(|record| record.offset_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 7, 10, 12, 13, 16, 17]
        );
    }
}
