use async_trait::async_trait;

use chroma_system::Operator;
use chroma_types::operator::{KnnMerge, KnnMergeInput, KnnOutput, RecordDistance};
use thiserror::Error;

/// The `KnnMergeOperator` selects the records nearest to target from the two vectors of records
/// which are both sorted by distance in ascending order
///
/// # Inputs
/// - `first_distances`: The first vector of records, sorted by distance in ascending order
/// - `second_distances`: The second vector of records, sorted by distance in ascending order
///
/// # Outputs
/// - `record_distances`: The nearest records in either vectors, sorted by distance in ascending order
///
/// # Usage
/// It can be used to merge the nearest results from the log and the vector segment

#[derive(Debug)]
pub struct KnnBinaryMergeInput {
    pub first_distances: Vec<RecordDistance>,
    pub second_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
#[error("Knn merge error (unreachable)")]
pub struct KnnMergeError;

#[async_trait]
impl Operator<KnnBinaryMergeInput, KnnOutput> for KnnMerge {
    type Error = KnnMergeError;

    async fn run(&self, input: &KnnBinaryMergeInput) -> Result<KnnOutput, KnnMergeError> {
        Ok(self.merge(KnnMergeInput {
            batch_distances: vec![
                input.first_distances.clone(),
                input.second_distances.clone(),
            ],
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::operators::{knn::RecordDistance, knn_merge::KnnMergeOperator};
    use chroma_system::Operator;

    use super::KnnBinaryMergeInput;

    /// The unit tests for `KnnMergeOperator` uses the following test data
    /// It generates records where the distance to target is the same as value of offset
    /// - First: 4, 8, ..., 100
    /// - Second: 1, 3, ..., 99
    fn setup_knn_merge_input() -> KnnBinaryMergeInput {
        KnnBinaryMergeInput {
            first_distances: (1..=100)
                .filter_map(|offset_id| {
                    (offset_id % 4 == 0).then_some(RecordDistance {
                        offset_id,
                        measure: offset_id as f32,
                    })
                })
                .collect(),
            second_distances: (1..=100)
                .filter_map(|offset_id| {
                    (offset_id % 2 != 0).then_some(RecordDistance {
                        offset_id,
                        measure: offset_id as f32,
                    })
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_simple_merge() {
        let knn_merge_input = setup_knn_merge_input();

        let knn_merge_operator = KnnMergeOperator { fetch: 6 };

        let knn_merge_output = knn_merge_operator
            .run(&knn_merge_input)
            .await
            .expect("KnnMergeOperator should not fail");

        assert_eq!(
            knn_merge_output
                .record_distances
                .iter()
                .map(|record| record.offset_id)
                .collect::<Vec<_>>(),
            vec![1, 3, 4, 5, 7, 8]
        );
    }
}
