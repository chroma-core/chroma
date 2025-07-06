use async_trait::async_trait;

use chroma_system::Operator;
use chroma_types::operator::{KnnMerge, RecordDistance};
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
    pub batch_distances: Vec<Vec<RecordDistance>>,
}

#[derive(Debug, Default)]
pub struct KnnMergeOutput {
    pub distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
#[error("Knn merge error (unreachable)")]
pub struct KnnMergeError;

#[async_trait]
impl Operator<KnnMergeInput, KnnMergeOutput> for KnnMerge {
    type Error = KnnMergeError;

    async fn run(&self, input: &KnnMergeInput) -> Result<KnnMergeOutput, KnnMergeError> {
        Ok(KnnMergeOutput {
            distances: self.merge(input.batch_distances.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_system::Operator;
    use chroma_types::operator::{KnnMerge, RecordDistance};

    use super::KnnMergeInput;

    /// The unit tests for `KnnMerge` operator uses the following test data
    /// It generates records where the distance to target is the same as value of offset
    /// - First: 4, 8, ..., 100
    /// - Second: 1, 3, ..., 99
    fn setup_knn_merge_input() -> KnnMergeInput {
        KnnMergeInput {
            batch_distances: vec![
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 3 == 1).then_some(RecordDistance {
                            offset_id,
                            measure: offset_id as f32,
                        })
                    })
                    .collect(),
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 5 == 2).then_some(RecordDistance {
                            offset_id,
                            measure: offset_id as f32,
                        })
                    })
                    .collect(),
                (1..=100)
                    .filter_map(|offset_id| {
                        (offset_id % 7 == 3).then_some(RecordDistance {
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

        let knn_merge_operator = KnnMerge { fetch: 10 };

        let knn_merge_output = knn_merge_operator
            .run(&knn_merge_input)
            .await
            .expect("KnnMergeOperator should not fail");

        assert_eq!(
            knn_merge_output
                .distances
                .iter()
                .map(|record| record.offset_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 7, 10, 12, 13, 16, 17]
        );
    }
}
