use tonic::async_trait;

use crate::execution::operator::Operator;

use super::knn::RecordDistance;

/// The `KnnMergeOperator` selects the records nearest to target from the two vectors of records
/// which are both sorted by distance in ascending order
///
/// # Parameters
/// - `fetch`: The total number of records to fetch
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
#[derive(Clone, Debug)]
pub struct KnnMergeOperator {
    pub fetch: u32,
}

#[derive(Debug)]
pub struct KnnMergeInput {
    pub first_distances: Vec<RecordDistance>,
    pub second_distances: Vec<RecordDistance>,
}

#[derive(Debug)]
pub struct KnnMergeOutput {
    pub record_distances: Vec<RecordDistance>,
}

pub type KnnMergeError = ();

#[async_trait]
impl Operator<KnnMergeInput, KnnMergeOutput> for KnnMergeOperator {
    type Error = KnnMergeError;

    async fn run(&self, input: &KnnMergeInput) -> Result<KnnMergeOutput, KnnMergeError> {
        let mut fetch = self.fetch;
        let mut first_index = 0;
        let mut second_index = 0;

        let mut merged_distance = Vec::new();

        while fetch > 0 {
            let first_dist = input.first_distances.get(first_index);
            let second_dist = input.second_distances.get(second_index);

            match (first_dist, second_dist) {
                (Some(fdist), Some(sdist)) => {
                    if fdist.measure < sdist.measure {
                        merged_distance.push(fdist.clone());
                        first_index += 1;
                    } else {
                        merged_distance.push(sdist.clone());
                        second_index += 1;
                    }
                }
                (None, Some(dist)) => {
                    merged_distance.push(dist.clone());
                    second_index += 1;
                }
                (Some(dist), None) => {
                    merged_distance.push(dist.clone());
                    first_index += 1;
                }
                _ => break,
            }
            fetch -= 1;
        }

        Ok(KnnMergeOutput {
            record_distances: merged_distance,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::{
        operator::Operator,
        operators::{knn::RecordDistance, knn_merge::KnnMergeOperator},
    };

    use super::KnnMergeInput;

    /// The unit tests for `KnnMergeOperator` uses the following test data
    /// It generates records where the distance to target is the same as value of offset
    /// - First: 4, 8, ..., 100
    /// - Second: 1, 3, ..., 99
    fn setup_knn_merge_input() -> KnnMergeInput {
        KnnMergeInput {
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
