use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tonic::async_trait;

use crate::execution::operator::Operator;

use super::knn::RecordDistance;

/// The `KnnMergeOperator` selects the records nearest to target according to provided distance meansure
///
/// # Parameters
/// - `fetch`: The total number of records to fetch
///
/// # Inputs
/// - `log_distances`: The nearest records in the log, sorted by distance in ascending order
/// - `segment_distances`: The nearest records in the compact segment, sorted by distance in ascending order
///
/// # Outputs
/// - `record_distances`: The nearest records, sorted by distance in ascending order
///
/// # Usage
/// It can be used to merge the nearest results from the log and the vector segment
#[derive(Clone, Debug)]
pub struct KnnMergeOperator {
    pub fetch: u32,
}

#[derive(Debug)]
pub struct KnnMergeInput {
    pub log_distances: Vec<RecordDistance>,
    pub segment_distances: Vec<RecordDistance>,
}

#[derive(Debug)]
pub struct KnnMergeOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnMergeError {
    #[error("Error converting incomplete input")]
    IncompleteInput,
}

impl ChromaError for KnnMergeError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnMergeError::IncompleteInput => ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait]
impl Operator<KnnMergeInput, KnnMergeOutput> for KnnMergeOperator {
    type Error = KnnMergeError;

    async fn run(&self, input: &KnnMergeInput) -> Result<KnnMergeOutput, KnnMergeError> {
        let mut fetch = self.fetch;
        let mut log_index = 0;
        let mut segment_index = 0;

        let mut merged_distance = Vec::new();

        while fetch > 0 {
            let log_dist = input.log_distances.get(log_index);
            let segment_dist = input.segment_distances.get(segment_index);

            match (log_dist, segment_dist) {
                (Some(ldist), Some(sdist)) => {
                    if ldist.measure < sdist.measure {
                        merged_distance.push(ldist.clone());
                        log_index += 1;
                    } else {
                        merged_distance.push(sdist.clone());
                        segment_index += 1;
                    }
                }
                (None, Some(dist)) => {
                    merged_distance.push(dist.clone());
                    segment_index += 1;
                }
                (Some(dist), None) => {
                    merged_distance.push(dist.clone());
                    log_index += 1;
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
    /// - Log: 4, 8, ..., 100
    /// - Compacted: 1, 3, ..., 99
    fn setup_knn_merge_input() -> KnnMergeInput {
        KnnMergeInput {
            log_distances: (1..=100)
                .filter_map(|offset_id| {
                    (offset_id % 4 == 0).then_some(RecordDistance {
                        offset_id,
                        measure: offset_id as f32,
                    })
                })
                .collect(),
            segment_distances: (1..=100)
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
