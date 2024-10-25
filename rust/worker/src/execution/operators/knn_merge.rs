use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tonic::async_trait;

use crate::execution::operator::Operator;

use super::knn::RecordDistance;

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
