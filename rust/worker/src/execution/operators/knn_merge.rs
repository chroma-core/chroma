use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tonic::async_trait;

use crate::execution::{operator::Operator, utils::Distance};

use super::{fetch_log::FetchLogOutput, fetch_segment::FetchSegmentOutput};

#[derive(Clone, Debug)]
pub struct KnnMergeOperator {
    pub fetch: u32,
}

#[derive(Clone, Debug, Default)]
pub struct PreKnnMergeState {
    pub logs: Option<FetchLogOutput>,
    pub segments: Option<FetchSegmentOutput>,
    pub log_distance: Option<Vec<Distance>>,
    pub segment_distance: Option<Vec<Distance>>,
}

#[derive(Debug)]
pub struct KnnMergeInput {
    pub logs: FetchLogOutput,
    pub segments: FetchSegmentOutput,
    pub log_distance: Vec<Distance>,
    pub segment_distance: Vec<Distance>,
}

#[derive(Debug)]
pub struct KnnMergeOutput {
    pub logs: FetchLogOutput,
    pub segments: FetchSegmentOutput,
    pub distance: Vec<Distance>,
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

impl TryFrom<PreKnnMergeState> for KnnMergeInput {
    type Error = KnnMergeError;

    fn try_from(value: PreKnnMergeState) -> Result<Self, KnnMergeError> {
        if let PreKnnMergeState {
            logs: Some(logs),
            segments: Some(segments),
            log_distance: Some(log_distance),
            segment_distance: Some(segment_distance),
        } = value
        {
            Ok(KnnMergeInput {
                logs,
                segments,
                log_distance,
                segment_distance,
            })
        } else {
            Err(KnnMergeError::IncompleteInput)
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
            let log_dist = input.log_distance.get(log_index);
            let segment_dist = input.segment_distance.get(segment_index);

            match (log_dist, segment_dist) {
                (Some(ld), Some(sd)) => {
                    if ld.measure < sd.measure {
                        merged_distance.push(ld.clone());
                        log_index += 1;
                    } else {
                        merged_distance.push(sd.clone());
                        segment_index += 1;
                    }
                }
                (None, Some(d)) => {
                    merged_distance.push(d.clone());
                    segment_index += 1;
                }
                (Some(d), None) => {
                    merged_distance.push(d.clone());
                    log_index += 1;
                }
                _ => {}
            }
            fetch -= 1;
        }

        Ok(KnnMergeOutput {
            logs: input.logs.clone(),
            segments: input.segments.clone(),
            distance: merged_distance,
        })
    }
}
