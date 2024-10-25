use chroma_error::ChromaError;
use thiserror::Error;
use tonic::async_trait;
use tracing::trace;

use crate::execution::{operator::Operator, utils::Distance};

use super::{
    fetch_log::FetchLogOutput,
    fetch_segment::FetchSegmentOutput,
    knn_merge::KnnMergeOutput,
    projection::{ProjectionError, ProjectionOperator, ProjectionRecord},
};

#[derive(Clone, Debug)]
pub struct KnnProjectionOperator {
    pub projection: ProjectionOperator,
    pub distance: bool,
}

#[derive(Clone, Debug)]
pub struct KnnProjectionInput {
    pub logs: FetchLogOutput,
    pub segments: FetchSegmentOutput,
    pub distance: Vec<Distance>,
}

impl From<KnnMergeOutput> for KnnProjectionInput {
    fn from(value: KnnMergeOutput) -> Self {
        Self {
            logs: value.logs,
            segments: value.segments,
            distance: value.distance,
        }
    }
}

#[derive(Debug)]
pub struct KnnProjectionRecord {
    pub record: ProjectionRecord,
    pub distance: Option<f32>,
}

#[derive(Debug)]
pub struct KnnProjectionOutput {
    pub records: Vec<KnnProjectionRecord>,
}

#[derive(Error, Debug)]
pub enum KnnProjectionError {
    #[error("Error running projection operator: {0}")]
    Projection(#[from] ProjectionError),
}

impl ChromaError for KnnProjectionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KnnProjectionError::Projection(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnProjectionInput, KnnProjectionOutput> for KnnProjectionOperator {
    type Error = KnnProjectionError;

    async fn run(
        &self,
        input: &KnnProjectionInput,
    ) -> Result<KnnProjectionOutput, KnnProjectionError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let result = self.projection.run(&(input.clone().into())).await?;

        return Ok(KnnProjectionOutput {
            records: result
                .records
                .into_iter()
                .zip(input.distance.clone())
                .map(
                    |(record, Distance { oid: _, measure })| KnnProjectionRecord {
                        record,
                        distance: self.distance.then_some(measure),
                    },
                )
                .collect(),
        });
    }
}
