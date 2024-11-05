use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_types::Segment;
use thiserror::Error;
use tonic::async_trait;
use tracing::trace;

use crate::execution::{
    operator::{Operator, OperatorType},
    operators::projection::ProjectionInput,
};

use super::{
    fetch_log::FetchLogOutput,
    knn::RecordDistance,
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
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Clone, Debug)]
pub struct KnnProjectionRecord {
    pub record: ProjectionRecord,
    pub distance: Option<f32>,
}

#[derive(Clone, Debug)]
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

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &KnnProjectionInput,
    ) -> Result<KnnProjectionOutput, KnnProjectionError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let projection_input = ProjectionInput {
            logs: input.logs.clone(),
            blockfile_provider: input.blockfile_provider.clone(),
            record_segment: input.record_segment.clone(),
            offset_ids: input
                .record_distances
                .iter()
                .map(
                    |RecordDistance {
                         offset_id,
                         measure: _,
                     }| *offset_id,
                )
                .collect(),
        };

        let result = self.projection.run(&projection_input).await?;

        return Ok(KnnProjectionOutput {
            records: result
                .records
                .into_iter()
                .zip(input.record_distances.clone())
                .map(
                    |(
                        record,
                        RecordDistance {
                            offset_id: _,
                            measure,
                        },
                    )| KnnProjectionRecord {
                        record,
                        distance: self.distance.then_some(measure),
                    },
                )
                .collect(),
        });
    }
}
