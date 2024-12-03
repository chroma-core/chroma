use crate::execution::operator::Operator;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::{materialize_logs, record_segment::RecordSegmentReader};
use crate::segment::{LogMaterializerError, MaterializedLogRecord};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_types::{Chunk, LogRecord, Segment};
use futures::TryFutureExt;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MaterializeLogOperatorError {
    #[error("Could not create record segment reader: {0}")]
    RecordSegmentReaderCreationFailed(#[from] RecordSegmentReaderCreationError),
    #[error("Log materialization failed: {0}")]
    LogMaterializationFailed(#[from] LogMaterializerError),
}

impl ChromaError for MaterializeLogOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            MaterializeLogOperatorError::RecordSegmentReaderCreationFailed(e) => e.code(),
            MaterializeLogOperatorError::LogMaterializationFailed(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct MaterializeLogOperator {}

impl MaterializeLogOperator {
    pub fn new() -> Box<Self> {
        Box::new(MaterializeLogOperator {})
    }
}

#[derive(Debug)]
pub struct MaterializeLogInput {
    logs: Chunk<LogRecord>,
    provider: BlockfileProvider,
    record_segment: Segment,
    offset_id: Arc<AtomicU32>,
}

impl MaterializeLogInput {
    pub fn new(
        logs: Chunk<LogRecord>,
        provider: BlockfileProvider,
        record_segment: Segment,
        offset_id: Arc<AtomicU32>,
    ) -> Self {
        MaterializeLogInput {
            logs,
            provider,
            record_segment,
            offset_id,
        }
    }
}

pub struct MaterializeLogOutput {
    pub(crate) result: Chunk<MaterializedLogRecord>,
}

impl std::fmt::Debug for MaterializeLogOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeLogOutput")
            .field("# of materialized records", &self.result.total_len())
            .finish()
    }
}

#[async_trait]
impl Operator<MaterializeLogInput, MaterializeLogOutput> for MaterializeLogOperator {
    type Error = MaterializeLogOperatorError;

    async fn run(&self, input: &MaterializeLogInput) -> Result<MaterializeLogOutput, Self::Error> {
        tracing::debug!("Materializing {} log entries", input.logs.total_len());

        let record_segment_reader =
            match RecordSegmentReader::from_segment(&input.record_segment, &input.provider).await {
                Ok(reader) => Some(reader),
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => None,
                        err => {
                            tracing::error!("Error creating record segment reader: {:?}", err);
                            return Err(
                                MaterializeLogOperatorError::RecordSegmentReaderCreationFailed(err),
                            );
                        }
                    }
                }
            };

        let result = materialize_logs(
            &record_segment_reader,
            &input.logs,
            Some(input.offset_id.clone()),
        )
        .map_err(MaterializeLogOperatorError::LogMaterializationFailed)
        .await?;

        Ok(MaterializeLogOutput { result })
    }
}
