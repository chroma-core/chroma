use std::sync::Arc;

use crate::execution::operator::Operator;
use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializerError;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use thiserror::Error;
use tracing::Instrument;

use super::materialize_logs::MaterializeLogOutput;

#[derive(Error, Debug)]
pub enum ApplyLogToSegmentWriterOperatorError {
    #[error("Preparation for log materialization failed {0}")]
    LogMaterializationPreparationError(#[from] RecordSegmentReaderCreationError),
    #[error("Log materialization failed {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializedLogsError(#[from] ApplyMaterializedLogError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializedLogsErrorMetadataSegment(#[from] MetadataSegmentError),
}

impl ChromaError for ApplyLogToSegmentWriterOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ApplyLogToSegmentWriterOperatorError::LogMaterializationPreparationError(e) => e.code(),
            ApplyLogToSegmentWriterOperatorError::LogMaterializationError(e) => e.code(),
            ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(e) => e.code(),
            ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsErrorMetadataSegment(e) => {
                e.code()
            }
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOperator {}

impl ApplyLogToSegmentWriterOperator {
    pub fn new() -> Box<Self> {
        Box::new(ApplyLogToSegmentWriterOperator {})
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterInput<Writer: SegmentWriter> {
    segment_writer: Writer,
    materialize_log_output: Arc<MaterializeLogOutput>,
}

impl<Writer: SegmentWriter> ApplyLogToSegmentWriterInput<Writer> {
    pub fn new(segment_writer: Writer, materialize_log_output: Arc<MaterializeLogOutput>) -> Self {
        ApplyLogToSegmentWriterInput {
            segment_writer,
            materialize_log_output,
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOutput {}

#[async_trait]
impl<Writer: SegmentWriter + Send + Sync + Clone>
    Operator<ApplyLogToSegmentWriterInput<Writer>, ApplyLogToSegmentWriterOutput>
    for ApplyLogToSegmentWriterOperator
{
    type Error = ApplyLogToSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "ApplyLogToSegmentWriterOperator"
    }

    async fn run(
        &self,
        input: &ApplyLogToSegmentWriterInput<Writer>,
    ) -> Result<ApplyLogToSegmentWriterOutput, Self::Error> {
        let materialized_chunk = input.materialize_log_output.get_materialized_records();

        // Apply materialized records.
        match input
            .segment_writer
            .apply_materialized_log_chunk(materialized_chunk.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to record segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(e));
            }
        }

        Ok(ApplyLogToSegmentWriterOutput {})
    }
}
