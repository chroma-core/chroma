use super::materialize_logs::MaterializeLogOutput;
use crate::execution::operator::Operator;
use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::ChromaSegmentWriter;
use crate::segment::LogMaterializerError;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use std::sync::Arc;
use thiserror::Error;
use tracing::Instrument;

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
pub struct ApplyLogToSegmentWriterInput<'a> {
    segment_writer: ChromaSegmentWriter<'a>,
    materialize_log_output: Arc<MaterializeLogOutput>,
}

impl<'a> ApplyLogToSegmentWriterInput<'a> {
    pub fn new(
        segment_writer: ChromaSegmentWriter<'a>,
        materialize_log_output: Arc<MaterializeLogOutput>,
    ) -> Self {
        ApplyLogToSegmentWriterInput {
            segment_writer,
            materialize_log_output,
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOutput<'a> {
    pub segment_writer: ChromaSegmentWriter<'a>,
}

#[async_trait]
impl<'a> Operator<ApplyLogToSegmentWriterInput<'a>, ApplyLogToSegmentWriterOutput<'a>>
    for ApplyLogToSegmentWriterOperator
{
    type Error = ApplyLogToSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "ApplyLogToSegmentWriterOperator"
    }

    async fn run(
        &self,
        input: &ApplyLogToSegmentWriterInput<'a>,
    ) -> Result<ApplyLogToSegmentWriterOutput<'a>, Self::Error> {
        let materialized_chunk = input.materialize_log_output.get_materialized_records();
        tracing::debug!("Materializing {} records", materialized_chunk.len());

        // Apply materialized records.
        match input
            .segment_writer
            .apply_materialized_log_chunk(materialized_chunk.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs",
                otel.name = format!(
                    "Apply materialized logs to segment writer {}",
                    input.segment_writer.get_name()
                ),
                segment = input.segment_writer.get_name()
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(e));
            }
        }

        Ok(ApplyLogToSegmentWriterOutput {
            segment_writer: input.segment_writer.clone(),
        })
    }
}
