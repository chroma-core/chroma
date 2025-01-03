use crate::execution::operator::Operator;
use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializerError;
use crate::segment::MaterializeLogsResult;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use thiserror::Error;
use tracing::Instrument;

#[derive(Error, Debug)]
pub enum ApplyLogToSegmentWriterOperatorError {
    #[error("Log materialization result is empty")]
    LogMaterializationResultEmpty,
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
            ApplyLogToSegmentWriterOperatorError::LogMaterializationResultEmpty => {
                ErrorCodes::Internal
            }
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
pub struct ApplyLogToSegmentWriterInput<'bf, Writer: SegmentWriter> {
    segment_writer: Writer,
    materialized_logs: MaterializeLogsResult,
    record_segment_reader: Option<RecordSegmentReader<'bf>>,
}

impl<'bf, Writer: SegmentWriter> ApplyLogToSegmentWriterInput<'bf, Writer> {
    pub fn new(
        segment_writer: Writer,
        materialized_logs: MaterializeLogsResult,
        record_segment_reader: Option<RecordSegmentReader<'bf>>,
    ) -> Self {
        ApplyLogToSegmentWriterInput {
            segment_writer,
            materialized_logs,
            record_segment_reader,
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOutput {}

#[async_trait]
impl<Writer: SegmentWriter + Send + Sync + Clone>
    Operator<ApplyLogToSegmentWriterInput<'_, Writer>, ApplyLogToSegmentWriterOutput>
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
        if input.materialized_logs.is_empty() {
            return Err(ApplyLogToSegmentWriterOperatorError::LogMaterializationResultEmpty);
        }

        // Apply materialized records.
        match input
            .segment_writer
            .apply_materialized_log_chunk(&input.record_segment_reader, &input.materialized_logs)
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

        Ok(ApplyLogToSegmentWriterOutput {})
    }
}
