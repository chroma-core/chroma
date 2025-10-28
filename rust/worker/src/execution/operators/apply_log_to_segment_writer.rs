use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_metadata::MetadataSegmentError,
    blockfile_record::{
        ApplyMaterializedLogError, RecordSegmentReader, RecordSegmentReaderCreationError,
    },
    types::{ChromaSegmentWriter, LogMaterializerError, MaterializeLogsResult},
};
use chroma_system::Operator;
use chroma_types::{Schema, SegmentUuid};
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
pub struct ApplyLogToSegmentWriterInput<'bf> {
    segment_writer: ChromaSegmentWriter<'bf>,
    materialized_logs: MaterializeLogsResult,
    record_segment_reader: Option<RecordSegmentReader<'bf>>,
    schema: Option<Schema>,
}

impl<'bf> ApplyLogToSegmentWriterInput<'bf> {
    pub fn new(
        segment_writer: ChromaSegmentWriter<'bf>,
        materialized_logs: MaterializeLogsResult,
        record_segment_reader: Option<RecordSegmentReader<'bf>>,
        schema: Option<Schema>,
    ) -> Self {
        ApplyLogToSegmentWriterInput {
            segment_writer,
            materialized_logs,
            record_segment_reader,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOutput {
    pub segment_id: SegmentUuid,
    pub segment_type: &'static str,
    pub schema_update: Option<Schema>,
}

#[async_trait]
impl Operator<ApplyLogToSegmentWriterInput<'_>, ApplyLogToSegmentWriterOutput>
    for ApplyLogToSegmentWriterOperator
{
    type Error = ApplyLogToSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "ApplyLogToSegmentWriterOperator"
    }

    async fn run(
        &self,
        input: &ApplyLogToSegmentWriterInput,
    ) -> Result<ApplyLogToSegmentWriterOutput, Self::Error> {
        if input.materialized_logs.is_empty() {
            return Err(ApplyLogToSegmentWriterOperatorError::LogMaterializationResultEmpty);
        }

        // Apply materialized records.
        let schema_update = match input
            .segment_writer
            .apply_materialized_log_chunk(
                &input.record_segment_reader,
                &input.materialized_logs,
                input.schema.clone(),
            )
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
            Ok(schema_update) => schema_update,
            Err(e) => {
                return Err(ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(e));
            }
        };

        Ok(ApplyLogToSegmentWriterOutput {
            segment_id: input.segment_writer.get_id(),
            segment_type: input.segment_writer.get_name(),
            schema_update,
        })
    }
}
