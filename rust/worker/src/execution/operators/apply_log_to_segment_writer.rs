use crate::execution::operator::Operator;
use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializer;
use crate::segment::LogMaterializerError;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_types::Chunk;
use chroma_types::LogRecord;
use chroma_types::Segment;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tracing::Instrument;
use tracing::Span;

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
    chunk: Chunk<LogRecord>,
    provider: BlockfileProvider,
    record_segment: Segment,
    offset_id: Arc<AtomicU32>,
}

impl<Writer: SegmentWriter> ApplyLogToSegmentWriterInput<Writer> {
    pub fn new(
        segment_writer: Writer,
        chunk: Chunk<LogRecord>,
        provider: BlockfileProvider,
        record_segment: Segment,
        offset_id: Arc<AtomicU32>,
    ) -> Self {
        ApplyLogToSegmentWriterInput {
            segment_writer,
            chunk,
            provider,
            record_segment,
            offset_id,
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogToSegmentWriterOutput {
    // pub(crate) segment_writer: Writer,
}

#[async_trait]
impl<Writer: SegmentWriter + Send + Sync + Clone>
    Operator<ApplyLogToSegmentWriterInput<Writer>, ApplyLogToSegmentWriterOutput>
    for ApplyLogToSegmentWriterOperator
{
    type Error = ApplyLogToSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "WriteSegmentsOperator"
    }

    async fn run(
        &self,
        input: &ApplyLogToSegmentWriterInput<Writer>,
    ) -> Result<ApplyLogToSegmentWriterOutput, Self::Error> {
        tracing::debug!("Materializing {} records", input.chunk.len());

        // Prepare for log materialization.
        let record_segment_reader: Option<RecordSegmentReader>;
        match RecordSegmentReader::from_segment(&input.record_segment, &input.provider).await {
            Ok(reader) => {
                record_segment_reader = Some(reader);
            }
            Err(e) => {
                match *e {
                    // Uninitialized segment is fine and means that the record
                    // segment is not yet initialized in storage.
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        record_segment_reader = None;
                    }
                    RecordSegmentReaderCreationError::BlockfileOpenError(e) => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(
                            ApplyLogToSegmentWriterOperatorError::LogMaterializationPreparationError(
                                RecordSegmentReaderCreationError::BlockfileOpenError(e),
                            ),
                        );
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(
                            ApplyLogToSegmentWriterOperatorError::LogMaterializationPreparationError(
                                RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                            ),
                        );
                    }
                    RecordSegmentReaderCreationError::DataRecordNotFound(c) => {
                        tracing::error!(
                            "Error creating record segment reader: offset {} not found",
                            c
                        );
                        return Err(
                            ApplyLogToSegmentWriterOperatorError::LogMaterializationPreparationError(*e),
                        );
                    }
                    RecordSegmentReaderCreationError::UserRecordNotFound(ref c) => {
                        tracing::error!(
                            "Error creating record segment reader: user {} not found",
                            c
                        );
                        return Err(
                            ApplyLogToSegmentWriterOperatorError::LogMaterializationPreparationError(*e),
                        );
                    }
                };
            }
        };
        let materializer = LogMaterializer::new(
            record_segment_reader,
            input.chunk.clone(),
            Some(input.offset_id.clone()),
        );
        // Materialize the logs.
        let res = match materializer
            .materialize()
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::error!("Error materializing records {}", e);
                return Err(ApplyLogToSegmentWriterOperatorError::LogMaterializationError(e));
            }
        };
        // Apply materialized records.
        match input
            .segment_writer
            .apply_materialized_log_chunk(res.clone())
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
