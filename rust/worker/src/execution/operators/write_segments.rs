use crate::execution::operator::Operator;
use crate::execution::orchestration::CompactWriters;
use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializerError;
use crate::segment::MaterializeLogsResult;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_types::Segment;
use thiserror::Error;
use tracing::Instrument;

#[derive(Error, Debug)]
pub enum WriteSegmentsOperatorError {
    #[error("Preparation for log materialization failed {0}")]
    LogMaterializationPreparationError(#[from] RecordSegmentReaderCreationError),
    #[error("Log materialization failed {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializedLogsError(#[from] ApplyMaterializedLogError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializedLogsErrorMetadataSegment(#[from] MetadataSegmentError),
}

impl ChromaError for WriteSegmentsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            WriteSegmentsOperatorError::LogMaterializationPreparationError(e) => e.code(),
            WriteSegmentsOperatorError::LogMaterializationError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializedLogsError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializedLogsErrorMetadataSegment(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOperator {}

impl WriteSegmentsOperator {
    pub fn new() -> Box<Self> {
        Box::new(WriteSegmentsOperator {})
    }
}

#[derive(Debug)]
pub struct WriteSegmentsInput {
    writers: CompactWriters,
    provider: BlockfileProvider,
    record_segment: Segment,
    materialized_logs: MaterializeLogsResult,
}

impl WriteSegmentsInput {
    pub fn new(
        writers: CompactWriters,
        provider: BlockfileProvider,
        record_segment: Segment,
        materialized_logs: MaterializeLogsResult,
    ) -> Self {
        WriteSegmentsInput {
            writers,
            provider,
            record_segment,
            materialized_logs,
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOutput {
    pub(crate) writers: CompactWriters,
}

#[async_trait]
impl Operator<WriteSegmentsInput, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = WriteSegmentsOperatorError;

    fn get_name(&self) -> &'static str {
        "WriteSegmentsOperator"
    }

    async fn run(&self, input: &WriteSegmentsInput) -> Result<WriteSegmentsOutput, Self::Error> {
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
                            WriteSegmentsOperatorError::LogMaterializationPreparationError(
                                RecordSegmentReaderCreationError::BlockfileOpenError(e),
                            ),
                        );
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(
                            WriteSegmentsOperatorError::LogMaterializationPreparationError(
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
                            WriteSegmentsOperatorError::LogMaterializationPreparationError(*e),
                        );
                    }
                    RecordSegmentReaderCreationError::UserRecordNotFound(ref c) => {
                        tracing::error!(
                            "Error creating record segment reader: user {} not found",
                            c
                        );
                        return Err(
                            WriteSegmentsOperatorError::LogMaterializationPreparationError(*e),
                        );
                    }
                };
            }
        };

        // Apply materialized records.
        input
            .writers
            .record
            .apply_materialized_log_chunk(&record_segment_reader, &input.materialized_logs)
            .instrument(tracing::trace_span!(
                "Apply materialized logs to record segment"
            ))
            .await
            .map_err(WriteSegmentsOperatorError::ApplyMaterializedLogsError)?;
        tracing::debug!("Applied materialized records to record segment");

        input
            .writers
            .metadata
            .apply_materialized_log_chunk(&record_segment_reader, &input.materialized_logs)
            .instrument(tracing::trace_span!(
                "Apply materialized logs to metadata segment"
            ))
            .await
            .map_err(WriteSegmentsOperatorError::ApplyMaterializedLogsError)?;
        tracing::debug!("Applied materialized records to metadata segment");

        input
            .writers
            .vector
            .apply_materialized_log_chunk(&record_segment_reader, &input.materialized_logs)
            .instrument(tracing::trace_span!(
                "Apply materialized logs to HNSW segment"
            ))
            .await
            .map_err(WriteSegmentsOperatorError::ApplyMaterializedLogsError)?;
        tracing::debug!("Applied Materialized Records to HNSW Segment");

        Ok(WriteSegmentsOutput {
            writers: input.writers.clone(),
        })
    }
}
