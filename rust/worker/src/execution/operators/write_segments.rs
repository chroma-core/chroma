use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializerError;
use crate::segment::MaterializedLogRecord;
use crate::segment::SegmentWriter;
use crate::{
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegmentWriter, record_segment::RecordSegmentWriter,
    },
};
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_types::Chunk;
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
    #[error("Uninitialized writer")]
    UninitializedWriter,
}

impl ChromaError for WriteSegmentsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            WriteSegmentsOperatorError::LogMaterializationPreparationError(e) => e.code(),
            WriteSegmentsOperatorError::LogMaterializationError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializedLogsError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializedLogsErrorMetadataSegment(e) => e.code(),
            WriteSegmentsOperatorError::UninitializedWriter => ErrorCodes::Internal,
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
    writers: Option<(
        RecordSegmentWriter,
        Box<DistributedHNSWSegmentWriter>,
        MetadataSegmentWriter<'static>,
    )>,
    materialized_logs: Chunk<MaterializedLogRecord>,
}

impl WriteSegmentsInput {
    pub fn new(
        writers: Option<(
            RecordSegmentWriter,
            Box<DistributedHNSWSegmentWriter>,
            MetadataSegmentWriter<'static>,
        )>,
        materialized_logs: Chunk<MaterializedLogRecord>,
    ) -> Self {
        WriteSegmentsInput {
            writers,
            materialized_logs,
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOutput {
    pub(crate) writers: Option<(
        RecordSegmentWriter,
        Box<DistributedHNSWSegmentWriter>,
        MetadataSegmentWriter<'static>,
    )>,
}

#[async_trait]
impl Operator<WriteSegmentsInput, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = WriteSegmentsOperatorError;

    fn get_name(&self) -> &'static str {
        "WriteSegmentsOperator"
    }

    async fn run(&self, input: &WriteSegmentsInput) -> Result<WriteSegmentsOutput, Self::Error> {
        let (record_segment_writer, hnsw_segment_writer, metadata_segment_writer) = input
            .writers
            .as_ref()
            .ok_or(WriteSegmentsOperatorError::UninitializedWriter)?;

        // Apply materialized records.
        match record_segment_writer
            .apply_materialized_log_chunk(input.materialized_logs.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to record segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializedLogsError(e));
            }
        }
        tracing::debug!("Applied materialized records to record segment");
        match metadata_segment_writer
            .apply_materialized_log_chunk(input.materialized_logs.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to metadata segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializedLogsError(e));
            }
        }
        tracing::debug!("Applied materialized records to metadata segment");
        match hnsw_segment_writer
            .apply_materialized_log_chunk(input.materialized_logs.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to HNSW segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializedLogsError(e));
            }
        }

        tracing::debug!("Applied Materialized Records to HNSW Segment");
        Ok(WriteSegmentsOutput {
            writers: Some((
                record_segment_writer.clone(),
                hnsw_segment_writer.clone(),
                metadata_segment_writer.clone(),
            )),
        })
    }
}
