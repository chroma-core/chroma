use crate::segment::metadata_segment::MetadataSegmentError;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializerError;
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
use thiserror::Error;
use tracing::Instrument;

use super::materialize_logs::MaterializeLogOutput;

#[derive(Error, Debug)]
pub enum WriteSegmentsOperatorError {
    #[error("Preparation for log materialization failed {0}")]
    LogMaterializationPreparationError(#[from] RecordSegmentReaderCreationError),
    #[error("Log materialization failed {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializatedLogsError(#[from] ApplyMaterializedLogError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializatedLogsErrorMetadataSegment(#[from] MetadataSegmentError),
}

impl ChromaError for WriteSegmentsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            WriteSegmentsOperatorError::LogMaterializationPreparationError(e) => e.code(),
            WriteSegmentsOperatorError::LogMaterializationError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializatedLogsErrorMetadataSegment(e) => e.code(),
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
    record_segment_writer: RecordSegmentWriter,
    hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
    metadata_segment_writer: MetadataSegmentWriter<'static>,
    materialize_log_output: MaterializeLogOutput,
}

impl WriteSegmentsInput {
    pub fn new(
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
        metadata_segment_writer: MetadataSegmentWriter<'static>,
        materialize_log_output: MaterializeLogOutput,
    ) -> Self {
        WriteSegmentsInput {
            record_segment_writer,
            hnsw_segment_writer,
            metadata_segment_writer,
            materialize_log_output,
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOutput {
    pub(crate) record_segment_writer: RecordSegmentWriter,
    pub(crate) hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
    pub(crate) metadata_segment_writer: MetadataSegmentWriter<'static>,
}

#[async_trait]
impl Operator<WriteSegmentsInput, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = WriteSegmentsOperatorError;

    fn get_name(&self) -> &'static str {
        "WriteSegmentsOperator"
    }

    async fn run(&self, input: &WriteSegmentsInput) -> Result<WriteSegmentsOutput, Self::Error> {
        let materialized_chunk = input.materialize_log_output.get_materialized_records();

        // Apply materialized records.
        match input
            .record_segment_writer
            .apply_materialized_log_chunk(materialized_chunk.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to record segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e));
            }
        }
        tracing::debug!("Applied materialized records to record segment");
        match input
            .metadata_segment_writer
            .apply_materialized_log_chunk(materialized_chunk.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to metadata segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e));
            }
        }
        tracing::debug!("Applied materialized records to metadata segment");
        match input
            .hnsw_segment_writer
            .apply_materialized_log_chunk(materialized_chunk.clone())
            .instrument(tracing::trace_span!(
                "Apply materialized logs to HNSW segment"
            ))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e));
            }
        }
        tracing::debug!("Applied Materialized Records to HNSW Segment");
        Ok(WriteSegmentsOutput {
            record_segment_writer: input.record_segment_writer.clone(),
            hnsw_segment_writer: input.hnsw_segment_writer.clone(),
            metadata_segment_writer: input.metadata_segment_writer.clone(),
        })
    }
}
