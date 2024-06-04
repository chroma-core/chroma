use crate::blockstore::provider::BlockfileProvider;
use crate::errors::ChromaError;
use crate::segment::record_segment::ApplyMaterializedLogError;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::LogMaterializer;
use crate::segment::LogMaterializerError;
use crate::segment::SegmentWriter;
use crate::types::Segment;
use crate::{
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegmentWriter, record_segment::RecordSegmentWriter,
    },
    types::LogRecord,
};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WriteSegmentsOperatorError {
    #[error("Preparation for log materialization failed {0}")]
    LogMaterializationPreparationError(#[from] RecordSegmentReaderCreationError),
    #[error("Log materialization failed {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Materialized logs failed to apply {0}")]
    ApplyMaterializatedLogsError(#[from] ApplyMaterializedLogError),
}

impl ChromaError for WriteSegmentsOperatorError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            WriteSegmentsOperatorError::LogMaterializationPreparationError(e) => e.code(),
            WriteSegmentsOperatorError::LogMaterializationError(e) => e.code(),
            WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e) => e.code(),
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
    chunk: Chunk<LogRecord>,
    provider: BlockfileProvider,
    record_segment: Segment,
}

impl<'me> WriteSegmentsInput {
    pub fn new(
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
        chunk: Chunk<LogRecord>,
        provider: BlockfileProvider,
        record_segment: Segment,
    ) -> Self {
        WriteSegmentsInput {
            record_segment_writer,
            hnsw_segment_writer,
            chunk,
            provider,
            record_segment,
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOutput {
    pub(crate) record_segment_writer: RecordSegmentWriter,
    pub(crate) hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
}

#[async_trait]
impl Operator<WriteSegmentsInput, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = WriteSegmentsOperatorError;

    async fn run(&self, input: &WriteSegmentsInput) -> Result<WriteSegmentsOutput, Self::Error> {
        tracing::debug!("Materializing N Records: {:?}", input.chunk.len());
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
                };
            }
        };
        let materializer = LogMaterializer::new(record_segment_reader, input.chunk.clone());
        // Materialize the logs.
        let res = match materializer.materialize().await {
            Ok(records) => records,
            Err(e) => {
                tracing::error!("Error materializing records {}", e);
                return Err(WriteSegmentsOperatorError::LogMaterializationError(e));
            }
        };
        // Apply materialized records.
        match input
            .record_segment_writer
            .apply_materialized_log_chunk(res.clone())
            .await
        {
            Ok(()) => (),
            Err(e) => {
                return Err(WriteSegmentsOperatorError::ApplyMaterializatedLogsError(e));
            }
        }
        tracing::debug!("Applied materialized records to record segment");
        match input
            .hnsw_segment_writer
            .apply_materialized_log_chunk(res)
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
        })
    }
}
