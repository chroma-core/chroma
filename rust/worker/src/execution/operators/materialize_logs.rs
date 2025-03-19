use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError};
use chroma_segment::types::{materialize_logs, LogMaterializerError, MaterializeLogsResult};
use chroma_system::Operator;
use chroma_types::{chroma_proto, Chunk, DataRecord, LogRecord, Segment};
use futures::TryFutureExt;
use prost::Message;
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

#[derive(Debug)]
pub struct MaterializeLogOutput {
    pub result: MaterializeLogsResult,
    pub logical_size_delta: i64,
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
            input.logs.clone(),
            Some(input.offset_id.clone()),
        )
        .map_err(MaterializeLogOperatorError::LogMaterializationFailed)
        .await?;

        let mut logical_size_delta = 0;
        for record in &result {
            let hydrated = record.hydrate(record_segment_reader.as_ref()).await?;
            let old_size = hydrated
                .get_data_record()
                .map(DataRecord::get_size)
                .unwrap_or_default() as i64;
            let merged_metadata = hydrated.merged_metadata();
            // NOTE: The size calculation should mirror DataRecord::get_size
            let new_size = (hydrated.get_user_id().len()
                + size_of_val(hydrated.merged_embeddings_ref())
                + if merged_metadata.is_empty() {
                    0
                } else {
                    chroma_proto::UpdateMetadata::from(merged_metadata)
                        .encode_to_vec()
                        .len()
                }
                + hydrated
                    .merged_document_ref()
                    .map(|doc| doc.len())
                    .unwrap_or_default()) as i64;
            logical_size_delta += new_size - old_size;
        }

        Ok(MaterializeLogOutput {
            result,
            logical_size_delta,
        })
    }
}
