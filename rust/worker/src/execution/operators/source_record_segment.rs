use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError, MaterializeLogsResult},
};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord, OperationRecord, Segment};
use futures::{StreamExt, TryStreamExt};
use thiserror::Error;

/// The `SourceRecordSegmentOperator` generates materialized logs from the data in the record segment
///
/// # Parameters
/// None
///
/// # Inputs
/// - `blockfile_provider`: The blockfile_provider
/// - `record_segment`: The record segment information
///
/// # Outputs
/// - `materialized_logs`: The materialized log generated from record segement data
/// - `collection_logical_size`: The logical size of the collection
#[derive(Clone, Debug)]
pub struct SourceRecordSegmentOperator {}

#[derive(Clone, Debug)]
pub struct SourceRecordSegmentInput {
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct SourceRecordSegmentOutput {
    pub materialized_logs: MaterializeLogsResult,
    pub collection_record_count: u64,
    pub collection_logical_size_bytes: u64,
}

#[derive(Debug, Error)]
pub enum SourceRecordSegmentError {
    #[error("Error materializing log: {0}")]
    LogMaterialization(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
}

impl ChromaError for SourceRecordSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            SourceRecordSegmentError::LogMaterialization(e) => e.code(),
            SourceRecordSegmentError::RecordReader(e) => e.code(),
            SourceRecordSegmentError::RecordSegment(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<SourceRecordSegmentInput, SourceRecordSegmentOutput> for SourceRecordSegmentOperator {
    type Error = SourceRecordSegmentError;
    async fn run(
        &self,
        input: &SourceRecordSegmentInput,
    ) -> Result<SourceRecordSegmentOutput, SourceRecordSegmentError> {
        tracing::trace!("[{}]: {:?}", self.get_name(), input);
        let raw_logs = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => {
                reader
                    .get_data_stream(..)
                    .await
                    .enumerate()
                    .map(|(offset, res)| {
                        res.map(|rec| LogRecord {
                            // Log offset starts with 1
                            log_offset: offset as i64 + 1,
                            record: OperationRecord {
                                id: rec.id.to_string(),
                                embedding: Some(rec.embedding.to_vec()),
                                encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
                                metadata: rec.metadata.map(|meta| {
                                    meta.into_iter().map(|(k, v)| (k, v.into())).collect()
                                }),
                                document: rec.document.map(ToString::to_string),
                                operation: chroma_types::Operation::Add,
                            },
                        })
                    })
                    .try_collect::<Vec<_>>()
                    .await?
            }
            Err(e) => return Err((*e).into()),
        };
        let collection_record_count = u64::try_from(raw_logs.len()).unwrap_or_default();
        let materialized_logs = materialize_logs(&None, Chunk::new(raw_logs.into()), None).await?;
        let mut collection_logical_size_bytes = 0;
        for record in &materialized_logs {
            // This should always be positive because the operation is always add
            collection_logical_size_bytes += record
                .hydrate(None)
                .await?
                .compute_logical_size_delta_bytes();
        }
        Ok(SourceRecordSegmentOutput {
            materialized_logs,
            collection_record_count,
            collection_logical_size_bytes: u64::try_from(collection_logical_size_bytes)
                .unwrap_or_default(),
        })
    }
}
