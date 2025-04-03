use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::RecordSegmentReader;
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord, OperationRecord};
use futures::{StreamExt, TryStreamExt};
use thiserror::Error;

/// The `SourceRecordSegmentOperator` generates logs from the data in the record segment
///
/// # Parameters
/// None
///
/// # Inputs
/// - `record_reader`: The record segment reader, if the collection is initialized
///
/// # Outputs
/// - Chunk of addition logs
#[derive(Clone, Debug)]
pub struct SourceRecordSegmentOperator {}

#[derive(Clone, Debug)]
pub struct SourceRecordSegmentInput {
    pub record_segment_reader: Option<RecordSegmentReader<'static>>,
}

pub type SourceRecordSegmentOutput = Chunk<LogRecord>;

#[derive(Debug, Error)]
pub enum SourceRecordSegmentError {
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
}

impl ChromaError for SourceRecordSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
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
        let logs = match input.record_segment_reader.as_ref() {
            Some(reader) => {
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
            None => Default::default(),
        };
        Ok(Chunk::new(logs.into()))
    }
}
