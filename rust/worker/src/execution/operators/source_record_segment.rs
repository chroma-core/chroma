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
                        res.map(|(_, rec)| LogRecord {
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

#[cfg(test)]
mod tests {
    use chroma_log::test::{int_as_id, upsert_generator, LoadFromGenerator};
    use chroma_segment::{blockfile_record::RecordSegmentReader, test::TestDistributedSegment};
    use chroma_system::Operator;
    use chroma_types::Operation;

    use crate::execution::operators::source_record_segment::SourceRecordSegmentOperator;

    use super::SourceRecordSegmentInput;

    /// The unit tests for `SourceRecordSegmentOperator` uses the following test data
    /// It generates 100 log records and compact them
    async fn setup_source_input() -> SourceRecordSegmentInput {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(100, upsert_generator)
            .await;
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
        ))
        .await
        .expect("Record segment reader should be initialized");
        SourceRecordSegmentInput {
            record_segment_reader: Some(reader),
        }
    }

    #[tokio::test]
    async fn test_source() {
        let source_input = Box::pin(setup_source_input()).await;

        let source_operator = SourceRecordSegmentOperator {};

        let source_output = source_operator
            .run(&source_input)
            .await
            .expect("SourceOperator should not fail");

        assert_eq!(source_output.len(), 100);
        for (offset, (record, _)) in source_output.iter().enumerate() {
            assert_eq!(record.log_offset, offset as i64 + 1);
            assert_eq!(record.record.id, int_as_id(offset + 1));
            assert_eq!(record.record.operation, Operation::Add);
        }
    }
}
