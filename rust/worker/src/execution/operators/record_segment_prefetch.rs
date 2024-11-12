use crate::{
    execution::operator::{Operator, OperatorType},
    segment::record_segment::RecordSegmentReader,
};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Segment;
use thiserror::Error;
use tonic::async_trait;

#[derive(Debug)]
pub(crate) struct OffsetIdToDataKeys {
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) struct OffsetIdToUserIdKeys {
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) enum Keys {
    OffsetIdToDataKeys(OffsetIdToDataKeys),
    OffsetIdToUserIdKeys(OffsetIdToUserIdKeys),
}

#[derive(Debug)]
pub(crate) struct RecordSegmentPrefetchIoInput {
    pub(crate) keys: Keys,
    pub(crate) segment: Segment,
    pub(crate) provider: BlockfileProvider,
}

#[derive(Debug)]
pub(crate) struct RecordSegmentPrefetchIoOutput {
    // This is fire and forget so nothing to return.
}

#[derive(Debug)]
pub(crate) struct RecordSegmentPrefetchIoOperator {}

impl RecordSegmentPrefetchIoOperator {
    pub fn new() -> Box<Self> {
        Box::new(RecordSegmentPrefetchIoOperator {})
    }
}

#[derive(Error, Debug)]
pub(crate) enum RecordSegmentPrefetchIoOperatorError {
    #[error("Error creating Record Segment reader")]
    RecordSegmentReaderCreationError,
}

impl ChromaError for RecordSegmentPrefetchIoOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::RecordSegmentReaderCreationError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<RecordSegmentPrefetchIoInput, RecordSegmentPrefetchIoOutput>
    for RecordSegmentPrefetchIoOperator
{
    type Error = RecordSegmentPrefetchIoOperatorError;

    fn get_name(&self) -> &'static str {
        "RecordSegmentPrefetchIoOperator"
    }

    async fn run(
        &self,
        input: &RecordSegmentPrefetchIoInput,
    ) -> Result<RecordSegmentPrefetchIoOutput, Self::Error> {
        match &input.keys {
            Keys::OffsetIdToDataKeys(keys) => {
                if keys.keys.is_empty() {
                    return Ok(RecordSegmentPrefetchIoOutput {});
                }
                // Construct record segment reader.
                let record_segment_reader = match RecordSegmentReader::from_segment(
                    &input.segment,
                    &input.provider,
                )
                .await
                {
                    Ok(reader) => reader,
                    Err(_) => {
                        return Err(
                            RecordSegmentPrefetchIoOperatorError::RecordSegmentReaderCreationError,
                        );
                    }
                };
                record_segment_reader.prefetch_id_to_data(&keys.keys).await;
            }
            Keys::OffsetIdToUserIdKeys(keys) => {
                if keys.keys.is_empty() {
                    return Ok(RecordSegmentPrefetchIoOutput {});
                }
                // Construct record segment reader.
                let record_segment_reader = match RecordSegmentReader::from_segment(
                    &input.segment,
                    &input.provider,
                )
                .await
                {
                    Ok(reader) => reader,
                    Err(_) => {
                        return Err(
                            RecordSegmentPrefetchIoOperatorError::RecordSegmentReaderCreationError,
                        );
                    }
                };
                record_segment_reader
                    .prefetch_id_to_user_id(&keys.keys)
                    .await;
            }
        }
        Ok(RecordSegmentPrefetchIoOutput {})
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
