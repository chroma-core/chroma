use thiserror::Error;

use tonic::async_trait;

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::Segment,
};

#[derive(Debug)]
pub(crate) struct CountRecordsOperator {}

impl CountRecordsOperator {
    pub(crate) fn new() -> Box<Self> {
        Box::new(CountRecordsOperator {})
    }
}

#[derive(Debug)]
pub(crate) struct CountRecordsInput {
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
}

impl CountRecordsInput {
    pub(crate) fn new(
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
    ) -> Self {
        Self {
            record_segment_definition,
            blockfile_provider,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CountRecordsOutput {
    pub(crate) count: usize,
}

#[derive(Error, Debug)]
pub(crate) enum CountRecordsError {
    #[error("Error reading record segment reader")]
    RecordSegmentReadError,
    #[error("Error creating record segment reader")]
    RecordSegmentError(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for CountRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountRecordsError::RecordSegmentError(_) => ErrorCodes::Internal,
            CountRecordsError::RecordSegmentReadError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<CountRecordsInput, CountRecordsOutput> for CountRecordsOperator {
    type Error = CountRecordsError;
    async fn run(
        &self,
        input: &CountRecordsInput,
    ) -> Result<CountRecordsOutput, CountRecordsError> {
        let segment_reader = RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await;
        match segment_reader {
            Ok(reader) => match reader.count().await {
                Ok(val) => {
                    return Ok(CountRecordsOutput { count: val });
                }
                Err(_) => {
                    println!("Error reading record segment");
                    return Err(CountRecordsError::RecordSegmentReadError);
                }
            },
            Err(e) => {
                println!("Error opening record segment");
                return Err(CountRecordsError::RecordSegmentError(*e));
            }
        }
    }
}
