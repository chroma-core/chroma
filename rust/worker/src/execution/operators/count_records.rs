use crate::{
    blockstore::provider::{BlockfileProvider, OpenError},
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::Segment,
};
use thiserror::Error;
use tonic::async_trait;

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
    RecordSegmentOpenError(#[from] OpenError),
    #[error("Error creating record segment reader")]
    RecordSegmentError(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment")]
    RecordSegmentReadError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CountRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountRecordsError::RecordSegmentError(_) => ErrorCodes::Internal,
            CountRecordsError::RecordSegmentReadError(e) => e.code(),
            CountRecordsError::RecordSegmentOpenError(e) => e.code(),
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
        let segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                match *e {
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        // This means no compaction has occured.
                        return Ok(CountRecordsOutput { count: 0 });
                    }
                    RecordSegmentReaderCreationError::BlockfileOpenError(e) => {
                        return Err(CountRecordsError::RecordSegmentOpenError(*e));
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        return Err(CountRecordsError::RecordSegmentError(*e));
                    }
                }
            }
        };

        let count = match segment_reader.count().await {
            Ok(val) => val,
            Err(e) => {
                return Err(CountRecordsError::RecordSegmentReadError(e));
            }
        };

        Ok(CountRecordsOutput { count })
    }
}
