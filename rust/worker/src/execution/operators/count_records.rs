use tonic::async_trait;

use crate::{
    blockstore::provider::BlockfileProvider, errors::ChromaError, execution::operator::Operator,
    segment::record_segment::RecordSegmentReader, types::Segment,
};

use super::merge_metadata_results::MergeMetadataResultsOperatorError;

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

#[async_trait]
impl Operator<CountRecordsInput, CountRecordsOutput> for CountRecordsOperator {
    type Error = MergeMetadataResultsOperatorError;
    async fn run(
        &self,
        input: &CountRecordsInput,
    ) -> Result<CountRecordsOutput, MergeMetadataResultsOperatorError> {
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
                    return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                }
            },
            Err(_) => {
                return Err(MergeMetadataResultsOperatorError::RecordSegmentError);
            }
        }
    }
}
