use std::collections::HashSet;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, LogRecord, Segment};
use thiserror::Error;
use tonic::async_trait;
use tracing::{trace, Instrument, Span};

use crate::{
    execution::operator::Operator,
    segment::{
        materialize_logs,
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializerError,
    },
};

/// The `PrefetchRecordOperator` prefetches the relevant records from the record segments to the cache
///
/// # Parameters
/// None
///
/// # Inputs
/// - `logs`: The latest logs of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `record_segment`: The record segment information
/// - `offset_ids`: The offset ids of the records to prefetch
///
/// # Outputs
/// None
///
/// # Usage
/// It can be used to populate cache with relevant data in parallel
#[derive(Debug)]
pub struct PrefetchRecordOperator {}

#[derive(Debug)]
pub struct PrefetchRecordInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub offset_ids: Vec<u32>,
}

pub type PrefetchRecordOutput = ();

#[derive(Error, Debug)]
pub enum PrefetchRecordError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for PrefetchRecordError {
    fn code(&self) -> ErrorCodes {
        match self {
            PrefetchRecordError::LogMaterializer(e) => e.code(),
            PrefetchRecordError::RecordReader(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<PrefetchRecordInput, PrefetchRecordOutput> for PrefetchRecordOperator {
    type Error = PrefetchRecordError;

    async fn run(
        &self,
        input: &PrefetchRecordInput,
    ) -> Result<PrefetchRecordOutput, PrefetchRecordError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                return Ok(())
            }
            Err(e) => return Err((*e).into()),
        };

        let some_reader = Some(record_segment_reader.clone());
        let materialized_logs = materialize_logs(&some_reader, &input.logs, None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        let mut record_segment_offset_ids: HashSet<_> =
            HashSet::from_iter(input.offset_ids.iter().cloned());
        for (log, _) in materialized_logs.iter() {
            record_segment_offset_ids.remove(&log.offset_id);
        }

        record_segment_reader
            .prefetch_id_to_data(&record_segment_offset_ids.into_iter().collect::<Vec<_>>())
            .await;

        Ok(())
    }
}
