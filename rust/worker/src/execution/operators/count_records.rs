use std::collections::HashSet;

use thiserror::Error;

use tonic::async_trait;

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{Operation, Segment},
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
    // Note: this vector needs to be in the same order as the log
    // for the counting logic to be correct.
    log_operation_and_id: Vec<(Operation, String)>,
}

impl CountRecordsInput {
    pub(crate) fn new(
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
        log_operation_and_id: Vec<(Operation, String)>,
    ) -> Self {
        Self {
            record_segment_definition,
            blockfile_provider,
            log_operation_and_id,
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
            CountRecordsError::RecordSegmentError(e) => e.code(),
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
        let reader = match segment_reader {
            Ok(r) => r,
            Err(e) => {
                println!("Error opening record segment");
                return Err(CountRecordsError::RecordSegmentError(*e));
            }
        };
        // Reconcile adds, updates and deletes.
        let mut present_id_set: HashSet<String> = HashSet::new();
        let mut res_count: i32 = 0;
        for (_, id) in &input.log_operation_and_id {
            // In theory, we can sort all the ids here
            // and send them to the reader so that the reader
            // can process all in one iteration of the sparse index.
            // In practice the blocks
            // will get cached so overall performance benefits
            // should not be significant.
            match reader.data_exists_for_user_id(id).await {
                Ok(exists) => {
                    if exists {
                        present_id_set.insert(id.clone());
                    }
                }
                Err(_) => {
                    println!("Error reading record segment");
                    return Err(CountRecordsError::RecordSegmentReadError);
                }
            }
        }
        let mut present_set_unique: HashSet<String> = present_id_set.clone();
        let mut absent_set_unique: HashSet<String> = HashSet::new();
        for (op, id) in &input.log_operation_and_id {
            if present_id_set.contains(id) {
                match op {
                    Operation::Add | Operation::Upsert => {
                        present_set_unique.insert(id.clone());
                    }
                    Operation::Delete => {
                        present_set_unique.remove(id);
                    }
                    Operation::Update => {}
                }
            } else {
                match op {
                    Operation::Add | Operation::Upsert => {
                        absent_set_unique.insert(id.clone());
                    }
                    Operation::Delete => {
                        absent_set_unique.remove(id);
                    }
                    Operation::Update => {}
                }
            }
        }
        // These are the records that are present in the record segment but have
        // been deleted more recently in the log.
        res_count -= (present_id_set.len() - present_set_unique.len()) as i32;
        // These are the records that are absent in the record segment but
        // have been inserted more recently in the log.
        res_count += absent_set_unique.len() as i32;
        // Finally, add the count from the record segment.
        match reader.count().await {
            Ok(val) => {
                res_count += val as i32;
            }
            Err(_) => {
                println!("Error reading record segment");
                return Err(CountRecordsError::RecordSegmentReadError);
            }
        };
        Ok(CountRecordsOutput {
            count: res_count as usize,
        })
    }
}
