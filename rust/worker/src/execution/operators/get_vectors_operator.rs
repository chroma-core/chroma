use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::record_segment::{self, RecordSegmentReader},
    types::{LogRecord, Segment},
};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug)]
pub struct GetVectorsOperator {}

impl GetVectorsOperator {
    pub fn new() -> Box<Self> {
        return Box::new(GetVectorsOperator {});
    }
}

/// The input to the get vectors operator.
/// # Parameters
/// * `record_segment_definition` - The segment definition for the record segment.
/// * `blockfile_provider` - The blockfile provider.
/// * `log_records` - The log records.
/// * `search_user_ids` - The user ids to search for.
#[derive(Debug)]
pub struct GetVectorsOperatorInput {
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
    log_records: Chunk<LogRecord>,
    search_user_ids: Vec<String>,
}

impl GetVectorsOperatorInput {
    pub fn new(
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
        log_records: Chunk<LogRecord>,
        search_user_ids: Vec<String>,
    ) -> Self {
        return GetVectorsOperatorInput {
            record_segment_definition,
            blockfile_provider,
            log_records,
            search_user_ids,
        };
    }
}

/// The output of the get vectors operator.
/// # Parameters
/// * `ids` - The ids of the vectors.
/// * `vectors` - The vectors.
/// # Notes
/// The vectors are in the same order as the ids.
#[derive(Debug)]
pub struct GetVectorsOperatorOutput {
    pub(crate) ids: Vec<String>,
    pub(crate) vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Error)]
pub enum GetVectorsOperatorError {
    #[error("Error creating record segment reader {0}")]
    RecordSegmentReaderCreationError(
        #[from] crate::segment::record_segment::RecordSegmentReaderCreationError,
    ),
}

impl ChromaError for GetVectorsOperatorError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<GetVectorsOperatorInput, GetVectorsOperatorOutput> for GetVectorsOperator {
    type Error = GetVectorsOperatorError;

    async fn run(
        &self,
        input: &GetVectorsOperatorInput,
    ) -> Result<GetVectorsOperatorOutput, Self::Error> {
        let mut output_vectors = HashMap::new();

        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Some(reader),
            Err(e) => match *e {
                record_segment::RecordSegmentReaderCreationError::UninitializedSegment => None,
                record_segment::RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreationError(
                        *e,
                    ))
                }
                record_segment::RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreationError(
                        *e,
                    ))
                }
            },
        };

        let logs = input.log_records.clone();
        let mut remaining_search_user_ids: HashSet<String> =
            HashSet::from_iter(input.search_user_ids.iter().cloned());
        for (log_record, _) in logs.iter() {
            if remaining_search_user_ids.contains(&log_record.record.id) {
                match log_record.record.operation {
                    crate::types::Operation::Add => {
                        // If the user id is already present in the log set, skip it
                        // We use the first add log entry for a user id if a
                        // user has multiple log entries
                        if output_vectors.contains_key(&log_record.record.id) {
                            continue;
                        }
                        let vector = log_record.record.embedding.as_ref().expect("Invariant violation. The log record for an add does not have an embedding.");
                        output_vectors.insert(log_record.record.id.clone(), vector.clone());
                        remaining_search_user_ids.remove(&log_record.record.id);
                    }
                    crate::types::Operation::Update => {
                        // If the update mutates the vector, we need to update the output
                        match &log_record.record.embedding {
                            Some(vector) => {
                                // This will overwrite the vector if it already exists
                                // (perhaps via an add operation)
                                output_vectors.insert(log_record.record.id.clone(), vector.clone());
                                remaining_search_user_ids.remove(&log_record.record.id);
                            }
                            None => {
                                // Nothing to do with this as the vector was not updated
                            }
                        }
                    }
                    crate::types::Operation::Upsert => {
                        // The upsert operation does not allow embeddings to be None
                        // So the final value is always present in the log
                        let vector = log_record.record.embedding.as_ref().expect("Invariant violation. The log record for an upsert does not have an embedding.");
                        output_vectors.insert(log_record.record.id.clone(), vector.clone());
                    }
                    crate::types::Operation::Delete => {
                        // If the user id is present in the output, remove it
                        output_vectors.remove(&log_record.record.id);
                    }
                }
            }
        }

        // Search the record segment for the remaining user ids
        if !remaining_search_user_ids.is_empty() {
            if let Some(reader) = record_segment_reader {
                for user_id in remaining_search_user_ids.iter() {
                    let read_data = reader.get_data_and_offset_id_for_user_id(user_id).await;
                    match read_data {
                        Ok((record, _)) => {
                            output_vectors.insert(record.id.to_string(), record.embedding.to_vec());
                        }
                        Err(_) => {
                            // If the user id is not found in the record segment, we do not add it to the output
                        }
                    }
                }
            }
        }

        let mut ids = Vec::new();
        let mut vectors = Vec::new();
        for (id, vector) in output_vectors.drain() {
            ids.push(id);
            vectors.push(vector);
        }
        return Ok(GetVectorsOperatorOutput { ids, vectors });
    }
}
