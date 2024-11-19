use crate::{
    execution::operator::Operator,
    segment::{
        materialize_logs,
        record_segment::{self, RecordSegmentReader},
        LogMaterializerError,
    },
};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, LogRecord, MaterializedLogOperation, Segment};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug)]
pub struct GetVectorsOperator {}

impl GetVectorsOperator {
    pub fn new() -> Box<Self> {
        Box::new(GetVectorsOperator {})
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
        GetVectorsOperatorInput {
            record_segment_definition,
            blockfile_provider,
            log_records,
            search_user_ids,
        }
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
    RecordSegmentReaderCreation(
        #[from] crate::segment::record_segment::RecordSegmentReaderCreationError,
    ),
    #[error(transparent)]
    RecordSegmentReader(#[from] Box<dyn ChromaError>),
    #[error("Error materializing logs {0}")]
    LogMaterialization(#[from] LogMaterializerError),
}

impl ChromaError for GetVectorsOperatorError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<GetVectorsOperatorInput, GetVectorsOperatorOutput> for GetVectorsOperator {
    type Error = GetVectorsOperatorError;

    fn get_name(&self) -> &'static str {
        "GetVectorsOperator"
    }

    async fn run(
        &self,
        input: &GetVectorsOperatorInput,
    ) -> Result<GetVectorsOperatorOutput, Self::Error> {
        let mut output_vectors = HashMap::new();

        // Materialize logs.
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
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreation(*e))
                }
                record_segment::RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreation(*e))
                }
                record_segment::RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreation(*e))
                }
                record_segment::RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                    return Err(GetVectorsOperatorError::RecordSegmentReaderCreation(*e))
                }
            },
        };
        // Step 1: Materialize the logs.
        let mat_records =
            match materialize_logs(&record_segment_reader, &input.log_records, None).await {
                Ok(records) => records,
                Err(e) => {
                    return Err(GetVectorsOperatorError::LogMaterialization(e));
                }
            };

        // Search the log records for the user ids
        let mut remaining_search_user_ids: HashSet<String> =
            HashSet::from_iter(input.search_user_ids.iter().cloned());
        for (log_record, _) in mat_records.iter() {
            // Log is the source of truth for these so don't consider these for
            // reading from the segment.
            let mut removed = false;
            if remaining_search_user_ids.contains(log_record.merged_user_id_ref()) {
                removed = true;
                remaining_search_user_ids.remove(log_record.merged_user_id_ref());
            }
            if removed && log_record.final_operation != MaterializedLogOperation::DeleteExisting {
                output_vectors.insert(
                    log_record.merged_user_id(),
                    log_record.merged_embeddings().to_vec(),
                );
            }
        }

        // Search the record segment for the remaining user ids
        if !remaining_search_user_ids.is_empty() {
            if let Some(reader) = record_segment_reader {
                for user_id in remaining_search_user_ids.iter() {
                    let read_data = reader.get_data_and_offset_id_for_user_id(user_id).await;
                    match read_data {
                        Ok(Some((record, _))) => {
                            output_vectors.insert(record.id.to_string(), record.embedding.to_vec());
                        }
                        Ok(None) => {}
                        Err(_) => {
                            // If the user id is not found in the record segment, we do not add it to the output
                        }
                    }
                }
            }
        }

        let mut ids = Vec::new();
        let mut vectors = Vec::new();
        for id in &input.search_user_ids {
            if output_vectors.contains_key(id) {
                ids.push(id.clone());
                vectors.push(output_vectors.remove(id).unwrap());
            }
        }
        return Ok(GetVectorsOperatorOutput { ids, vectors });
    }
}
