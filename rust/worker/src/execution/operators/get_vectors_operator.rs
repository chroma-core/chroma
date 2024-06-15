use crate::{
    blockstore::provider::BlockfileProvider,
    execution::{data::data_chunk::Chunk, operator::Operator},
    types::{LogRecord, Segment},
};
use async_trait::async_trait;
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
pub enum GetVectorsOperatorError {}

#[async_trait]
impl Operator<GetVectorsOperatorInput, GetVectorsOperatorOutput> for GetVectorsOperator {
    type Error = GetVectorsOperatorError;

    async fn run(
        &self,
        input: &GetVectorsOperatorInput,
    ) -> Result<GetVectorsOperatorOutput, Self::Error> {
        unimplemented!()

        // let logs = output.logs();
        //         let mut log_user_ids_to_vectors = HashMap::new();
        //         for (log, index) in logs.iter() {
        //             match log.record.operation {
        //                 crate::types::Operation::Add => {
        //                     let user_id = log.record.id.clone();
        //                     // If the user id is already present in the log set, skip it
        //                     // We use the first log entry for a user id if a
        //                     // user has multiple log entries
        //                     if log_user_ids_to_vectors.contains_key(&user_id) {
        //                         continue;
        //                     }
        //                     // Otherwise, Add the vector to the output
        //                     let vector = log.record.embedding.expect("Invariant violation. The log record for an add does not have an embedding.");
        //                     log_user_ids_to_vectors.insert(user_id, vector);
        //                 }
        //                 crate::types::Operation::Update => {
        //                     // If the update touches the vector, we need to update the output
        //                     if log_user_ids_to_vectors.contains_key(&log.record.id) {
        //                         match log.record.embedding {
        //                             Some(vector) => {
        //                                 log_user_ids_to_vectors
        //                                     .insert(log.record.id.clone(), vector);
        //                             }
        //                             None => {
        //                                 // Nothing to do with this as the vector was not updated
        //                             }
        //                         }
        //                     }
        //                 }
        //                 crate::types::Operation::Upsert => todo!(),
        //                 crate::types::Operation::Delete => todo!(),
        //             }
        //         }
    }
}
