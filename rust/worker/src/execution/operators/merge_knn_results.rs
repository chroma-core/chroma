use crate::{
    blockstore::provider::BlockfileProvider, errors::ChromaError, execution::operator::Operator,
    segment::record_segment::RecordSegmentReader, types::Segment,
};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug)]
pub struct MergeKnnResultsOperator {}

#[derive(Debug)]
pub struct MergeKnnResultsOperatorInput {
    hnsw_result_offset_ids: Vec<usize>,
    hnsw_result_distances: Vec<f32>,
    brute_force_result_user_ids: Vec<String>,
    brute_force_result_distances: Vec<f32>,
    k: usize,
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
}

impl MergeKnnResultsOperatorInput {
    pub fn new(
        hnsw_result_offset_ids: Vec<usize>,
        hnsw_result_distances: Vec<f32>,
        brute_force_result_user_ids: Vec<String>,
        brute_force_result_distances: Vec<f32>,
        k: usize,
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
    ) -> Self {
        Self {
            hnsw_result_offset_ids,
            hnsw_result_distances,
            brute_force_result_user_ids,
            brute_force_result_distances,
            k,
            record_segment_definition,
            blockfile_provider: blockfile_provider,
        }
    }
}

#[derive(Debug)]
pub struct MergeKnnResultsOperatorOutput {
    pub user_ids: Vec<String>,
    pub distances: Vec<f32>,
}

#[derive(Error, Debug)]
pub enum MergeKnnResultsOperatorError {}

impl ChromaError for MergeKnnResultsOperatorError {
    fn code(&self) -> crate::errors::ErrorCodes {
        return crate::errors::ErrorCodes::UNKNOWN;
    }
}

pub type MergeKnnResultsOperatorResult =
    Result<MergeKnnResultsOperatorOutput, Box<dyn ChromaError>>;

#[async_trait]
impl Operator<MergeKnnResultsOperatorInput, MergeKnnResultsOperatorOutput>
    for MergeKnnResultsOperator
{
    type Error = Box<dyn ChromaError>;

    async fn run(&self, input: &MergeKnnResultsOperatorInput) -> MergeKnnResultsOperatorResult {
        // Convert the HNSW result offset IDs to user IDs
        let mut hnsw_result_user_ids = Vec::new();

        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                println!("Error creating Record Segment Reader: {:?}", e);
                return Err(e);
            }
        };

        for offset_id in &input.hnsw_result_offset_ids {
            let user_id = record_segment_reader
                .get_user_id_for_offset_id(*offset_id as u32)
                .await;
            match user_id {
                Ok(user_id) => {
                    hnsw_result_user_ids.push(user_id);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let mut result_user_ids = Vec::with_capacity(input.k);
        let mut result_distances = Vec::with_capacity(input.k);

        // Merge the HNSW and brute force results together by the minimum distance top k
        let mut hnsw_index = 0;
        let mut brute_force_index = 0;

        // TODO: This doesn't have to clone the user IDs, but it's easier for now
        while (result_user_ids.len() <= input.k)
            && (hnsw_index < input.hnsw_result_offset_ids.len()
                || brute_force_index < input.brute_force_result_user_ids.len())
        {
            if hnsw_index < input.hnsw_result_offset_ids.len()
                && brute_force_index < input.brute_force_result_user_ids.len()
            {
                if input.hnsw_result_distances[hnsw_index]
                    < input.brute_force_result_distances[brute_force_index]
                {
                    result_user_ids.push(hnsw_result_user_ids[hnsw_index].to_string());
                    result_distances.push(input.hnsw_result_distances[hnsw_index]);
                    hnsw_index += 1;
                } else {
                    result_user_ids
                        .push(input.brute_force_result_user_ids[brute_force_index].to_string());
                    result_distances.push(input.brute_force_result_distances[brute_force_index]);
                    brute_force_index += 1;
                }
            } else if hnsw_index < input.hnsw_result_offset_ids.len() {
                result_user_ids.push(hnsw_result_user_ids[hnsw_index].to_string());
                result_distances.push(input.hnsw_result_distances[hnsw_index]);
                hnsw_index += 1;
            } else {
                result_user_ids
                    .push(input.brute_force_result_user_ids[brute_force_index].to_string());
                result_distances.push(input.brute_force_result_distances[brute_force_index]);
                brute_force_index += 1;
            }
        }

        Ok(MergeKnnResultsOperatorOutput {
            user_ids: result_user_ids,
            distances: result_distances,
        })
    }
}
