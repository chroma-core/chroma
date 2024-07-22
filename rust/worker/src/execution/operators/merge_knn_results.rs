use crate::{
    blockstore::provider::BlockfileProvider,
    errors::ChromaError,
    execution::operator::Operator,
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::Segment,
};
use async_trait::async_trait;
use thiserror::Error;
use tracing::{Instrument, Span};

#[derive(Debug)]
pub struct MergeKnnResultsOperator {}

#[derive(Debug)]
pub struct MergeKnnResultsOperatorInput {
    hnsw_result_offset_ids: Vec<usize>,
    hnsw_result_distances: Vec<f32>,
    brute_force_result_user_ids: Vec<String>,
    brute_force_result_distances: Vec<f32>,
    brute_force_result_vectors: Option<Vec<Vec<f32>>>,
    include_vectors: bool,
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
        brute_force_result_vectors: Option<Vec<Vec<f32>>>,
        include_vectors: bool,
        k: usize,
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
    ) -> Self {
        Self {
            hnsw_result_offset_ids,
            hnsw_result_distances,
            brute_force_result_user_ids,
            brute_force_result_distances,
            brute_force_result_vectors,
            include_vectors,
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
    pub vectors: Option<Vec<Vec<f32>>>,
}

#[derive(Error, Debug)]
pub enum MergeKnnResultsOperatorError {}

impl ChromaError for MergeKnnResultsOperatorError {
    fn code(&self) -> crate::errors::ErrorCodes {
        return crate::errors::ErrorCodes::UNKNOWN;
    }
}

#[async_trait]
impl Operator<MergeKnnResultsOperatorInput, MergeKnnResultsOperatorOutput>
    for MergeKnnResultsOperator
{
    type Error = Box<dyn ChromaError>;

    fn get_name(&self) -> &'static str {
        "MergeKnnResultsOperator"
    }

    async fn run(
        &self,
        input: &MergeKnnResultsOperatorInput,
    ) -> Result<MergeKnnResultsOperatorOutput, Self::Error> {
        let (result_user_ids, result_distances, result_vectors) =
            match RecordSegmentReader::from_segment(
                &input.record_segment_definition,
                &input.blockfile_provider,
            )
            .await
            {
                Ok(reader) => {
                    println!("Record Segment Reader created successfully");
                    // Convert the HNSW result offset IDs to user IDs
                    let mut hnsw_result_user_ids = Vec::new();
                    let mut hnsw_result_vectors = None;
                    if input.include_vectors {
                        hnsw_result_vectors = Some(Vec::new());
                    }
                    for offset_id in &input.hnsw_result_offset_ids {
                        let user_id = reader.get_user_id_for_offset_id(*offset_id as u32).await;
                        match user_id {
                            Ok(user_id) => hnsw_result_user_ids.push(user_id),
                            Err(e) => return Err(e),
                        }
                        if let Some(hnsw_result_vectors) = &mut hnsw_result_vectors {
                            let record = reader.get_data_for_offset_id(*offset_id as u32).await;
                            match record {
                                Ok(record) => hnsw_result_vectors.push(record.embedding.to_vec()),
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    merge_results(
                        &hnsw_result_user_ids,
                        &input.hnsw_result_distances,
                        &hnsw_result_vectors,
                        &input.brute_force_result_user_ids,
                        &input.brute_force_result_distances,
                        &input.brute_force_result_vectors,
                        input.include_vectors,
                        input.k,
                    )
                }
                Err(e) => match *e {
                    RecordSegmentReaderCreationError::BlockfileOpenError(e) => {
                        return Err(e);
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        return Err(e);
                    }
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        // The record segment doesn't exist - which implies no HNSW results
                        let hnsw_result_user_ids = Vec::new();
                        let hnsw_result_distances = Vec::new();
                        let hnsw_result_vectors = None;
                        merge_results(
                            &hnsw_result_user_ids,
                            &hnsw_result_distances,
                            &hnsw_result_vectors,
                            &input.brute_force_result_user_ids,
                            &input.brute_force_result_distances,
                            &input.brute_force_result_vectors,
                            input.include_vectors,
                            input.k,
                        )
                    }
                },
            };

        Ok(MergeKnnResultsOperatorOutput {
            user_ids: result_user_ids,
            distances: result_distances,
            vectors: result_vectors,
        })
    }
}

fn merge_results(
    hnsw_result_user_ids: &Vec<&str>,
    hnsw_result_distances: &Vec<f32>,
    hnsw_result_vectors: &Option<Vec<Vec<f32>>>,
    brute_force_result_user_ids: &Vec<String>,
    brute_force_result_distances: &Vec<f32>,
    brute_force_result_vectors: &Option<Vec<Vec<f32>>>,
    include_vectors: bool,
    k: usize,
) -> (Vec<String>, Vec<f32>, Option<Vec<Vec<f32>>>) {
    let mut result_user_ids = Vec::with_capacity(k);
    let mut result_distances = Vec::with_capacity(k);
    let mut result_vectors = None;
    if include_vectors {
        result_vectors = Some(Vec::with_capacity(k));
    }

    // Merge the HNSW and brute force results together by the minimum distance top k
    let mut hnsw_index = 0;
    let mut brute_force_index = 0;

    // TODO: This doesn't have to clone the user IDs, but it's easier for now
    while (result_user_ids.len() <= k)
        && (hnsw_index < hnsw_result_user_ids.len()
            || brute_force_index < brute_force_result_user_ids.len())
    {
        if hnsw_index < hnsw_result_user_ids.len()
            && brute_force_index < brute_force_result_user_ids.len()
        {
            if hnsw_result_distances[hnsw_index] < brute_force_result_distances[brute_force_index] {
                result_user_ids.push(hnsw_result_user_ids[hnsw_index].to_string());
                result_distances.push(hnsw_result_distances[hnsw_index]);
                if include_vectors {
                    result_vectors
                        .as_mut()
                        .expect("Include vectors is true, result_vectors should be Some")
                        .push(
                            hnsw_result_vectors.as_ref().expect(
                                "Include vectors is true, hnsw_result_vectors should be Some",
                            )[hnsw_index]
                                .to_vec(),
                        );
                }
                hnsw_index += 1;
            } else {
                result_user_ids.push(brute_force_result_user_ids[brute_force_index].to_string());
                result_distances.push(brute_force_result_distances[brute_force_index]);
                if include_vectors {
                    result_vectors
                        .as_mut()
                        .expect("Include vectors is true, result_vectors should be Some")
                        .push(
                            brute_force_result_vectors.as_ref().expect(
                                "Include vectors is true, brute_force_result_vectors should be Some",
                            )[brute_force_index]
                                .to_vec(),
                        );
                }
                brute_force_index += 1;
            }
        } else if hnsw_index < hnsw_result_user_ids.len() {
            result_user_ids.push(hnsw_result_user_ids[hnsw_index].to_string());
            result_distances.push(hnsw_result_distances[hnsw_index]);
            if include_vectors {
                result_vectors
                    .as_mut()
                    .expect("Include vectors is true, result_vectors should be Some")
                    .push(
                        hnsw_result_vectors
                            .as_ref()
                            .expect("Include vectors is true, hnsw_result_vectors should be Some")
                            [hnsw_index]
                            .to_vec(),
                    );
            }
            hnsw_index += 1;
        } else if brute_force_index < brute_force_result_user_ids.len() {
            result_user_ids.push(brute_force_result_user_ids[brute_force_index].to_string());
            result_distances.push(brute_force_result_distances[brute_force_index]);
            if include_vectors {
                result_vectors
                    .as_mut()
                    .expect("Include vectors is true, result_vectors should be Some")
                    .push(
                        brute_force_result_vectors.as_ref().expect(
                            "Include vectors is true, brute_force_result_vectors should be Some",
                        )[brute_force_index]
                            .to_vec(),
                    );
            }
            brute_force_index += 1;
        }
    }

    (result_user_ids, result_distances, result_vectors)
}
