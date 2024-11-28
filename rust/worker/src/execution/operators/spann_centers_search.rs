use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::utils::rng_query;
use thiserror::Error;
use tonic::async_trait;

use crate::{
    execution::operator::Operator,
    segment::spann_segment::{SpannSegmentReader, SpannSegmentReaderContext},
};

#[derive(Debug)]
pub struct SpannCentersSearchInput {
    reader_context: SpannSegmentReaderContext,
    // Assumes that query is already normalized in case of cosine.
    query: Vec<f32>,
    k: usize,
    rng_epsilon: f32,
    rng_factor: f32,
    distance_function: DistanceFunction,
}

#[derive(Debug)]
pub struct SpannCentersSearchOutput {
    center_ids: Vec<usize>,
}

#[derive(Error, Debug)]
pub enum SpannCentersSearchError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderCreationError,
    #[error("Error querying RNG")]
    RngQueryError,
}

impl ChromaError for SpannCentersSearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderCreationError => ErrorCodes::Internal,
            Self::RngQueryError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug)]
pub struct SpannCentersSearchOperator {}

impl SpannCentersSearchOperator {
    pub fn new() -> Box<Self> {
        Box::new(SpannCentersSearchOperator {})
    }
}

#[async_trait]
impl Operator<SpannCentersSearchInput, SpannCentersSearchOutput> for SpannCentersSearchOperator {
    type Error = SpannCentersSearchError;

    async fn run(
        &self,
        input: &SpannCentersSearchInput,
    ) -> Result<SpannCentersSearchOutput, SpannCentersSearchError> {
        let spann_reader = SpannSegmentReader::from_segment(
            &input.reader_context.segment,
            &input.reader_context.blockfile_provider,
            &input.reader_context.hnsw_provider,
            input.reader_context.dimension,
        )
        .await
        .map_err(|_| SpannCentersSearchError::SpannSegmentReaderCreationError)?;
        // RNG Query.
        let res = rng_query(
            &input.query,
            spann_reader.index_reader.hnsw_index.clone(),
            input.k,
            input.rng_epsilon,
            input.rng_factor,
            input.distance_function.clone(),
        )
        .await
        .map_err(|_| SpannCentersSearchError::RngQueryError)?;
        Ok(SpannCentersSearchOutput { center_ids: res.0 })
    }
}
