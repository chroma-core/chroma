use crate::{
    errors::ChromaError, execution::operator::Operator,
    segment::distributed_hnsw_segment::DistributedHNSWSegmentReader,
};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug)]
pub struct HnswKnnOperator {}

#[derive(Debug)]
pub struct HnswKnnOperatorInput {
    pub segment: Box<DistributedHNSWSegmentReader>,
    pub query: Vec<f32>,
    pub k: usize,
    pub allowed_ids: Arc<[String]>,
    pub disallowed_ids: Arc<[String]>,
}

#[derive(Debug)]
pub struct HnswKnnOperatorOutput {
    pub offset_ids: Vec<usize>,
    pub distances: Vec<f32>,
}

pub type HnswKnnOperatorResult = Result<HnswKnnOperatorOutput, Box<dyn ChromaError>>;

#[async_trait]
impl Operator<HnswKnnOperatorInput, HnswKnnOperatorOutput> for HnswKnnOperator {
    type Error = Box<dyn ChromaError>;

    async fn run(&self, input: &HnswKnnOperatorInput) -> HnswKnnOperatorResult {
        // TODO: pass in the updated + deleted ids from log and the result from the metadata filtering
        let (offset_ids, distances) = input.segment.query(&input.query, input.k);
        Ok(HnswKnnOperatorOutput {
            offset_ids,
            distances,
        })
    }
}
