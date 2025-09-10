use async_trait::async_trait;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    operator::{Knn, KnnOutput, RecordMeasure},
    SignedRoaringBitmap,
};
use thiserror::Error;

use chroma_segment::distributed_hnsw::DistributedHNSWSegmentReader;
use chroma_system::Operator;

#[derive(Debug)]
pub struct KnnHnswInput {
    pub(crate) hnsw_reader: Box<DistributedHNSWSegmentReader>,
    pub compact_offset_ids: SignedRoaringBitmap,
    pub distance_function: DistanceFunction,
}

#[derive(Error, Debug)]
pub enum KnnHnswError {
    #[error("Error querying hnsw index: {0}")]
    HnswIndex(#[from] Box<dyn ChromaError>),
}

impl ChromaError for KnnHnswError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnHnswError::HnswIndex(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnHnswInput, KnnOutput> for Knn {
    type Error = KnnHnswError;

    async fn run(&self, input: &KnnHnswInput) -> Result<KnnOutput, KnnHnswError> {
        let (allowed, disallowed) = match &input.compact_offset_ids {
            SignedRoaringBitmap::Include(rbm) if rbm.is_empty() => return Ok(Default::default()),
            SignedRoaringBitmap::Include(rbm) => (
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
                Vec::new(),
            ),
            SignedRoaringBitmap::Exclude(rbm) => (
                Vec::new(),
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
            ),
        };

        let embedding_vector;
        let embedding = if let DistanceFunction::Cosine = input.distance_function {
            embedding_vector = normalize(&self.embedding);
            &embedding_vector
        } else {
            &self.embedding
        };

        let (offset_ids, distances) =
            input
                .hnsw_reader
                .query(embedding, self.fetch as usize, &allowed, &disallowed)?;
        Ok(KnnOutput {
            distances: offset_ids
                .into_iter()
                .map(|offset_id| offset_id as u32)
                .zip(distances)
                .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
                .collect(),
        })
    }
}
