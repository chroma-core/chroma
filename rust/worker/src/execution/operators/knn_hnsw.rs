use chroma_error::ChromaError;
use chroma_types::SignedRoaringBitmap;
use thiserror::Error;
use tonic::async_trait;

use crate::execution::{operator::Operator, utils::RecordDistance};

use super::{
    fetch_segment::{FetchSegmentError, FetchSegmentOutput},
    knn::KnnOperator,
};

#[derive(Debug)]
struct KnnHnswInput {
    segments: FetchSegmentOutput,
    compact_offset_ids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KnnHnswOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnHnswError {
    #[error("Error processing fetch segment output: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error querying knn index: {0}")]
    KnnIndex(#[from] Box<dyn ChromaError>),
}

impl ChromaError for KnnHnswError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KnnHnswError::FetchSegment(e) => e.code(),
            KnnHnswError::KnnIndex(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnHnswInput, KnnHnswOutput> for KnnOperator {
    type Error = KnnHnswError;

    async fn run(&self, input: &KnnHnswInput) -> Result<KnnHnswOutput, KnnHnswError> {
        let (allowed, disallowed) = match &input.compact_offset_ids {
            SignedRoaringBitmap::Include(rbm) if rbm.is_empty() => {
                return Ok(KnnHnswOutput {
                    record_distances: Vec::new(),
                })
            }
            SignedRoaringBitmap::Include(rbm) => (
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
                Vec::new(),
            ),
            SignedRoaringBitmap::Exclude(rbm) => (
                Vec::new(),
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
            ),
        };

        let record_distances = match input.segments.knn_segment_reader().await? {
            Some(reader) => {
                let (offset_ids, distances) =
                    reader.query(&self.embedding, self.fetch as usize, &allowed, &disallowed)?;
                offset_ids
                    .into_iter()
                    .map(|offset_id| offset_id as u32)
                    .zip(distances)
                    .map(|(offset_id, measure)| RecordDistance { offset_id, measure })
                    .collect()
            }
            None => Vec::new(),
        };
        Ok(KnnHnswOutput { record_distances })
    }
}
