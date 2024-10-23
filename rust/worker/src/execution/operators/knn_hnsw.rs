use chroma_error::ChromaError;
use chroma_types::SignedRoaringBitmap;
use thiserror::Error;
use tonic::async_trait;

use crate::execution::{operator::Operator, utils::Distance};

use super::{
    fetch_segment::{FetchSegmentError, FetchSegmentOutput},
    knn::KNNOperator,
};

#[derive(Debug)]
struct KNNHNSWInput {
    segments: FetchSegmentOutput,
    compact_oids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KNNHNSWOutput {
    pub segments: FetchSegmentOutput,
    pub distances: Vec<Distance>,
}

#[derive(Error, Debug)]
pub enum KNNHNSWError {
    #[error("Error processing fetch segment output: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error querying knn index: {0}")]
    KNNIndex(Box<dyn ChromaError>),
}

impl ChromaError for KNNHNSWError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KNNHNSWError::FetchSegment(e) => e.code(),
            KNNHNSWError::KNNIndex(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KNNHNSWInput, KNNHNSWOutput> for KNNOperator {
    type Error = KNNHNSWError;

    async fn run(&self, input: &KNNHNSWInput) -> Result<KNNHNSWOutput, KNNHNSWError> {
        let (allowed, disallowed) = match &input.compact_oids {
            SignedRoaringBitmap::Include(rbm) if rbm.is_empty() => {
                return Ok(KNNHNSWOutput {
                    segments: input.segments.clone(),
                    distances: Vec::new(),
                })
            }
            SignedRoaringBitmap::Include(rbm) => {
                (rbm.iter().map(|oid| oid as usize).collect(), Vec::new())
            }
            SignedRoaringBitmap::Exclude(rbm) => {
                (Vec::new(), rbm.iter().map(|oid| oid as usize).collect())
            }
        };
        match input.segments.knn_segment_reader().await?.query(
            &self.embedding,
            self.fetch as usize,
            &allowed,
            &disallowed,
        ) {
            Ok((oids, distances)) => Ok(KNNHNSWOutput {
                segments: input.segments.clone(),
                distances: oids
                    .into_iter()
                    .map(|oid| oid as u32)
                    .zip(distances)
                    .map(|(oid, measure)| Distance { oid, measure })
                    .collect(),
            }),
            Err(e) => Err(KNNHNSWError::KNNIndex(e)),
        }
    }
}
