use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReader};
use chroma_system::Operator;
use chroma_types::operator::Knn;
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannCenterSearchInput {
    pub count: usize,
    pub centroid_rerank_factor: usize,
    pub reader: QuantizedSpannSegmentReader,
}

#[derive(Debug)]
pub struct QuantizedSpannCenterSearchOutput {
    pub cluster_ids: Vec<u32>,
    pub rotated_query: Arc<[f32]>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannCenterSearchError {
    #[error("Error searching quantized spann centers: {0}")]
    CenterSearchError(#[from] QuantizedSpannSegmentError),
}

impl ChromaError for QuantizedSpannCenterSearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::CenterSearchError(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<QuantizedSpannCenterSearchInput, QuantizedSpannCenterSearchOutput> for Knn {
    type Error = QuantizedSpannCenterSearchError;

    async fn run(
        &self,
        input: &QuantizedSpannCenterSearchInput,
    ) -> Result<QuantizedSpannCenterSearchOutput, QuantizedSpannCenterSearchError> {
        let rotated_query = input.reader.rotate(&self.embedding)?;
        let cluster_ids = input.reader.navigate(
            &rotated_query,
            input.count,
            input.centroid_rerank_factor,
        )?;
        Ok(QuantizedSpannCenterSearchOutput {
            cluster_ids,
            rotated_query: rotated_query.into(),
        })
    }
}
