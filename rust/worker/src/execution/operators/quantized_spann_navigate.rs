use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReader};
use chroma_system::Operator;
use chroma_types::operator::Knn;
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannNavigateInput {
    pub count: usize,
    pub reader: QuantizedSpannSegmentReader,
}

#[derive(Debug)]
pub struct QuantizedSpannNavigateOutput {
    pub cluster_ids: Vec<u32>,
    pub rotated_query: Arc<[f32]>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannNavigateError {
    #[error("Error navigating quantized spann segment: {0}")]
    NavigateError(#[from] QuantizedSpannSegmentError),
}

impl ChromaError for QuantizedSpannNavigateError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::NavigateError(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<QuantizedSpannNavigateInput, QuantizedSpannNavigateOutput> for Knn {
    type Error = QuantizedSpannNavigateError;

    async fn run(
        &self,
        input: &QuantizedSpannNavigateInput,
    ) -> Result<QuantizedSpannNavigateOutput, QuantizedSpannNavigateError> {
        let rotated_query = input.reader.rotate(&self.embedding);
        let cluster_ids = input.reader.navigate(&rotated_query, input.count)?;
        Ok(QuantizedSpannNavigateOutput {
            cluster_ids,
            rotated_query: rotated_query.into(),
        })
    }
}
