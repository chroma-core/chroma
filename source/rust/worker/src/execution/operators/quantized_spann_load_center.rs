use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReader},
    spann_provider::SpannProvider,
};
use chroma_system::{Operator, OperatorType};
use chroma_types::{Collection, Segment};
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannLoadCenterOutput {
    pub reader: QuantizedSpannSegmentReader,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannLoadCenterError {
    #[error("Error loading quantized spann center: {0}")]
    LoadCenterError(#[from] QuantizedSpannSegmentError),
}

impl ChromaError for QuantizedSpannLoadCenterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::LoadCenterError(e) => e.code(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuantizedSpannLoadCenterOperator {
    pub collection: Collection,
    pub spann_provider: SpannProvider,
    pub vector_segment: Segment,
}

#[async_trait]
impl Operator<(), QuantizedSpannLoadCenterOutput> for QuantizedSpannLoadCenterOperator {
    type Error = QuantizedSpannLoadCenterError;

    async fn run(
        &self,
        _input: &(),
    ) -> Result<QuantizedSpannLoadCenterOutput, QuantizedSpannLoadCenterError> {
        let reader = self
            .spann_provider
            .read_quantized_usearch(&self.collection, &self.vector_segment)
            .await?;
        Ok(QuantizedSpannLoadCenterOutput { reader })
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
