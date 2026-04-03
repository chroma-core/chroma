use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReaderShard},
    spann_provider::SpannProvider,
};
use chroma_system::{Operator, OperatorType};
use chroma_types::{Collection, Segment, SegmentShard};
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannLoadCenterOutput {
    pub reader: QuantizedSpannSegmentReaderShard,
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
        let vector_segment_shard = SegmentShard::from((&self.vector_segment, 0));
        let reader = self
            .spann_provider
            .read_quantized_usearch(&self.collection, &vector_segment_shard)
            .await?;
        Ok(QuantizedSpannLoadCenterOutput { reader })
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
