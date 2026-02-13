use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::quantized_spann::QuantizedSpannSegmentReader;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannLoadClusterInput {
    pub cluster_id: u32,
}

#[derive(Debug)]
pub struct QuantizedSpannLoadClusterOutput {
    pub cluster_id: u32,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannLoadClusterError {
    #[error("Error loading quantized spann cluster")]
    LoadClusterError,
}

impl ChromaError for QuantizedSpannLoadClusterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::LoadClusterError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuantizedSpannLoadClusterOperator {
    pub reader: QuantizedSpannSegmentReader,
}

#[async_trait]
impl Operator<QuantizedSpannLoadClusterInput, QuantizedSpannLoadClusterOutput>
    for QuantizedSpannLoadClusterOperator
{
    type Error = QuantizedSpannLoadClusterError;

    async fn run(
        &self,
        input: &QuantizedSpannLoadClusterInput,
    ) -> Result<QuantizedSpannLoadClusterOutput, QuantizedSpannLoadClusterError> {
        self.reader.load_cluster(input.cluster_id).await;
        Ok(QuantizedSpannLoadClusterOutput {
            cluster_id: input.cluster_id,
        })
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
