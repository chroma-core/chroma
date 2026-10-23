use std::collections::HashMap;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReader};
use chroma_system::{Operator, OperatorType};
use chroma_types::QuantizedClusterOwned;
use futures::future::try_join_all;
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannLoadClusterInput {
    pub cluster_id: u32,
}

#[derive(Debug)]
pub struct QuantizedSpannLoadClusterOutput {
    pub cluster: QuantizedClusterOwned,
    pub global_versions: HashMap<u32, u32>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannLoadClusterError {
    #[error("Error loading quantized spann cluster: {0}")]
    LoadClusterError(#[from] QuantizedSpannSegmentError),
}

impl ChromaError for QuantizedSpannLoadClusterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::LoadClusterError(e) => e.code(),
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
        let cluster = self.reader.get_cluster(input.cluster_id).await?;

        let versions =
            try_join_all(cluster.ids.iter().map(|&id| self.reader.get_version(id))).await?;

        let global_versions = cluster
            .ids
            .iter()
            .copied()
            .zip(versions)
            .collect::<HashMap<_, _>>();

        Ok(QuantizedSpannLoadClusterOutput {
            cluster,
            global_versions,
        })
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
