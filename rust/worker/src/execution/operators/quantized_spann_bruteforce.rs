use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::utils::query_quantized_cluster;
use chroma_system::Operator;
use chroma_types::{
    operator::RecordMeasure, QuantizedCluster, QuantizedClusterOwned, SignedRoaringBitmap,
};
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannBruteforceInput {
    pub cluster: QuantizedClusterOwned,
    pub global_versions: HashMap<u32, u32>,
}

#[derive(Debug)]
pub struct QuantizedSpannBruteforceOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannBruteforceError {
    #[error("Error in quantized spann bruteforce")]
    BruteforceError,
}

impl ChromaError for QuantizedSpannBruteforceError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::BruteforceError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuantizedSpannBruteforceOperator {
    pub count: usize,
    /// Data quantization bits (1 or 4).
    pub data_bits: u8,
    pub distance_function: DistanceFunction,
    pub filter: SignedRoaringBitmap,
    pub rotated_query: Arc<[f32]>,
}

#[async_trait]
impl Operator<QuantizedSpannBruteforceInput, QuantizedSpannBruteforceOutput>
    for QuantizedSpannBruteforceOperator
{
    type Error = QuantizedSpannBruteforceError;

    async fn run(
        &self,
        input: &QuantizedSpannBruteforceInput,
    ) -> Result<QuantizedSpannBruteforceOutput, QuantizedSpannBruteforceError> {
        let cluster = QuantizedCluster::from(&input.cluster);

        let result = query_quantized_cluster(
            &cluster,
            &self.rotated_query,
            &self.distance_function,
            self.data_bits,
            |id, version| {
                if input.global_versions.get(&id) != Some(&version) {
                    return false;
                }
                match &self.filter {
                    SignedRoaringBitmap::Include(rbm) => rbm.contains(id),
                    SignedRoaringBitmap::Exclude(rbm) => !rbm.contains(id),
                }
            },
        );

        let mut records = result
            .keys
            .into_iter()
            .zip(result.distances)
            .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
            .collect::<Vec<_>>();

        records.truncate(self.count);

        Ok(QuantizedSpannBruteforceOutput { records })
    }
}
