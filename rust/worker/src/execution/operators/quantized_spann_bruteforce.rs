use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentReader};
use chroma_system::Operator;
use chroma_types::{operator::RecordMeasure, SignedRoaringBitmap};
use thiserror::Error;

#[derive(Debug)]
pub struct QuantizedSpannBruteforceInput {
    pub cluster_id: u32,
}

#[derive(Debug)]
pub struct QuantizedSpannBruteforceOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannBruteforceError {
    #[error("Error in quantized spann bruteforce: {0}")]
    BruteforceError(#[from] QuantizedSpannSegmentError),
}

impl ChromaError for QuantizedSpannBruteforceError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::BruteforceError(e) => e.code(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuantizedSpannBruteforceOperator {
    pub count: usize,
    pub filter: SignedRoaringBitmap,
    pub reader: QuantizedSpannSegmentReader,
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
        let mut records = self
            .reader
            .bruteforce(input.cluster_id, &self.rotated_query)
            .await?;

        // Apply metadata/document filter.
        records.retain(|record| match &self.filter {
            SignedRoaringBitmap::Include(rbm) => rbm.contains(record.offset_id),
            SignedRoaringBitmap::Exclude(rbm) => !rbm.contains(record.offset_id),
        });

        // Truncate to top-k (records are already sorted by increasing distance).
        records.truncate(self.count);

        Ok(QuantizedSpannBruteforceOutput { records })
    }
}
