use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError};
use chroma_system::Operator;
use chroma_types::{operator::RecordMeasure, Segment};
use thiserror::Error;

/// Input for the rerank operator: the merged candidate set (approximate distances, sorted
/// ascending) produced by the merge step after quantized bruteforce scoring.
#[derive(Debug)]
pub struct QuantizedSpannRerankInput {
    pub candidates: Vec<RecordMeasure>,
}

/// Output: the top-k results re-scored with exact distances, sorted ascending.
#[derive(Debug)]
pub struct QuantizedSpannRerankOutput {
    pub measures: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannRerankError {
    #[error("Failed to open record segment reader: {0}")]
    ReaderCreation(#[from] RecordSegmentReaderCreationError),
    #[error("Failed to fetch embedding for offset_id {0}: {1}")]
    EmbeddingFetch(u32, Box<dyn ChromaError>),
    #[error("Embedding missing for offset_id {0}")]
    EmbeddingMissing(u32),
}

impl ChromaError for QuantizedSpannRerankError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::ReaderCreation(e) => e.code(),
            Self::EmbeddingFetch(_, e) => e.code(),
            Self::EmbeddingMissing(_) => ErrorCodes::NotFound,
        }
    }
}

/// Re-scores candidates produced by the quantized bruteforce + merge pipeline with exact
/// (full-precision) distances, then keeps the top `k`.
///
/// This is Approach B: rerank after the cross-cluster merge, so we fetch embeddings only for
/// the merged top-`k * vector_rerank_factor` set rather than per cluster.
#[derive(Debug, Clone)]
pub struct QuantizedSpannRerankOperator {
    /// Final number of results to return.
    pub k: usize,
    /// Original (unrotated) query vector.
    pub embedding: Vec<f32>,
    pub distance_function: DistanceFunction,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
}

#[async_trait]
impl Operator<QuantizedSpannRerankInput, QuantizedSpannRerankOutput>
    for QuantizedSpannRerankOperator
{
    type Error = QuantizedSpannRerankError;

    async fn run(
        &self,
        input: &QuantizedSpannRerankInput,
    ) -> Result<QuantizedSpannRerankOutput, QuantizedSpannRerankError> {
        let reader =
            Box::pin(RecordSegmentReader::from_segment(&self.record_segment, &self.blockfile_provider))
                .await
                .map_err(|e| QuantizedSpannRerankError::ReaderCreation(*e))?;

        // Normalize query once for cosine distance (matches how embeddings are stored).
        let query_norm;
        let query = if let DistanceFunction::Cosine = self.distance_function {
            query_norm = normalize(&self.embedding);
            &query_norm[..]
        } else {
            &self.embedding[..]
        };

        let mut scored: Vec<RecordMeasure> = Vec::with_capacity(input.candidates.len());

        for candidate in &input.candidates {
            let record = reader
                .get_data_for_offset_id(candidate.offset_id)
                .await
                .map_err(|e| QuantizedSpannRerankError::EmbeddingFetch(candidate.offset_id, e))?
                .ok_or(QuantizedSpannRerankError::EmbeddingMissing(candidate.offset_id))?;

            let exact_dist = self.distance_function.distance(query, record.embedding);
            scored.push(RecordMeasure {
                offset_id: candidate.offset_id,
                measure: exact_dist,
            });
        }

        scored.sort_unstable_by(|a, b| a.measure.total_cmp(&b.measure));
        scored.truncate(self.k);

        Ok(QuantizedSpannRerankOutput { measures: scored })
    }
}
