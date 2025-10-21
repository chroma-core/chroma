use chroma_types::SparseVector;
use thiserror::Error;

use crate::embed::bm25_tokenizer::Bm25Tokenizer;
use crate::embed::murmur3_abs_hasher::Murmur3AbsHasher;
use crate::embed::{EmbeddingFunction, TokenHasher, Tokenizer};

/// Error type for BM25 sparse embedding.
///
/// This is an empty enum (uninhabited type), meaning it can never be constructed.
/// BM25 encoding with infallible tokenizers and hashers cannot fail.
#[derive(Debug, Error)]
pub enum BM25SparseEmbeddingError {}

/// BM25 sparse embedding function parameterized by tokenizer and hasher.
///
/// The BM25 formula used:
/// score = tf * (k + 1) / (tf + k * (1 - b + b * doc_len / avg_len))
///
/// Where:
/// - tf: term frequency (count of token in document)
/// - doc_len: document length in tokens (not characters)
/// - k, b, avg_len: BM25 parameters
///
/// Type parameters:
/// - T: Tokenizer implementation (e.g., Bm25Tokenizer)
/// - H: TokenHasher implementation (e.g., Murmur3AbsHasher)
pub struct BM25SparseEmbeddingFunction<T, H>
where
    T: Tokenizer,
    H: TokenHasher,
{
    pub tokenizer: T,
    pub hasher: H,
    pub k: f32,
    pub b: f32,
    pub avg_len: f32,
}

impl Default for BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher> {
    /// Create a default BM25 implementation using Bm25Tokenizer and Murmur3AbsHasher.
    ///
    /// Default parameters:
    /// - k: 1.2 (BM25 saturation parameter)
    /// - b: 0.75 (length normalization parameter)
    /// - avg_len: 256.0 (average document length in tokens)
    fn default() -> Self {
        Self {
            tokenizer: Bm25Tokenizer::default(),
            hasher: Murmur3AbsHasher::default(),
            k: 1.2,
            b: 0.75,
            avg_len: 256.0,
        }
    }
}

impl<T, H> BM25SparseEmbeddingFunction<T, H>
where
    T: Tokenizer,
    H: TokenHasher,
{
    /// Encode a single text string into a sparse vector.
    pub fn encode(&self, text: &str) -> Result<SparseVector, BM25SparseEmbeddingError> {
        // Step 1: Tokenize text
        let tokens = self.tokenizer.tokenize(text);

        // Step 2: Document length = token count (following fastembed standard)
        let doc_len = tokens.len() as f32;

        // Step 3: Hash tokens to IDs
        let mut token_ids = Vec::with_capacity(tokens.len());
        for token in tokens {
            let id = self.hasher.hash(&token);
            token_ids.push(id);
        }

        // Step 4: Sort token IDs to group identical IDs together
        token_ids.sort_unstable();

        // Step 5: Calculate BM25 scores for each unique token
        let sparse_pairs = token_ids.chunk_by(|a, b| a == b).map(|chunk| {
            let id = chunk[0];
            let tf = chunk.len() as f32;

            // BM25 formula
            let score = tf * (self.k + 1.0)
                / (tf + self.k * (1.0 - self.b + self.b * doc_len / self.avg_len));

            (id, score)
        });

        Ok(SparseVector::from_pairs(sparse_pairs))
    }
}

#[async_trait::async_trait]
impl<T, H> EmbeddingFunction for BM25SparseEmbeddingFunction<T, H>
where
    T: Tokenizer + Send + Sync + 'static,
    H: TokenHasher + Send + Sync + 'static,
{
    type Embedding = SparseVector;
    type Error = BM25SparseEmbeddingError;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Self::Embedding>, Self::Error> {
        batches.iter().map(|text| self.encode(text)).collect()
    }
}
