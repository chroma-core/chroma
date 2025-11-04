use chroma_types::{
    EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration, SparseVector,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::embed::bm25_tokenizer::Bm25Tokenizer;
use crate::embed::murmur3_abs_hasher::Murmur3AbsHasher;
use crate::embed::{SparseEmbeddingFunction, TokenHasher, Tokenizer};

/// Error type for BM25 sparse embedding.
#[derive(Debug, Error)]
pub enum BM25SparseEmbeddingError {
    /// JSON serialization or deserialization error.
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

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
    /// Tokenizer for converting text into tokens.
    pub tokenizer: T,
    /// Hasher for converting tokens into u32 identifiers.
    pub hasher: H,
    /// BM25 saturation parameter (typically 1.2).
    pub k: f32,
    /// BM25 length normalization parameter (typically 0.75).
    pub b: f32,
    /// Average document length in tokens for normalization.
    pub avg_len: f32,
}

impl BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher> {
    /// Create BM25 with default Bm25Tokenizer and Murmur3AbsHasher.
    ///
    /// This is the standard configuration matching Python's fastembed BM25.
    ///
    /// Default parameters:
    /// - k: 1.2 (BM25 saturation parameter)
    /// - b: 0.75 (length normalization parameter)
    /// - avg_len: 256.0 (average document length in tokens)
    /// - tokenizer: English stemmer with 179 stopwords, 40 char token limit
    /// - hasher: Murmur3 with seed 0, abs() behavior
    pub fn default_murmur3_abs() -> Self {
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
    pub fn encode(&self, text: &str) -> SparseVector {
        let tokens = self.tokenizer.tokenize(text);

        let doc_len = tokens.len() as f32;

        let mut token_ids = Vec::with_capacity(tokens.len());
        for token in tokens {
            let id = self.hasher.hash(&token);
            token_ids.push(id);
        }

        token_ids.sort_unstable();

        let sparse_pairs = token_ids.chunk_by(|a, b| a == b).map(|chunk| {
            let id = chunk[0];
            let tf = chunk.len() as f32;

            // BM25 formula
            let score = tf * (self.k + 1.0)
                / (tf + self.k * (1.0 - self.b + self.b * doc_len / self.avg_len));

            (id, score)
        });

        SparseVector::from_pairs(sparse_pairs)
    }
}

/// Configuration for BM25 sparse embedding function.
#[derive(Serialize, Deserialize)]
pub struct BM25Config {
    k: f32,
    b: f32,
    avg_doc_length: f32,
    token_max_length: usize,
}

impl TryFrom<BM25Config> for EmbeddingFunctionConfiguration {
    type Error = BM25SparseEmbeddingError;

    fn try_from(value: BM25Config) -> Result<Self, Self::Error> {
        Ok(EmbeddingFunctionConfiguration::Known(
            EmbeddingFunctionNewConfiguration {
                name: BM25SparseEmbeddingFunction::<Bm25Tokenizer, Murmur3AbsHasher>::get_name()
                    .to_string(),
                config: serde_json::to_value(value)?,
            },
        ))
    }
}

#[async_trait::async_trait]
impl SparseEmbeddingFunction for BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher> {
    type Error = BM25SparseEmbeddingError;
    type Config = BM25Config;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<SparseVector>, Self::Error> {
        Ok(batches
            .iter()
            .map(|text| self.encode(text))
            .collect::<Vec<_>>())
    }

    fn get_name() -> &'static str {
        "chroma_bm25"
    }

    fn build_from_config(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let tokenizer = Bm25Tokenizer {
            token_max_length: config.token_max_length,
            ..Default::default()
        };

        Ok(Self {
            tokenizer,
            hasher: Murmur3AbsHasher::default(),
            k: config.k,
            b: config.b,
            avg_len: config.avg_doc_length,
        })
    }

    fn get_config(&self) -> Result<Self::Config, Self::Error> {
        Ok(BM25Config {
            k: self.k,
            b: self.b,
            avg_doc_length: self.avg_len,
            token_max_length: self.tokenizer.token_max_length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests comprehensive tokenization covering:
    /// - Possessive forms (Bolt's)
    /// - Special characters (~, parentheses)
    /// - Numbers (27.8, 44.72)
    /// - Mixed case and abbreviations (mph, km/h)
    /// - Hyphens in compound words
    /// - Maximum token variety (12 unique tokens after processing)
    #[test]
    fn test_bm25_comprehensive_tokenization() {
        let bm25 = BM25SparseEmbeddingFunction::default_murmur3_abs();
        let text = "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)";

        let result = bm25.encode(text);

        let expected_indices = vec![
            230246813, 395514983, 458027949, 488165615, 729632045, 734978415, 997512866,
            1114505193, 1381820790, 1501587190, 1649421877, 1837285388,
        ];
        let expected_value = 1.6391153;

        assert_eq!(result.indices.len(), 12);
        assert_eq!(result.indices, expected_indices);

        for &value in &result.values {
            assert!((value - expected_value).abs() < 1e-5);
        }
    }

    /// Tests tokenizer's handling of:
    /// - Stopword filtering ("The" is filtered out)
    /// - Multiple consecutive spaces
    /// - Hyphens in compound words (space-time)
    /// - Full uppercase words (WARPS)
    /// - Trailing punctuation (...)
    /// - Stemming (objects -> object)
    #[test]
    fn test_bm25_stopwords_and_punctuation() {
        let bm25 = BM25SparseEmbeddingFunction::default_murmur3_abs();
        let text = "The   space-time   continuum   WARPS   near   massive   objects...";

        let result = bm25.encode(text);

        let expected_indices = vec![
            90097469, 519064992, 737893654, 1110755108, 1950894484, 2031641008, 2058513491,
        ];
        let expected_value = 1.660867;

        assert_eq!(result.indices.len(), 7);
        assert_eq!(result.indices, expected_indices);

        for &value in &result.values {
            assert!((value - expected_value).abs() < 1e-5);
        }
    }
}
