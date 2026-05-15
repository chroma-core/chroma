use chroma_types::SparseVector;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::embed::bm25_tokenizer::Bm25Tokenizer;
use crate::embed::murmur3_abs_hasher::Murmur3AbsHasher;
use crate::embed::{
    EmbeddingError, EmbeddingFunction, SparseEmbeddingFunction, TokenHasher, Tokenizer,
};

const CHROMA_BM25_NAME: &str = "chroma_bm25";

/// Default Chroma BM25 sparse embedding function.
pub type ChromaBm25EmbeddingFunction = BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher>;

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
    /// Whether to store tokens in the created sparse vectors.
    pub include_tokens: bool,
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
            include_tokens: true,
            tokenizer: Bm25Tokenizer::default(),
            hasher: Murmur3AbsHasher::default(),
            k: 1.2,
            b: 0.75,
            avg_len: 256.0,
        }
    }

    /// Construct BM25 from a persisted Chroma BM25 configuration.
    pub fn try_from_config(
        configuration: &chroma_types::EmbeddingFunctionNewConfiguration,
    ) -> Result<Self, EmbeddingError> {
        let config: ChromaBm25Config = serde_json::from_value(configuration.config.clone())?;
        Ok(Self::from_config(config))
    }

    fn from_config(config: ChromaBm25Config) -> Self {
        let token_max_length = config.token_max_length.unwrap_or(40) as usize;
        let tokenizer = match config.stopwords {
            Some(stopwords) => Bm25Tokenizer::with_owned_stopwords(stopwords, token_max_length),
            None => Bm25Tokenizer {
                token_max_length,
                ..Default::default()
            },
        };

        Self {
            include_tokens: config.include_tokens.unwrap_or(true),
            tokenizer,
            hasher: Murmur3AbsHasher::default(),
            k: config.k.unwrap_or(1.2),
            b: config.b.unwrap_or(0.75),
            avg_len: config.avg_doc_length.unwrap_or(256.0),
        }
    }
}

impl From<&BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher>> for ChromaBm25Config {
    fn from(value: &BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher>) -> Self {
        let default_stopwords = Bm25Tokenizer::default_stopwords();
        let has_default_stopwords = value.tokenizer.stopwords.len() == default_stopwords.len()
            && value
                .tokenizer
                .stopwords
                .iter()
                .all(|word| default_stopwords.contains(word.as_ref()));
        let stopwords = if has_default_stopwords {
            None
        } else {
            let mut stopwords = value
                .tokenizer
                .stopwords
                .iter()
                .map(|word| word.as_ref().to_string())
                .collect::<Vec<_>>();
            stopwords.sort();
            Some(stopwords)
        };

        ChromaBm25Config {
            k: Some(value.k),
            b: Some(value.b),
            avg_doc_length: Some(value.avg_len),
            token_max_length: Some(value.tokenizer.token_max_length as u64),
            stopwords,
            include_tokens: Some(value.include_tokens),
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
        let tokens = self.tokenizer.tokenize(text);

        let doc_len = tokens.len() as f32;

        if self.include_tokens {
            let mut token_ids = Vec::with_capacity(tokens.len());
            for token in tokens {
                let id = self.hasher.hash(&token);
                token_ids.push((id, token));
            }

            token_ids.sort_unstable();

            let sparse_triples = token_ids.chunk_by(|a, b| a.0 == b.0).map(|chunk| {
                let id = chunk[0].0;
                let tk = chunk[0].1.clone();
                let tf = chunk.len() as f32;

                // BM25 formula
                let score = tf * (self.k + 1.0)
                    / (tf + self.k * (1.0 - self.b + self.b * doc_len / self.avg_len));

                (tk, id, score)
            });

            Ok(SparseVector::from_triples(sparse_triples))
        } else {
            let mut token_ids = Vec::with_capacity(tokens.len());
            for token in tokens {
                let id = self.hasher.hash(&token);
                token_ids.push(id);
            }

            token_ids.sort_unstable();

            let sparse_pairs = token_ids.chunk_by(|a, b| *a == *b).map(|chunk| {
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

#[async_trait::async_trait]
impl SparseEmbeddingFunction for BM25SparseEmbeddingFunction<Bm25Tokenizer, Murmur3AbsHasher> {
    fn name(&self) -> &str {
        CHROMA_BM25_NAME
    }

    fn configuration(&self) -> chroma_types::EmbeddingFunctionConfiguration {
        (
            CHROMA_BM25_NAME,
            serde_json::json!(ChromaBm25Config::from(self)),
        )
            .into()
    }

    async fn embed_documents(&self, batches: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError> {
        batches
            .iter()
            .map(|text| {
                self.encode(text)
                    .map_err(|err| EmbeddingError::InvalidInput(err.to_string()))
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ChromaBm25Config {
    k: Option<f32>,
    b: Option<f32>,
    avg_doc_length: Option<f32>,
    token_max_length: Option<u64>,
    stopwords: Option<Vec<String>>,
    include_tokens: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::{EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration};

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

        let result = bm25.encode(text).unwrap();

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

        let result = bm25.encode(text).unwrap();

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

    #[test]
    fn test_bm25_config_round_trip() {
        let config = EmbeddingFunctionNewConfiguration {
            name: "chroma-bm25".to_string(),
            config: serde_json::json!({
                "k": 1.5,
                "b": 0.5,
                "avg_doc_length": 128.0,
                "token_max_length": 12,
                "stopwords": ["and", "the"],
                "include_tokens": false
            }),
        };
        let bm25 = BM25SparseEmbeddingFunction::try_from_config(&config).unwrap();

        assert_eq!(
            bm25.configuration(),
            EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                name: "chroma_bm25".to_string(),
                config: serde_json::json!({
                    "k": 1.5,
                    "b": 0.5,
                    "avg_doc_length": 128.0,
                    "token_max_length": 12,
                    "stopwords": ["and", "the"],
                    "include_tokens": false
                }),
            })
        );
    }
}
