//! Embedding function abstractions for converting text to vector representations.
//!
//! This module provides the [`EmbeddingFunction`] trait that defines how to transform
//! text strings into embeddings. Implementations are available for various
//! embedding models, including dense embeddings (Ollama) and sparse embeddings (BM25).

use std::{
    error::Error,
    fmt::{Debug, Display},
};

/// BM25 sparse embedding implementation.
pub mod bm25;
/// Text tokenization utilities for BM25.
pub mod bm25_tokenizer;
/// MurmurHash3 absolute value hasher for token hashing.
pub mod murmur3_abs_hasher;
#[cfg(feature = "ollama")]
pub mod ollama;

/// Transforms text strings into embeddings.
///
/// Embedding functions are the bridge between human-readable text and the vector space
/// where similarity search operates. This trait supports both dense embeddings (e.g., from
/// neural models) and sparse embeddings (e.g., BM25 token weights). Implementations must
/// be thread-safe and support batch processing for efficiency.
///
/// # Examples
///
/// ```ignore
/// use chroma::embed::EmbeddingFunction;
///
/// async fn process_documents<E: EmbeddingFunction>(embedder: E, docs: Vec<&str>) {
///     let embeddings = embedder.embed_strs(&docs).await.unwrap();
///     assert_eq!(embeddings.len(), docs.len());
/// }
/// ```
#[async_trait::async_trait]
pub trait EmbeddingFunction: Send + Sync + 'static {
    /// The embedding type produced by this function.
    ///
    /// Can be dense vectors (`Vec<f32>`) for neural embeddings or sparse representations
    /// for token-based models like BM25.
    type Embedding: Debug;

    /// The error type returned when embedding fails.
    ///
    /// Must implement standard error traits to enable composition with other error types
    /// and display meaningful diagnostic information.
    type Error: Error + Display;

    /// Converts a batch of text strings into their embedding representations.
    ///
    /// Processes all inputs in a single request to the underlying model, returning embeddings
    /// in the same order as the input strings. The type and dimensionality of returned embeddings
    /// depend on the specific model implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedding model is unreachable, the input exceeds model limits,
    /// or the model returns malformed data.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use chroma::embed::EmbeddingFunction;
    /// # async fn example<E: EmbeddingFunction>(embedder: E) -> Result<(), E::Error> {
    /// let texts = vec!["Hello world", "Embedding example"];
    /// let embeddings = embedder.embed_strs(&texts).await?;
    /// assert_eq!(embeddings.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Self::Embedding>, Self::Error>;
}

/// Generic tokenizer interface for text processing.
pub trait Tokenizer {
    /// Tokenize text into a vector of tokens.
    fn tokenize(&self, text: &str) -> Vec<String>;
}

/// Hashes tokens to u32 identifiers for sparse representations.
pub trait TokenHasher {
    /// Hash a token string to a u32 identifier.
    fn hash(&self, token: &str) -> u32;
}
