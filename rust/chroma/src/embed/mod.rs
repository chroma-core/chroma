//! Embedding function abstractions for converting text to vector representations.
//!
//! This module provides the [`EmbeddingFunction`] trait that defines how to transform
//! text strings into embeddings. Implementations are available for various
//! embedding models, including dense embeddings (Ollama) and sparse embeddings (BM25).

use std::error::Error;

use chroma_types::{EmbeddingFunctionConfiguration, SparseVector};

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
/// ```
/// use chroma::embed::DenseEmbeddingFunction;
///
/// async fn process_documents<E: DenseEmbeddingFunction>(embedder: E, docs: Vec<&str>) {
///     let embeddings = embedder.embed_strs(&docs).await.unwrap();
///     assert_eq!(embeddings.len(), docs.len());
/// }
/// ```
#[async_trait::async_trait]
pub trait DenseEmbeddingFunction: Send + Sync + 'static {
    /// The error type returned when embedding fails.
    ///
    /// Must implement standard error traits to enable composition with other error types
    /// and display meaningful diagnostic information.
    type Error: Error;

    /// The configuration type used to serialize/deserialize the embedding function.
    type Config: serde::Serialize
        + serde::de::DeserializeOwned
        + TryInto<EmbeddingFunctionConfiguration>;

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
    /// ```
    /// # use chroma::embed::DenseEmbeddingFunction;
    /// # async fn example<E: DenseEmbeddingFunction>(embedder: E) -> Result<(), E::Error> {
    /// let texts = vec!["Hello world", "Embedding example"];
    /// let embeddings = embedder.embed_strs(&texts).await?;
    /// assert_eq!(embeddings.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error>;

    /// Constructs an embedding function from a configuration object. Used to hydrate embedding functions when fetching collections.
    fn build_from_config(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Serializes the embedding function's configuration to a JSON object. Used to store embedding function configurations when persisting collections.
    fn get_config(&self) -> Result<Self::Config, Self::Error>;

    /// Returns the unique name of the embedding function implementation.
    fn get_name() -> &'static str
    where
        Self: Sized;
}

/// Transforms text strings into embeddings.
///
/// Embedding functions are the bridge between human-readable text and the vector space
/// where similarity search operates. This trait supports both dense embeddings (e.g., from
/// neural models) and sparse embeddings (e.g., BM25 token weights). Implementations must
/// be thread-safe and support batch processing for efficiency.
///
/// # Examples
///
/// ```
/// use chroma::embed::SparseEmbeddingFunction;
///
/// async fn process_documents<E: SparseEmbeddingFunction>(embedder: E, docs: Vec<&str>) {
///     let embeddings = embedder.embed_strs(&docs).await.unwrap();
///     assert_eq!(embeddings.len(), docs.len());
/// }
/// ```
#[async_trait::async_trait]
pub trait SparseEmbeddingFunction: Send + Sync + 'static {
    /// The error type returned when embedding fails.
    ///
    /// Must implement standard error traits to enable composition with other error types
    /// and display meaningful diagnostic information.
    type Error: Error;

    /// The configuration type used to serialize/deserialize the embedding function.
    type Config: serde::Serialize
        + serde::de::DeserializeOwned
        + TryInto<EmbeddingFunctionConfiguration>;

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
    /// ```
    /// # use chroma::embed::SparseEmbeddingFunction;
    /// # async fn example<E: SparseEmbeddingFunction>(embedder: E) -> Result<(), E::Error> {
    /// let texts = vec!["Hello world", "Embedding example"];
    /// let embeddings = embedder.embed_strs(&texts).await?;
    /// assert_eq!(embeddings.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<SparseVector>, Self::Error>;

    /// Constructs an embedding function from a configuration object. Used to hydrate embedding functions when fetching collections.
    fn build_from_config(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Serializes the embedding function's configuration to a JSON object. Used to store embedding function configurations when persisting collections.
    fn get_config(&self) -> Result<Self::Config, Self::Error>;

    /// Returns the unique name of the embedding function implementation.
    fn get_name() -> &'static str
    where
        Self: Sized;
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
