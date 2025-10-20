//! Embedding function abstractions for converting text to vector representations.
//!
//! This module provides the [`EmbeddingFunction`] trait that defines how to transform
//! text strings into dense vector embeddings. Implementations are available for various
//! embedding models, including Ollama when the `ollama` feature is enabled.

#[cfg(feature = "ollama")]
pub mod ollama;

/// Transforms text strings into dense vector embeddings.
///
/// Embedding functions are the bridge between human-readable text and the vector space
/// where similarity search operates. Implementations must be thread-safe and support
/// batch processing for efficiency.
///
/// # Examples
///
/// ```ignore
/// use chroma::embed::EmbeddingFunction;
///
/// async fn process_documents<E: EmbeddingFunction>(embedder: E, docs: Vec<&str>) {
///     let vectors = embedder.embed(&docs).await.unwrap();
///     assert_eq!(vectors.len(), docs.len());
/// }
/// ```
#[async_trait::async_trait]
pub trait EmbeddingFunction: Send + Sync + 'static {
    /// The error type returned when embedding fails.
    ///
    /// Must implement standard error traits to enable composition with other error types
    /// and display meaningful diagnostic information.
    type Error: std::error::Error + std::fmt::Display;

    /// Converts a batch of text strings into their vector representations.
    ///
    /// Processes all inputs in a single request to the underlying model, returning embeddings
    /// in the same order as the input strings. The dimensionality of returned vectors depends
    /// on the specific model implementation.
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
    /// let vectors = embedder.embed(&texts).await?;
    /// assert_eq!(vectors.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    async fn embed(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error>;
}
