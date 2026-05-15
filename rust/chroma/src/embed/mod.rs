//! Embedding function abstractions for converting text to vector representations.
//!
//! This module provides the [`EmbeddingFunction`] trait that defines how to transform
//! text strings into embeddings. Implementations are available for various
//! embedding models, including dense embeddings (Ollama) and sparse embeddings (BM25).

use std::{
    error::Error,
    fmt::{Debug, Display},
    sync::Arc,
};

use chroma_types::{EmbeddingFunctionConfiguration, Space, SparseVector};
use thiserror::Error;

/// BM25 sparse embedding implementation.
pub mod bm25;
/// Text tokenization utilities for BM25.
pub mod bm25_tokenizer;
/// Chroma Cloud embedding function implementations.
pub mod chroma_cloud;
/// MurmurHash3 absolute value hasher for token hashing.
pub mod murmur3_abs_hasher;
#[cfg(feature = "ollama")]
pub mod ollama;

/// Error type shared by object-safe embedding functions.
#[derive(Debug, Error)]
pub enum EmbeddingError {
    /// The named embedding function is known to Chroma but is not supported by this client.
    #[error("embedding function '{name}' is not supported by this client")]
    UnsupportedEmbeddingFunction {
        /// Embedding function name from persisted configuration.
        name: String,
    },
    /// The collection or request did not provide enough text to embed.
    #[error("embedding input error: {0}")]
    InvalidInput(String),
    /// An embedding provider returned a different number of vectors than requested.
    #[error("embedding function returned {actual} embeddings for {expected} inputs")]
    LengthMismatch {
        /// Number of input texts sent to the embedding function.
        expected: usize,
        /// Number of embeddings returned by the embedding function.
        actual: usize,
    },
    /// The embedding function configuration could not be interpreted.
    #[error("embedding configuration error: {0}")]
    Configuration(String),
    /// HTTP request to an embedding provider failed before a response was received.
    #[error("embedding provider request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// HTTP provider returned an error response or malformed provider payload.
    #[error("embedding provider error{status}: {message}")]
    Provider {
        /// Optional HTTP status code.
        status: EmbeddingStatus,
        /// Provider error body or diagnostic message.
        message: String,
    },
    /// JSON serialization or deserialization failed for an embedding provider.
    #[error("embedding serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Optional status code for embedding provider failures.
#[derive(Debug, Clone, Copy)]
pub struct EmbeddingStatus(Option<reqwest::StatusCode>);

impl EmbeddingStatus {
    /// Construct an empty embedding provider status.
    pub fn none() -> Self {
        Self(None)
    }

    /// Construct an embedding provider status from an HTTP status code.
    pub fn some(status: reqwest::StatusCode) -> Self {
        Self(Some(status))
    }
}

impl Display for EmbeddingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(status) => write!(f, " ({status})"),
            None => Ok(()),
        }
    }
}

/// Object-safe dense embedding function interface.
#[async_trait::async_trait]
pub trait DenseEmbeddingFunction: Send + Sync + 'static {
    /// Stable embedding function name used in persisted configuration.
    fn name(&self) -> &str {
        "legacy"
    }

    /// Persistable embedding function configuration.
    fn configuration(&self) -> EmbeddingFunctionConfiguration {
        EmbeddingFunctionConfiguration::Legacy
    }

    /// Default vector space for embeddings produced by this function.
    fn default_space(&self) -> Space {
        Space::L2
    }

    /// Vector spaces supported by this embedding function.
    fn supported_spaces(&self) -> Vec<Space> {
        vec![self.default_space()]
    }

    /// Embed documents for storage.
    async fn embed_documents(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError>;

    /// Embed query text for search.
    async fn embed_query(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.embed_documents(input).await
    }
}

/// Object-safe sparse embedding function interface.
#[async_trait::async_trait]
pub trait SparseEmbeddingFunction: Send + Sync + 'static {
    /// Stable embedding function name used in persisted configuration.
    fn name(&self) -> &str {
        "legacy"
    }

    /// Persistable embedding function configuration.
    fn configuration(&self) -> EmbeddingFunctionConfiguration {
        EmbeddingFunctionConfiguration::Legacy
    }

    /// Embed documents for sparse-vector storage.
    async fn embed_documents(&self, input: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError>;

    /// Embed query text for sparse-vector search.
    async fn embed_query(&self, input: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError> {
        self.embed_documents(input).await
    }
}

/// Build a supported dense embedding function from persisted configuration.
pub fn dense_embedding_function_from_config(
    configuration: &EmbeddingFunctionConfiguration,
    chroma_cloud_api_key: Option<String>,
) -> Result<Arc<dyn DenseEmbeddingFunction>, EmbeddingError> {
    let EmbeddingFunctionConfiguration::Known(configuration) = configuration else {
        return Err(EmbeddingError::UnsupportedEmbeddingFunction {
            name: embedding_function_configuration_name(configuration).to_string(),
        });
    };

    match configuration.name.as_str() {
        #[cfg(feature = "ollama")]
        "ollama" => Ok(Arc::new(ollama::OllamaEmbeddingFunction::try_from_config(
            configuration,
        )?)),
        #[cfg(not(feature = "ollama"))]
        "ollama" => Err(EmbeddingError::UnsupportedEmbeddingFunction {
            name: configuration.name.clone(),
        }),
        "chroma-cloud-qwen" => Ok(Arc::new(
            chroma_cloud::ChromaCloudQwenEmbeddingFunction::try_from_config(
                configuration,
                chroma_cloud_api_key,
            )?,
        )),
        _ => Err(EmbeddingError::UnsupportedEmbeddingFunction {
            name: configuration.name.clone(),
        }),
    }
}

/// Build a supported sparse embedding function from persisted configuration.
pub fn sparse_embedding_function_from_config(
    configuration: &EmbeddingFunctionConfiguration,
    chroma_cloud_api_key: Option<String>,
) -> Result<Arc<dyn SparseEmbeddingFunction>, EmbeddingError> {
    let EmbeddingFunctionConfiguration::Known(configuration) = configuration else {
        return Err(EmbeddingError::UnsupportedEmbeddingFunction {
            name: embedding_function_configuration_name(configuration).to_string(),
        });
    };

    match configuration.name.as_str() {
        "chroma_bm25" | "chroma-bm25" => Ok(Arc::new(
            bm25::ChromaBm25EmbeddingFunction::try_from_config(configuration)?,
        )),
        "chroma-cloud-splade" => Ok(Arc::new(
            chroma_cloud::ChromaCloudSpladeEmbeddingFunction::try_from_config(
                configuration,
                chroma_cloud_api_key,
            )?,
        )),
        _ => Err(EmbeddingError::UnsupportedEmbeddingFunction {
            name: configuration.name.clone(),
        }),
    }
}

fn embedding_function_configuration_name(
    configuration: &EmbeddingFunctionConfiguration,
) -> &'static str {
    match configuration {
        EmbeddingFunctionConfiguration::Legacy => "legacy",
        EmbeddingFunctionConfiguration::Known(_) => "known",
        EmbeddingFunctionConfiguration::Unknown => "unknown",
    }
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
