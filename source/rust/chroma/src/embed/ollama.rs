//! Ollama embedding function implementation for local model inference.
//!
//! This module provides [`OllamaEmbeddingFunction`], which connects to a locally running
//! Ollama instance to generate embeddings using models like `nomic-embed-text` or `mxbai-embed-large`.
//! Ollama enables privacy-preserving embeddings without sending data to external APIs.

use reqwest::RequestBuilder;

use super::EmbeddingFunction;

/////////////////////////////////////// OllamaEmbeddingError ///////////////////////////////////////

/// Errors that occur during Ollama embedding operations.
#[derive(Debug, thiserror::Error)]
pub enum OllamaEmbeddingError {
    /// Network request to the Ollama server failed.
    ///
    /// This includes connection errors, timeouts, and invalid responses from the Ollama API.
    #[error("request failed: {0}")]
    Reqwest(#[from] reqwest::Error),
}

////////////////////////////////////// OllamaEmbeddingFunction /////////////////////////////////////

/// Generates embeddings using a locally running Ollama instance.
///
/// Connects to an Ollama server (typically at `http://localhost:11434`) and uses the specified
/// model to transform text into vector embeddings. This enables privacy-preserving semantic search
/// without external API dependencies.
///
/// # Examples
///
/// ```ignore
/// use chroma::embed::ollama::OllamaEmbeddingFunction;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let embedder = OllamaEmbeddingFunction::new(
///     "http://localhost:11434",
///     "nomic-embed-text"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub struct OllamaEmbeddingFunction {
    client: reqwest::Client,
    host: String,
    model: String,
}

impl OllamaEmbeddingFunction {
    /// Constructs a new Ollama embedding function and verifies connectivity.
    ///
    /// Connects to the specified Ollama host and performs a heartbeat check to ensure
    /// the server is reachable and the model is available. The model must already be
    /// pulled locally using `ollama pull <model>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Ollama server is unreachable, the model is not found,
    /// or the heartbeat request fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chroma::embed::ollama::OllamaEmbeddingFunction;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let embedder = OllamaEmbeddingFunction::new(
    ///     "http://localhost:11434",
    ///     "nomic-embed-text"
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        host: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, OllamaEmbeddingError> {
        let client = reqwest::Client::new();
        let host = host.into();
        let model = model.into();
        let this = Self {
            client,
            host,
            model,
        };
        this.heartbeat().await?;
        Ok(this)
    }

    /// Verifies that the Ollama server is responsive and the model is accessible.
    ///
    /// Sends a minimal embedding request to confirm the connection is healthy. This is
    /// automatically called during construction but can be invoked manually to check
    /// server status after initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if the server is unreachable or the model is unavailable.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use chroma::embed::ollama::OllamaEmbeddingFunction;
    /// # async fn example(embedder: OllamaEmbeddingFunction) -> Result<(), Box<dyn std::error::Error>> {
    /// embedder.heartbeat().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn heartbeat(&self) -> Result<(), OllamaEmbeddingError> {
        self.embed_strs(&["heartbeat"]).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EmbeddingFunction for OllamaEmbeddingFunction {
    type Embedding = Vec<f32>;
    type Error = OllamaEmbeddingError;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error> {
        let model = &self.model;
        let input = batches;
        let req = EmbedRequest { model, input };
        let resp = req
            .make_request(self)
            .send()
            .await?
            .error_for_status()?
            .json::<EmbedResponse>()
            .await?;
        Ok(resp.embeddings)
    }
}

/////////////////////////////////////////// EmbedRequest ///////////////////////////////////////////

/// A request to embed multiple input documents.
#[derive(Clone, Debug, serde::Serialize)]
pub struct EmbedRequest<'a> {
    /// The name of the model to use for embedding.
    pub model: &'a str,
    /// The input texts to embed.
    pub input: &'a [&'a str],
}

impl EmbedRequest<'_> {
    /// Create a new RequestBuilder for this embed request.
    pub fn make_request(&self, ef: &OllamaEmbeddingFunction) -> RequestBuilder {
        ef.client.post(format!("{}/api/embed", ef.host)).json(self)
    }
}

/////////////////////////////////////////// EmbedResponse //////////////////////////////////////////

/// A response to an embed response.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct EmbedResponse {
    /// The name of the model used to generate the response.
    pub model: String,
    /// The embeddings of the input, in the same order.
    pub embeddings: Vec<Vec<f32>>,
    /// The duration of the response.
    pub total_duration: Option<f64>,
    /// The duration of loading the model.
    pub load_duration: Option<f64>,
    /// The number of tokens counted in the prompt.
    pub prompt_eval_count: Option<f64>,
}
