//! Chroma Cloud embedding function implementations.
//!
//! This module mirrors the Python `chromadb` Chroma Cloud embedding functions:
//! `chroma-cloud-qwen` for dense embeddings and `chroma-cloud-splade` for sparse embeddings.

use std::{collections::HashMap, env};

use chroma_types::{
    EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration, SparseVector,
    SparseVectorLengthMismatch,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, InvalidHeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, Value};
use thiserror::Error;

use crate::embed::EmbeddingFunction;

const DEFAULT_CHROMA_EMBED_URL: &str = "https://embed.trychroma.com";
const DEFAULT_API_KEY_ENV_VAR: &str = "CHROMA_API_KEY";
const QWEN_NAME: &str = "chroma-cloud-qwen";
const SPLADE_NAME: &str = "chroma-cloud-splade";

/// Errors returned by Chroma Cloud embedding functions.
#[derive(Debug, Error)]
pub enum ChromaCloudEmbeddingError {
    /// No API key was supplied and the configured environment variable was unset.
    #[error("API key not found in environment variable {env_var}")]
    MissingApiKey {
        /// Environment variable checked for the API key.
        env_var: String,
    },
    /// An API key could not be converted to an HTTP header value.
    #[error("invalid API key header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    /// The configured model is not supported by this Rust client.
    #[error("unsupported Chroma Cloud embedding model: {0}")]
    UnsupportedModel(String),
    /// The HTTP request failed.
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Chroma Cloud returned an error payload or an unexpected payload shape.
    #[error("Chroma Cloud embedding API error: {0}")]
    Api(String),
    /// Chroma Cloud returned an invalid sparse vector.
    #[error("invalid sparse vector: {0}")]
    SparseVector(#[from] SparseVectorLengthMismatch),
    /// A Chroma Cloud embedding function configuration was invalid.
    #[error("invalid Chroma Cloud embedding function config: {0}")]
    InvalidConfig(String),
}

/// Dense Chroma Cloud Qwen embedding model.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChromaCloudQwenEmbeddingModel {
    /// `Qwen/Qwen3-Embedding-0.6B`.
    #[serde(rename = "Qwen/Qwen3-Embedding-0.6B")]
    #[default]
    Qwen3Embedding0p6b,
}

impl ChromaCloudQwenEmbeddingModel {
    /// Returns the model identifier sent to Chroma Cloud.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Qwen3Embedding0p6b => "Qwen/Qwen3-Embedding-0.6B",
        }
    }
}

/// Sparse Chroma Cloud Splade embedding model.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChromaCloudSpladeEmbeddingModel {
    /// `prithivida/Splade_PP_en_v1`.
    #[serde(rename = "prithivida/Splade_PP_en_v1")]
    #[default]
    SpladePpEnV1,
}

impl ChromaCloudSpladeEmbeddingModel {
    /// Returns the model identifier sent to Chroma Cloud.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SpladePpEnV1 => "prithivida/Splade_PP_en_v1",
        }
    }
}

/// Per-task Qwen instructions for document and query embeddings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChromaCloudQwenTaskInstructions {
    /// Instruction used when embedding documents.
    pub documents: String,
    /// Instruction used when embedding queries.
    pub query: String,
}

/// Chroma Cloud Qwen embedding function.
///
/// This implementation calls the Chroma Cloud embedding endpoint and produces dense
/// `Vec<f32>` embeddings.
pub struct ChromaCloudQwenEmbeddingFunction {
    client: reqwest::Client,
    api_url: String,
    model: ChromaCloudQwenEmbeddingModel,
    task: Option<String>,
    instructions: HashMap<String, ChromaCloudQwenTaskInstructions>,
    api_key_env_var: String,
}

/// Fluent builder for [`ChromaCloudQwenEmbeddingFunction`].
#[derive(Clone, Debug)]
pub struct ChromaCloudQwenEmbeddingFunctionBuilder {
    api_key: Option<String>,
    client_api_key: Option<String>,
    api_key_env_var: String,
    embed_url: Option<String>,
    model: ChromaCloudQwenEmbeddingModel,
    task: Option<String>,
    instructions: HashMap<String, ChromaCloudQwenTaskInstructions>,
}

impl Default for ChromaCloudQwenEmbeddingFunctionBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            client_api_key: None,
            api_key_env_var: DEFAULT_API_KEY_ENV_VAR.to_string(),
            embed_url: None,
            model: ChromaCloudQwenEmbeddingModel::default(),
            task: None,
            instructions: default_qwen_instructions(),
        }
    }
}

impl ChromaCloudQwenEmbeddingFunctionBuilder {
    /// Sets an explicit Chroma Cloud API key.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub(crate) fn client_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.client_api_key = Some(api_key.into());
        self
    }

    /// Sets the environment variable used to look up the Chroma Cloud API key.
    ///
    /// Defaults to `CHROMA_API_KEY`.
    pub fn api_key_env_var(mut self, api_key_env_var: impl Into<String>) -> Self {
        self.api_key_env_var = api_key_env_var.into();
        self
    }

    /// Sets the Chroma Cloud embedding endpoint.
    ///
    /// Defaults to `CHROMA_EMBED_URL` when set, otherwise `https://embed.trychroma.com`.
    pub fn embed_url(mut self, embed_url: impl Into<String>) -> Self {
        self.embed_url = Some(embed_url.into());
        self
    }

    /// Sets the Qwen model.
    pub fn model(mut self, model: ChromaCloudQwenEmbeddingModel) -> Self {
        self.model = model;
        self
    }

    /// Sets the task used to choose document/query instructions.
    pub fn task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Clears the configured task.
    pub fn without_task(mut self) -> Self {
        self.task = None;
        self
    }

    /// Replaces the full instruction map.
    pub fn instructions(
        mut self,
        instructions: HashMap<String, ChromaCloudQwenTaskInstructions>,
    ) -> Self {
        self.instructions = instructions;
        self
    }

    /// Adds or replaces instructions for one task.
    pub fn instruction(
        mut self,
        task: impl Into<String>,
        documents: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        insert_qwen_instruction(&mut self.instructions, task, documents, query);
        self
    }

    /// Builds the embedding function.
    ///
    /// The API key is resolved in this order: explicit `api_key`, `api_key_env_var`,
    /// then the client API key used by collection auto-configuration.
    pub fn build(self) -> Result<ChromaCloudQwenEmbeddingFunction, ChromaCloudEmbeddingError> {
        let api_key = resolve_api_key(self.api_key, &self.api_key_env_var, self.client_api_key)?;
        let client = new_chroma_cloud_client(api_key, self.model.as_str())?;
        Ok(ChromaCloudQwenEmbeddingFunction {
            client,
            api_url: trim_trailing_slash(self.embed_url.unwrap_or_else(chroma_embed_url_from_env)),
            model: self.model,
            task: self.task,
            instructions: self.instructions,
            api_key_env_var: self.api_key_env_var,
        })
    }
}

/// Fluent builder for the `chroma-cloud-qwen` known embedding function configuration.
#[derive(Clone, Debug)]
pub struct ChromaCloudQwenEmbeddingConfigurationBuilder {
    api_key_env_var: String,
    model: ChromaCloudQwenEmbeddingModel,
    task: Option<String>,
    instructions: HashMap<String, ChromaCloudQwenTaskInstructions>,
}

impl Default for ChromaCloudQwenEmbeddingConfigurationBuilder {
    fn default() -> Self {
        Self {
            api_key_env_var: DEFAULT_API_KEY_ENV_VAR.to_string(),
            model: ChromaCloudQwenEmbeddingModel::default(),
            task: None,
            instructions: default_qwen_instructions(),
        }
    }
}

impl ChromaCloudQwenEmbeddingConfigurationBuilder {
    /// Sets the API-key environment variable serialized into the configuration.
    pub fn api_key_env_var(mut self, api_key_env_var: impl Into<String>) -> Self {
        self.api_key_env_var = api_key_env_var.into();
        self
    }

    /// Sets the Qwen model serialized into the configuration.
    pub fn model(mut self, model: ChromaCloudQwenEmbeddingModel) -> Self {
        self.model = model;
        self
    }

    /// Sets the task serialized into the configuration.
    pub fn task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Clears the task serialized into the configuration.
    pub fn without_task(mut self) -> Self {
        self.task = None;
        self
    }

    /// Replaces the full instruction map serialized into the configuration.
    pub fn instructions(
        mut self,
        instructions: HashMap<String, ChromaCloudQwenTaskInstructions>,
    ) -> Self {
        self.instructions = instructions;
        self
    }

    /// Adds or replaces instructions for one task in the serialized configuration.
    pub fn instruction(
        mut self,
        task: impl Into<String>,
        documents: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        insert_qwen_instruction(&mut self.instructions, task, documents, query);
        self
    }

    /// Builds the known embedding function configuration.
    pub fn build(self) -> EmbeddingFunctionConfiguration {
        known_embedding_function_configuration(
            QWEN_NAME,
            qwen_config_value(
                &self.api_key_env_var,
                self.model,
                self.task.as_deref(),
                &self.instructions,
            ),
        )
    }
}

impl ChromaCloudQwenEmbeddingFunction {
    /// Returns the known embedding function name used in collection configuration.
    pub fn name() -> &'static str {
        QWEN_NAME
    }

    /// Returns a fluent builder for Qwen embedding functions.
    pub fn builder() -> ChromaCloudQwenEmbeddingFunctionBuilder {
        ChromaCloudQwenEmbeddingFunctionBuilder::default()
    }

    /// Returns a fluent builder for `chroma-cloud-qwen` collection configuration.
    pub fn configuration() -> ChromaCloudQwenEmbeddingConfigurationBuilder {
        ChromaCloudQwenEmbeddingConfigurationBuilder::default()
    }

    /// Constructs a Qwen embedding function from a known embedding function configuration.
    ///
    /// The API key is read from the configured `api_key_env_var` first. If that environment
    /// variable is unset, `client_api_key` is used.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is not `chroma-cloud-qwen`, the model is
    /// unsupported, or no API key is available.
    pub(crate) fn try_from_config(
        config: &EmbeddingFunctionNewConfiguration,
        client_api_key: Option<&str>,
    ) -> Result<Self, ChromaCloudEmbeddingError> {
        if config.name != QWEN_NAME {
            return Err(ChromaCloudEmbeddingError::InvalidConfig(format!(
                "expected {QWEN_NAME}, got {}",
                config.name
            )));
        }
        let config: QwenConfig = from_value(config.config.clone())
            .map_err(|err| ChromaCloudEmbeddingError::InvalidConfig(err.to_string()))?;
        let api_key_env_var = config
            .api_key_env_var
            .unwrap_or_else(|| DEFAULT_API_KEY_ENV_VAR.to_string());
        let mut builder = Self::builder()
            .api_key_env_var(api_key_env_var)
            .model(config.model)
            .instructions(
                config
                    .instructions
                    .unwrap_or_else(default_qwen_instructions),
            );
        if let Some(api_key) = client_api_key {
            builder = builder.client_api_key(api_key);
        }
        if let Some(task) = config.task {
            builder = builder.task(task);
        }
        builder.build()
    }

    /// Returns this embedding function's serializable configuration.
    pub fn get_config(&self) -> Value {
        qwen_config_value(
            &self.api_key_env_var,
            self.model,
            self.task.as_deref(),
            &self.instructions,
        )
    }

    async fn embed_with_instruction(
        &self,
        batches: &[&str],
        instruction: &str,
    ) -> Result<Vec<Vec<f32>>, ChromaCloudEmbeddingError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let request = DenseEmbeddingRequest {
            instructions: instruction,
            texts: batches,
        };
        let response = self
            .client
            .post(&self.api_url)
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json::<DenseEmbeddingResponse>()
            .await?;
        response.into_embeddings()
    }

    fn document_instruction(&self) -> &str {
        self.task
            .as_ref()
            .and_then(|task| self.instructions.get(task))
            .map(|instructions| instructions.documents.as_str())
            .unwrap_or("")
    }

    fn query_instruction(&self) -> &str {
        self.task
            .as_ref()
            .and_then(|task| self.instructions.get(task))
            .map(|instructions| instructions.query.as_str())
            .unwrap_or("")
    }
}

#[async_trait::async_trait]
impl EmbeddingFunction for ChromaCloudQwenEmbeddingFunction {
    type Embedding = Vec<f32>;
    type Error = ChromaCloudEmbeddingError;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error> {
        self.embed_with_instruction(batches, self.document_instruction())
            .await
    }

    async fn embed_query_strs(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error> {
        self.embed_with_instruction(batches, self.query_instruction())
            .await
    }
}

/// Chroma Cloud Splade sparse embedding function.
///
/// This implementation calls the Chroma Cloud sparse embedding endpoint and produces
/// [`SparseVector`] embeddings.
pub struct ChromaCloudSpladeEmbeddingFunction {
    client: reqwest::Client,
    api_url: String,
    model: ChromaCloudSpladeEmbeddingModel,
    include_tokens: bool,
    api_key_env_var: String,
}

/// Fluent builder for [`ChromaCloudSpladeEmbeddingFunction`].
#[derive(Clone, Debug)]
pub struct ChromaCloudSpladeEmbeddingFunctionBuilder {
    api_key: Option<String>,
    api_key_env_var: String,
    embed_url: Option<String>,
    model: ChromaCloudSpladeEmbeddingModel,
    include_tokens: bool,
}

impl Default for ChromaCloudSpladeEmbeddingFunctionBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            api_key_env_var: DEFAULT_API_KEY_ENV_VAR.to_string(),
            embed_url: None,
            model: ChromaCloudSpladeEmbeddingModel::default(),
            include_tokens: false,
        }
    }
}

impl ChromaCloudSpladeEmbeddingFunctionBuilder {
    /// Sets an explicit Chroma Cloud API key.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Sets the environment variable used to look up the Chroma Cloud API key.
    ///
    /// Defaults to `CHROMA_API_KEY`.
    pub fn api_key_env_var(mut self, api_key_env_var: impl Into<String>) -> Self {
        self.api_key_env_var = api_key_env_var.into();
        self
    }

    /// Sets the Chroma Cloud embedding endpoint.
    ///
    /// Defaults to `CHROMA_EMBED_URL` when set, otherwise `https://embed.trychroma.com`.
    pub fn embed_url(mut self, embed_url: impl Into<String>) -> Self {
        self.embed_url = Some(embed_url.into());
        self
    }

    /// Sets the Splade model.
    pub fn model(mut self, model: ChromaCloudSpladeEmbeddingModel) -> Self {
        self.model = model;
        self
    }

    /// Sets whether sparse vectors include token labels.
    pub fn include_tokens(mut self, include_tokens: bool) -> Self {
        self.include_tokens = include_tokens;
        self
    }

    /// Builds the embedding function.
    ///
    /// The API key is resolved in this order: explicit `api_key`, then `api_key_env_var`.
    pub fn build(self) -> Result<ChromaCloudSpladeEmbeddingFunction, ChromaCloudEmbeddingError> {
        let api_key = resolve_api_key(self.api_key, &self.api_key_env_var, None)?;
        let client = new_chroma_cloud_client(api_key, self.model.as_str())?;
        Ok(ChromaCloudSpladeEmbeddingFunction {
            client,
            api_url: format!(
                "{}/embed_sparse",
                trim_trailing_slash(self.embed_url.unwrap_or_else(chroma_embed_url_from_env))
            ),
            model: self.model,
            include_tokens: self.include_tokens,
            api_key_env_var: self.api_key_env_var,
        })
    }
}

/// Fluent builder for the `chroma-cloud-splade` known embedding function configuration.
#[derive(Clone, Debug)]
pub struct ChromaCloudSpladeEmbeddingConfigurationBuilder {
    api_key_env_var: String,
    model: ChromaCloudSpladeEmbeddingModel,
    include_tokens: bool,
}

impl Default for ChromaCloudSpladeEmbeddingConfigurationBuilder {
    fn default() -> Self {
        Self {
            api_key_env_var: DEFAULT_API_KEY_ENV_VAR.to_string(),
            model: ChromaCloudSpladeEmbeddingModel::default(),
            include_tokens: false,
        }
    }
}

impl ChromaCloudSpladeEmbeddingConfigurationBuilder {
    /// Sets the API-key environment variable serialized into the configuration.
    pub fn api_key_env_var(mut self, api_key_env_var: impl Into<String>) -> Self {
        self.api_key_env_var = api_key_env_var.into();
        self
    }

    /// Sets the Splade model serialized into the configuration.
    pub fn model(mut self, model: ChromaCloudSpladeEmbeddingModel) -> Self {
        self.model = model;
        self
    }

    /// Sets whether token labels should be included in the serialized configuration.
    pub fn include_tokens(mut self, include_tokens: bool) -> Self {
        self.include_tokens = include_tokens;
        self
    }

    /// Builds the known embedding function configuration.
    pub fn build(self) -> EmbeddingFunctionConfiguration {
        known_embedding_function_configuration(
            SPLADE_NAME,
            splade_config_value(&self.api_key_env_var, self.model, self.include_tokens),
        )
    }
}

impl ChromaCloudSpladeEmbeddingFunction {
    /// Returns the known embedding function name used in collection configuration.
    pub fn name() -> &'static str {
        SPLADE_NAME
    }

    /// Returns a fluent builder for Splade embedding functions.
    pub fn builder() -> ChromaCloudSpladeEmbeddingFunctionBuilder {
        ChromaCloudSpladeEmbeddingFunctionBuilder::default()
    }

    /// Returns a fluent builder for `chroma-cloud-splade` collection configuration.
    pub fn configuration() -> ChromaCloudSpladeEmbeddingConfigurationBuilder {
        ChromaCloudSpladeEmbeddingConfigurationBuilder::default()
    }

    /// Returns this embedding function's serializable configuration.
    pub fn get_config(&self) -> Value {
        splade_config_value(&self.api_key_env_var, self.model, self.include_tokens)
    }
}

#[async_trait::async_trait]
impl EmbeddingFunction for ChromaCloudSpladeEmbeddingFunction {
    type Embedding = SparseVector;
    type Error = ChromaCloudEmbeddingError;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<SparseVector>, Self::Error> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let request = SparseEmbeddingRequest {
            texts: batches,
            task: "",
            target: "",
            fetch_tokens: if self.include_tokens { "true" } else { "false" },
        };
        let response = self
            .client
            .post(&self.api_url)
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json::<SparseEmbeddingResponse>()
            .await?;
        response
            .embeddings
            .into_iter()
            .map(|embedding| embedding.into_sparse_vector(self.include_tokens))
            .collect()
    }
}

#[derive(Deserialize)]
struct QwenConfig {
    model: ChromaCloudQwenEmbeddingModel,
    task: Option<String>,
    api_key_env_var: Option<String>,
    instructions: Option<HashMap<String, ChromaCloudQwenTaskInstructions>>,
}

#[derive(Serialize)]
struct DenseEmbeddingRequest<'a> {
    instructions: &'a str,
    texts: &'a [&'a str],
}

#[derive(Deserialize)]
struct DenseEmbeddingResponse {
    embeddings: Option<Vec<Vec<f32>>>,
    error: Option<String>,
}

impl DenseEmbeddingResponse {
    fn into_embeddings(self) -> Result<Vec<Vec<f32>>, ChromaCloudEmbeddingError> {
        self.embeddings.ok_or_else(|| {
            ChromaCloudEmbeddingError::Api(
                self.error
                    .unwrap_or_else(|| "missing embeddings".to_string()),
            )
        })
    }
}

#[derive(Serialize)]
struct SparseEmbeddingRequest<'a> {
    texts: &'a [&'a str],
    task: &'a str,
    target: &'a str,
    fetch_tokens: &'a str,
}

#[derive(Deserialize)]
struct SparseEmbeddingResponse {
    embeddings: Vec<SparseEmbedding>,
}

#[derive(Deserialize)]
struct SparseEmbedding {
    indices: Vec<u32>,
    values: Vec<f32>,
    #[serde(default, alias = "tokens")]
    labels: Option<Vec<String>>,
}

impl SparseEmbedding {
    fn into_sparse_vector(
        self,
        include_tokens: bool,
    ) -> Result<SparseVector, ChromaCloudEmbeddingError> {
        if self.indices.len() != self.values.len() {
            return Err(SparseVectorLengthMismatch.into());
        }
        if include_tokens {
            if let Some(labels) = self.labels {
                if labels.len() != self.indices.len() {
                    return Err(SparseVectorLengthMismatch.into());
                }
                let mut triples = self
                    .indices
                    .into_iter()
                    .zip(self.values)
                    .zip(labels)
                    .map(|((index, value), label)| (label, index, value))
                    .collect::<Vec<_>>();
                triples.sort_unstable_by_key(|(_, index, _)| *index);
                return Ok(SparseVector::from_triples(triples));
            }
        }
        let mut pairs = self
            .indices
            .into_iter()
            .zip(self.values)
            .collect::<Vec<_>>();
        pairs.sort_unstable_by_key(|(index, _)| *index);
        Ok(SparseVector::from_pairs(pairs))
    }
}

fn resolve_api_key(
    api_key: Option<String>,
    api_key_env_var: &str,
    client_api_key: Option<String>,
) -> Result<String, ChromaCloudEmbeddingError> {
    api_key
        .or_else(|| env::var(api_key_env_var).ok())
        .or(client_api_key)
        .ok_or_else(|| ChromaCloudEmbeddingError::MissingApiKey {
            env_var: api_key_env_var.to_string(),
        })
}

fn insert_qwen_instruction(
    instructions: &mut HashMap<String, ChromaCloudQwenTaskInstructions>,
    task: impl Into<String>,
    documents: impl Into<String>,
    query: impl Into<String>,
) {
    instructions.insert(
        task.into(),
        ChromaCloudQwenTaskInstructions {
            documents: documents.into(),
            query: query.into(),
        },
    );
}

fn known_embedding_function_configuration(
    name: &str,
    config: Value,
) -> EmbeddingFunctionConfiguration {
    EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
        name: name.to_string(),
        config,
    })
}

fn qwen_config_value(
    api_key_env_var: &str,
    model: ChromaCloudQwenEmbeddingModel,
    task: Option<&str>,
    instructions: &HashMap<String, ChromaCloudQwenTaskInstructions>,
) -> Value {
    json!({
        "api_key_env_var": api_key_env_var,
        "model": model.as_str(),
        "task": task,
        "instructions": instructions,
    })
}

fn splade_config_value(
    api_key_env_var: &str,
    model: ChromaCloudSpladeEmbeddingModel,
    include_tokens: bool,
) -> Value {
    json!({
        "api_key_env_var": api_key_env_var,
        "model": model.as_str(),
        "include_tokens": include_tokens,
    })
}

fn new_chroma_cloud_client(
    api_key: String,
    model: &str,
) -> Result<reqwest::Client, ChromaCloudEmbeddingError> {
    let mut headers = HeaderMap::new();
    let mut api_key = HeaderValue::from_str(&api_key)?;
    api_key.set_sensitive(true);
    headers.insert(HeaderName::from_static("x-chroma-token"), api_key);
    headers.insert(
        HeaderName::from_static("x-chroma-embedding-model"),
        HeaderValue::from_str(model)?,
    );
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .build()?)
}

fn chroma_embed_url_from_env() -> String {
    env::var("CHROMA_EMBED_URL").unwrap_or_else(|_| DEFAULT_CHROMA_EMBED_URL.to_string())
}

fn trim_trailing_slash(url: String) -> String {
    url.trim_end_matches('/').to_string()
}

fn default_qwen_instructions() -> HashMap<String, ChromaCloudQwenTaskInstructions> {
    let mut instructions = HashMap::new();
    instructions.insert(
        "nl_to_code".to_string(),
        ChromaCloudQwenTaskInstructions {
            documents: String::new(),
            query: "Given a question about coding, retrieval code or passage that can solve user's question".to_string(),
        },
    );
    instructions
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::MockServer;
    use serde_json::json;

    #[tokio::test]
    async fn qwen_embeds_documents_and_queries_with_expected_instructions() {
        let server = MockServer::start_async().await;
        let documents = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/")
                    .header("x-chroma-token", "test-api-key")
                    .header("x-chroma-embedding-model", "Qwen/Qwen3-Embedding-0.6B")
                    .json_body(json!({
                        "instructions": "",
                        "texts": ["doc"],
                    }));
                then.status(200)
                    .json_body(json!({"embeddings": [[1.0, 2.0]]}));
            })
            .await;
        let queries = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/")
                    .json_body(json!({
                        "instructions": "Given a question about coding, retrieval code or passage that can solve user's question",
                        "texts": ["query"],
                    }));
                then.status(200).json_body(json!({"embeddings": [[3.0, 4.0]]}));
            })
            .await;

        let embedding_function = ChromaCloudQwenEmbeddingFunction::builder()
            .api_key("test-api-key")
            .embed_url(server.base_url())
            .model(ChromaCloudQwenEmbeddingModel::default())
            .task("nl_to_code")
            .build()
            .unwrap();

        assert_eq!(
            embedding_function.embed_strs(&["doc"]).await.unwrap(),
            vec![vec![1.0, 2.0]]
        );
        assert_eq!(
            embedding_function
                .embed_query_strs(&["query"])
                .await
                .unwrap(),
            vec![vec![3.0, 4.0]]
        );
        assert_eq!(documents.calls(), 1);
        assert_eq!(queries.calls(), 1);
    }

    #[tokio::test]
    async fn splade_embeds_sparse_vectors() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/embed_sparse")
                    .header("x-chroma-token", "test-api-key")
                    .header("x-chroma-embedding-model", "prithivida/Splade_PP_en_v1")
                    .json_body(json!({
                        "texts": ["doc"],
                        "task": "",
                        "target": "",
                        "fetch_tokens": "true",
                    }));
                then.status(200).json_body(json!({
                    "embeddings": [{
                        "indices": [3, 1],
                        "values": [0.3, 0.1],
                        "labels": ["three", "one"],
                    }]
                }));
            })
            .await;

        let embedding_function = ChromaCloudSpladeEmbeddingFunction::builder()
            .api_key("test-api-key")
            .embed_url(server.base_url())
            .model(ChromaCloudSpladeEmbeddingModel::default())
            .include_tokens(true)
            .build()
            .unwrap();

        let embeddings = embedding_function.embed_strs(&["doc"]).await.unwrap();

        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].indices, vec![1, 3]);
        assert_eq!(embeddings[0].values, vec![0.1, 0.3]);
        assert_eq!(
            embeddings[0].tokens,
            Some(vec!["one".to_string(), "three".to_string()])
        );
        assert_eq!(mock.calls(), 1);
    }
}
