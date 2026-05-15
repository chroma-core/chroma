//! Chroma Cloud embedding function implementations.

use std::collections::BTreeMap;

use chroma_types::SparseVector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::embed::{
    DenseEmbeddingFunction, EmbeddingError, EmbeddingStatus, SparseEmbeddingFunction,
};

const DEFAULT_CHROMA_EMBED_URL: &str = "https://embed.trychroma.com";
const DEFAULT_API_KEY_ENV_VAR: &str = "CHROMA_API_KEY";
const QWEN_NAME: &str = "chroma-cloud-qwen";
const SPLADE_NAME: &str = "chroma-cloud-splade";
const DEFAULT_QWEN_MODEL: &str = "Qwen/Qwen3-Embedding-0.6B";
const DEFAULT_SPLADE_MODEL: &str = "prithivida/Splade_PP_en_v1";

fn default_api_key_env_var() -> String {
    DEFAULT_API_KEY_ENV_VAR.to_string()
}

fn default_chroma_embed_url() -> String {
    std::env::var("CHROMA_EMBED_URL")
        .unwrap_or_else(|_| DEFAULT_CHROMA_EMBED_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn default_qwen_instructions() -> BTreeMap<String, BTreeMap<String, String>> {
    let mut targets = BTreeMap::new();
    targets.insert("documents".to_string(), String::new());
    targets.insert(
        "query".to_string(),
        "Given a question about coding, retrieval code or passage that can solve user's question"
            .to_string(),
    );

    let mut instructions = BTreeMap::new();
    instructions.insert("nl_to_code".to_string(), targets);
    instructions
}

fn env_or_fallback_api_key(env_var: &str, fallback: Option<String>) -> Option<String> {
    std::env::var(env_var).ok().or(fallback)
}

fn headers(api_key: Option<&str>, model: &str) -> Result<HeaderMap, EmbeddingError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        HeaderName::from_static("x-chroma-embedding-model"),
        HeaderValue::from_str(model).map_err(|err| {
            EmbeddingError::Configuration(format!("invalid embedding model header: {err}"))
        })?,
    );
    headers.insert(
        HeaderName::from_static("x-chroma-token"),
        HeaderValue::from_str(api_key.unwrap_or_default()).map_err(|err| {
            EmbeddingError::Configuration(format!("invalid Chroma API key header: {err}"))
        })?,
    );
    Ok(headers)
}

/// Options for constructing a Chroma Cloud Qwen embedding function.
#[derive(Clone, Debug)]
pub struct ChromaCloudQwenOptions {
    /// Qwen embedding model identifier.
    pub model: String,
    /// Optional task name used to select document and query instructions.
    pub task: Option<String>,
    /// Instruction map keyed by task and target (`documents` or `query`).
    pub instructions: BTreeMap<String, BTreeMap<String, String>>,
    /// Environment variable used to discover the Chroma API key.
    pub api_key_env_var: String,
    /// Optional API key fallback used when the environment variable is absent.
    pub api_key: Option<String>,
    /// Chroma embedding API URL.
    pub url: String,
}

impl Default for ChromaCloudQwenOptions {
    fn default() -> Self {
        Self {
            model: DEFAULT_QWEN_MODEL.to_string(),
            task: None,
            instructions: default_qwen_instructions(),
            api_key_env_var: default_api_key_env_var(),
            api_key: None,
            url: default_chroma_embed_url(),
        }
    }
}

/// Dense embedding function backed by the Chroma Cloud Qwen embedding API.
pub struct ChromaCloudQwenEmbeddingFunction {
    client: reqwest::Client,
    options: ChromaCloudQwenOptions,
}

impl ChromaCloudQwenEmbeddingFunction {
    /// Construct a Qwen embedding function from options.
    pub fn new(options: ChromaCloudQwenOptions) -> Self {
        Self {
            client: reqwest::Client::new(),
            options,
        }
    }

    /// Construct a Qwen embedding function from persisted configuration.
    pub fn try_from_config(
        configuration: &chroma_types::EmbeddingFunctionNewConfiguration,
        chroma_cloud_api_key: Option<String>,
    ) -> Result<Self, EmbeddingError> {
        let config: ChromaCloudQwenConfig = serde_json::from_value(configuration.config.clone())?;
        let api_key = env_or_fallback_api_key(&config.api_key_env_var, chroma_cloud_api_key);
        Ok(Self::new(ChromaCloudQwenOptions {
            model: config.model,
            task: config.task,
            instructions: config.instructions,
            api_key_env_var: config.api_key_env_var,
            api_key,
            url: default_chroma_embed_url(),
        }))
    }

    fn instruction(&self, target: &str) -> String {
        self.options
            .task
            .as_ref()
            .and_then(|task| self.options.instructions.get(task))
            .and_then(|targets| targets.get(target))
            .cloned()
            .unwrap_or_default()
    }

    async fn embed_with_instruction(
        &self,
        input: &[&str],
        instruction: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Serialize)]
        struct Request<'a> {
            texts: &'a [&'a str],
            instructions: String,
        }

        #[derive(Deserialize)]
        struct Response {
            embeddings: Vec<Vec<f32>>,
        }

        let headers = headers(self.options.api_key.as_deref(), &self.options.model)?;
        let response = self
            .client
            .post(&self.options.url)
            .headers(headers)
            .json(&Request {
                texts: input,
                instructions: instruction,
            })
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(EmbeddingError::Provider {
                status: EmbeddingStatus::some(status),
                message,
            });
        }

        let response = response.json::<Response>().await?;
        Ok(response.embeddings)
    }
}

#[async_trait::async_trait]
impl DenseEmbeddingFunction for ChromaCloudQwenEmbeddingFunction {
    fn name(&self) -> &str {
        QWEN_NAME
    }

    fn configuration(&self) -> chroma_types::EmbeddingFunctionConfiguration {
        (
            QWEN_NAME,
            serde_json::json!(ChromaCloudQwenConfig::from(&self.options)),
        )
            .into()
    }

    fn default_space(&self) -> chroma_types::Space {
        chroma_types::Space::Cosine
    }

    fn supported_spaces(&self) -> Vec<chroma_types::Space> {
        vec![
            chroma_types::Space::Cosine,
            chroma_types::Space::L2,
            chroma_types::Space::Ip,
        ]
    }

    async fn embed_documents(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.embed_with_instruction(input, self.instruction("documents"))
            .await
    }

    async fn embed_query(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.embed_with_instruction(input, self.instruction("query"))
            .await
    }
}

/// Options for constructing a Chroma Cloud SPLADE embedding function.
#[derive(Clone, Debug)]
pub struct ChromaCloudSpladeOptions {
    /// SPLADE embedding model identifier.
    pub model: String,
    /// Environment variable used to discover the Chroma API key.
    pub api_key_env_var: String,
    /// Optional API key fallback used when the environment variable is absent.
    pub api_key: Option<String>,
    /// Whether token labels should be retained in sparse vectors.
    pub include_tokens: bool,
    /// Chroma sparse embedding API URL.
    pub url: String,
}

impl Default for ChromaCloudSpladeOptions {
    fn default() -> Self {
        Self {
            model: DEFAULT_SPLADE_MODEL.to_string(),
            api_key_env_var: default_api_key_env_var(),
            api_key: None,
            include_tokens: false,
            url: format!("{}/embed_sparse", default_chroma_embed_url()),
        }
    }
}

/// Sparse embedding function backed by the Chroma Cloud SPLADE embedding API.
pub struct ChromaCloudSpladeEmbeddingFunction {
    client: reqwest::Client,
    options: ChromaCloudSpladeOptions,
}

impl ChromaCloudSpladeEmbeddingFunction {
    /// Construct a SPLADE embedding function from options.
    pub fn new(options: ChromaCloudSpladeOptions) -> Self {
        Self {
            client: reqwest::Client::new(),
            options,
        }
    }

    /// Construct a SPLADE embedding function from persisted configuration.
    pub fn try_from_config(
        configuration: &chroma_types::EmbeddingFunctionNewConfiguration,
        chroma_cloud_api_key: Option<String>,
    ) -> Result<Self, EmbeddingError> {
        let config: ChromaCloudSpladeConfig = serde_json::from_value(configuration.config.clone())?;
        let api_key = env_or_fallback_api_key(&config.api_key_env_var, chroma_cloud_api_key);
        Ok(Self::new(ChromaCloudSpladeOptions {
            model: config.model,
            api_key_env_var: config.api_key_env_var,
            api_key,
            include_tokens: config.include_tokens.unwrap_or(false),
            url: format!("{}/embed_sparse", default_chroma_embed_url()),
        }))
    }

    async fn embed(&self, input: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Serialize)]
        struct Request<'a> {
            texts: &'a [&'a str],
            task: &'static str,
            target: &'static str,
            fetch_tokens: &'static str,
        }

        #[derive(Deserialize)]
        struct Response {
            embeddings: Vec<CloudSparseVector>,
        }

        let headers = headers(self.options.api_key.as_deref(), &self.options.model)?;
        let response = self
            .client
            .post(&self.options.url)
            .headers(headers)
            .json(&Request {
                texts: input,
                task: "",
                target: "",
                fetch_tokens: if self.options.include_tokens {
                    "true"
                } else {
                    "false"
                },
            })
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(EmbeddingError::Provider {
                status: EmbeddingStatus::some(status),
                message,
            });
        }

        response
            .json::<Response>()
            .await?
            .embeddings
            .into_iter()
            .map(SparseVector::try_from)
            .collect()
    }
}

#[async_trait::async_trait]
impl SparseEmbeddingFunction for ChromaCloudSpladeEmbeddingFunction {
    fn name(&self) -> &str {
        SPLADE_NAME
    }

    fn configuration(&self) -> chroma_types::EmbeddingFunctionConfiguration {
        (
            SPLADE_NAME,
            serde_json::json!(ChromaCloudSpladeConfig::from(&self.options)),
        )
            .into()
    }

    async fn embed_documents(&self, input: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError> {
        self.embed(input).await
    }

    async fn embed_query(&self, input: &[&str]) -> Result<Vec<SparseVector>, EmbeddingError> {
        self.embed(input).await
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChromaCloudQwenConfig {
    model: String,
    task: Option<String>,
    #[serde(default = "default_qwen_instructions")]
    instructions: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default = "default_api_key_env_var")]
    api_key_env_var: String,
}

impl From<&ChromaCloudQwenOptions> for ChromaCloudQwenConfig {
    fn from(value: &ChromaCloudQwenOptions) -> Self {
        ChromaCloudQwenConfig {
            model: value.model.clone(),
            task: value.task.clone(),
            instructions: value.instructions.clone(),
            api_key_env_var: value.api_key_env_var.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChromaCloudSpladeConfig {
    model: String,
    #[serde(default = "default_api_key_env_var")]
    api_key_env_var: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    include_tokens: Option<bool>,
}

impl From<&ChromaCloudSpladeOptions> for ChromaCloudSpladeConfig {
    fn from(value: &ChromaCloudSpladeOptions) -> Self {
        ChromaCloudSpladeConfig {
            model: value.model.clone(),
            api_key_env_var: value.api_key_env_var.clone(),
            include_tokens: Some(value.include_tokens),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CloudSparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
    #[serde(alias = "labels")]
    tokens: Option<Vec<String>>,
}

impl TryFrom<CloudSparseVector> for SparseVector {
    type Error = EmbeddingError;

    fn try_from(value: CloudSparseVector) -> Result<Self, Self::Error> {
        let CloudSparseVector {
            indices,
            values,
            tokens,
        } = value;

        if indices.len() != values.len() {
            return Err(EmbeddingError::InvalidInput(
                "sparse vector indices and values have different lengths".to_string(),
            ));
        }

        if let Some(tokens) = tokens.as_ref() {
            if tokens.len() != indices.len() {
                return Err(EmbeddingError::InvalidInput(
                    "sparse vector tokens and indices have different lengths".to_string(),
                ));
            }
        }

        let mut entries = indices
            .into_iter()
            .zip(values)
            .enumerate()
            .map(|(position, (index, score))| {
                let token = tokens
                    .as_ref()
                    .and_then(|tokens| tokens.get(position).cloned());
                (index, score, token)
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(index, _, _)| *index);

        let indices = entries
            .iter()
            .map(|(index, _, _)| *index)
            .collect::<Vec<_>>();
        let values = entries
            .iter()
            .map(|(_, value, _)| *value)
            .collect::<Vec<_>>();
        let tokens = tokens.map(|_| {
            entries
                .into_iter()
                .map(|(_, _, token)| token.unwrap_or_default())
                .collect()
        });

        let vector = match tokens {
            Some(tokens) => SparseVector::new_with_tokens(indices, values, tokens),
            None => SparseVector::new(indices, values),
        }
        .map_err(|err| EmbeddingError::InvalidInput(err.to_string()))?;
        vector
            .validate()
            .map_err(|err| EmbeddingError::InvalidInput(err.to_string()))?;
        Ok(vector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::{DenseEmbeddingFunction, SparseEmbeddingFunction};
    use chroma_types::{EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration};
    use httpmock::MockServer;

    #[test]
    fn qwen_config_round_trip() {
        let options = ChromaCloudQwenOptions {
            task: Some("nl_to_code".to_string()),
            ..Default::default()
        };
        let embedder = ChromaCloudQwenEmbeddingFunction::new(options);

        assert_eq!(
            embedder.configuration(),
            EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                name: QWEN_NAME.to_string(),
                config: serde_json::json!({
                    "model": DEFAULT_QWEN_MODEL,
                    "task": "nl_to_code",
                    "instructions": default_qwen_instructions(),
                    "api_key_env_var": DEFAULT_API_KEY_ENV_VAR
                }),
            })
        );
    }

    #[test]
    fn splade_config_round_trip() {
        let embedder = ChromaCloudSpladeEmbeddingFunction::new(ChromaCloudSpladeOptions::default());

        assert_eq!(
            embedder.configuration(),
            EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                name: SPLADE_NAME.to_string(),
                config: serde_json::json!({
                    "model": DEFAULT_SPLADE_MODEL,
                    "api_key_env_var": DEFAULT_API_KEY_ENV_VAR,
                    "include_tokens": false
                }),
            })
        );
    }

    #[tokio::test]
    async fn qwen_sends_headers_and_query_instructions() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/")
                    .header("x-chroma-token", "test-key")
                    .header("x-chroma-embedding-model", DEFAULT_QWEN_MODEL)
                    .json_body(serde_json::json!({
                        "texts": ["hello"],
                        "instructions": "query instruction"
                    }));
                then.status(200).json_body(serde_json::json!({
                    "embeddings": [[1.0, 2.0]],
                    "num_tokens": 2
                }));
            })
            .await;

        let mut options = ChromaCloudQwenOptions::default();
        options.api_key = Some("test-key".to_string());
        options.url = server.base_url();
        options.task = Some("task".to_string());
        options.instructions = BTreeMap::from([(
            "task".to_string(),
            BTreeMap::from([
                ("documents".to_string(), "document instruction".to_string()),
                ("query".to_string(), "query instruction".to_string()),
            ]),
        )]);
        let embedder = ChromaCloudQwenEmbeddingFunction::new(options);

        let embeddings = embedder.embed_query(&["hello"]).await.unwrap();

        assert_eq!(embeddings, vec![vec![1.0, 2.0]]);
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn splade_sorts_sparse_vectors() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/embed_sparse")
                    .header("x-chroma-token", "test-key")
                    .header("x-chroma-embedding-model", DEFAULT_SPLADE_MODEL)
                    .json_body(serde_json::json!({
                        "texts": ["hello"],
                        "task": "",
                        "target": "",
                        "fetch_tokens": "true"
                    }));
                then.status(200).json_body(serde_json::json!({
                    "embeddings": [{
                        "indices": [9, 1],
                        "values": [0.9, 0.1],
                        "labels": ["nine", "one"]
                    }]
                }));
            })
            .await;

        let mut options = ChromaCloudSpladeOptions::default();
        options.api_key = Some("test-key".to_string());
        options.url = format!("{}/embed_sparse", server.base_url());
        options.include_tokens = true;
        let embedder = ChromaCloudSpladeEmbeddingFunction::new(options);

        let embeddings = embedder.embed_documents(&["hello"]).await.unwrap();

        assert_eq!(
            embeddings,
            vec![SparseVector::new_with_tokens(
                vec![1, 9],
                vec![0.1, 0.9],
                vec!["one".to_string(), "nine".to_string()]
            )
            .unwrap()]
        );
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn cloud_provider_http_errors_are_reported() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method("POST").path("/");
                then.status(500).body("provider failed");
            })
            .await;

        let options = ChromaCloudQwenOptions {
            api_key: Some("test-key".to_string()),
            url: server.base_url(),
            ..Default::default()
        };
        let embedder = ChromaCloudQwenEmbeddingFunction::new(options);

        let err = embedder.embed_documents(&["hello"]).await.unwrap_err();

        match err {
            EmbeddingError::Provider { status, message } => {
                assert_eq!(status.0.unwrap().as_u16(), 500);
                assert_eq!(message, "provider failed");
            }
            other => panic!("expected provider error, got {other:?}"),
        }
    }
}
