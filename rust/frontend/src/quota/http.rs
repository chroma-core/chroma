use std::{future::Future, pin::Pin};

use chroma_types::{plan::SearchPayload, CollectionUuid, Metadata, UpdateMetadata, Where};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::{
    Action, QuotaEnforcer, QuotaEnforcerError, QuotaExceededError, QuotaOverrides, QuotaPayload,
};

/// Configuration for the HTTP-based quota enforcer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpQuotaEnforcerConfig {
    /// Base URL of the quota sidecar (e.g., "http://localhost:8081").
    pub url: String,
    /// Request timeout in milliseconds. Default: 5000.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// If true, allow requests when the sidecar is unreachable.
    /// If false, deny requests on sidecar failure.
    #[serde(default)]
    pub fail_open: bool,
}

fn default_timeout_ms() -> u64 {
    5000
}

/// An all-owned version of [`QuotaPayload`] for HTTP deserialization.
///
/// The sidecar receives this struct and reconstructs a borrowed
/// [`QuotaPayload`] from it.
#[derive(Debug, Serialize, Deserialize)]
pub struct OwnedQuotaPayload {
    pub action: Action,
    pub tenant: String,
    pub api_token: Option<String>,
    pub create_collection_metadata: Option<Metadata>,
    pub update_collection_metadata: Option<UpdateMetadata>,
    pub ids: Option<Vec<String>>,
    pub add_embeddings: Option<Vec<Vec<f32>>>,
    pub update_embeddings: Option<Vec<Option<Vec<f32>>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<Metadata>>>,
    pub update_metadatas: Option<Vec<Option<UpdateMetadata>>>,
    pub r#where: Option<Where>,
    pub collection_name: Option<String>,
    pub collection_new_name: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub n_results: Option<u32>,
    pub query_embeddings: Option<Vec<Vec<f32>>>,
    pub query_ids: Option<Vec<String>>,
    pub collection_uuid: Option<CollectionUuid>,
    pub search_payloads: Vec<SearchPayload>,
}

impl<'a> From<&'a QuotaPayload<'a>> for OwnedQuotaPayload {
    fn from(p: &'a QuotaPayload<'a>) -> Self {
        Self {
            action: p.action.clone(),
            tenant: p.tenant.clone(),
            api_token: p.api_token.clone(),
            create_collection_metadata: p.create_collection_metadata.cloned(),
            update_collection_metadata: p.update_collection_metadata.cloned(),
            ids: p.ids.map(|s| s.to_vec()),
            add_embeddings: p.add_embeddings.map(|s| s.to_vec()),
            update_embeddings: p.update_embeddings.map(|s| s.to_vec()),
            documents: p.documents.map(|s| s.to_vec()),
            uris: p.uris.map(|s| s.to_vec()),
            metadatas: p.metadatas.map(|s| s.to_vec()),
            update_metadatas: p.update_metadatas.map(|s| s.to_vec()),
            r#where: p.r#where.cloned(),
            collection_name: p.collection_name.map(|s| s.to_string()),
            collection_new_name: p.collection_new_name.map(|s| s.to_string()),
            limit: p.limit,
            offset: p.offset,
            n_results: p.n_results,
            query_embeddings: p.query_embeddings.map(|s| s.to_vec()),
            query_ids: p.query_ids.map(|s| s.to_vec()),
            collection_uuid: p.collection_uuid,
            search_payloads: p.search_payloads.to_vec(),
        }
    }
}

/// HTTP response from the quota sidecar on success (200).
#[derive(Debug, Serialize, Deserialize)]
pub struct QuotaEnforceResponse {
    pub overrides: Option<QuotaOverrides>,
}

/// HTTP error response from the quota sidecar (4xx/5xx).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum QuotaEnforceErrorResponse {
    QuotaExceeded(QuotaExceededError),
    ApiKeyMissing,
    Unauthorized,
    InitializationFailed,
    GenericQuotaError { message: String },
}

impl From<QuotaEnforceErrorResponse> for QuotaEnforcerError {
    fn from(resp: QuotaEnforceErrorResponse) -> Self {
        match resp {
            QuotaEnforceErrorResponse::QuotaExceeded(e) => QuotaEnforcerError::QuotaExceeded(e),
            QuotaEnforceErrorResponse::ApiKeyMissing => QuotaEnforcerError::ApiKeyMissing,
            QuotaEnforceErrorResponse::Unauthorized => QuotaEnforcerError::Unauthorized,
            QuotaEnforceErrorResponse::InitializationFailed => {
                QuotaEnforcerError::InitializationFailed
            }
            QuotaEnforceErrorResponse::GenericQuotaError { message } => {
                QuotaEnforcerError::GenericQuotaError(message)
            }
        }
    }
}

impl From<&QuotaEnforcerError> for QuotaEnforceErrorResponse {
    fn from(err: &QuotaEnforcerError) -> Self {
        match err {
            QuotaEnforcerError::QuotaExceeded(e) => QuotaEnforceErrorResponse::QuotaExceeded(
                QuotaExceededError {
                    usage_type: e.usage_type.clone(),
                    action: e.action.clone(),
                    usage: e.usage,
                    limit: e.limit,
                    message: e.message.clone(),
                },
            ),
            QuotaEnforcerError::ApiKeyMissing => QuotaEnforceErrorResponse::ApiKeyMissing,
            QuotaEnforcerError::Unauthorized => QuotaEnforceErrorResponse::Unauthorized,
            QuotaEnforcerError::InitializationFailed => {
                QuotaEnforceErrorResponse::InitializationFailed
            }
            QuotaEnforcerError::GenericQuotaError(msg) => {
                QuotaEnforceErrorResponse::GenericQuotaError {
                    message: msg.clone(),
                }
            }
        }
    }
}

/// Quota enforcer that delegates to an HTTP sidecar.
pub struct HttpQuotaEnforcer {
    client: Client,
    enforce_url: String,
    fail_open: bool,
}

impl HttpQuotaEnforcer {
    pub fn new(config: &HttpQuotaEnforcerConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .expect("Failed to build HTTP client for quota enforcer");

        let enforce_url = format!("{}/enforce", config.url.trim_end_matches('/'));

        Self {
            client,
            enforce_url,
            fail_open: config.fail_open,
        }
    }
}

impl QuotaEnforcer for HttpQuotaEnforcer {
    fn enforce<'other>(
        &'other self,
        payload: &'other QuotaPayload<'other>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<QuotaOverrides>, QuotaEnforcerError>> + Send + 'other,
        >,
    > {
        Box::pin(async move {
            let owned = OwnedQuotaPayload::from(payload);

            let response: reqwest::Response =
                match self.client.post(&self.enforce_url).json(&owned).send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                    tracing::error!("Quota sidecar request failed: {}", e);
                    if self.fail_open {
                        tracing::warn!("Quota sidecar unreachable, failing open");
                        return Ok(None);
                    }
                    return Err(QuotaEnforcerError::GenericQuotaError(format!(
                        "Quota sidecar unreachable: {}",
                        e
                    )));
                }
            };

            let status = response.status();

            if status.is_success() {
                let body = response.json::<QuotaEnforceResponse>().await.map_err(|e| {
                    QuotaEnforcerError::GenericQuotaError(format!(
                        "Failed to parse quota response: {}",
                        e
                    ))
                })?;
                Ok(body.overrides)
            } else {
                let error_body =
                    response.json::<QuotaEnforceErrorResponse>().await.map_err(|e| {
                        QuotaEnforcerError::GenericQuotaError(format!(
                            "Failed to parse quota error response (status {}): {}",
                            status, e
                        ))
                    })?;
                Err(error_body.into())
            }
        })
    }
}
