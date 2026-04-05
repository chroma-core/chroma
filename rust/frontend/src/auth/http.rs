use std::{collections::HashMap, future::Future, pin::Pin};

use axum::http::{HeaderMap, StatusCode};
use chroma_api_types::GetUserIdentityResponse;
use chroma_types::Collection;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};

/// Configuration for the HTTP-based auth backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpAuthConfig {
    /// Base URL of the auth sidecar (e.g., "http://localhost:8082").
    pub url: String,
    /// Request timeout in milliseconds. Default: 5000.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5000
}

/// Known auth-relevant headers to forward to the sidecar.
const AUTH_HEADERS: &[&str] = &[
    "authorization",
    "x-chroma-token",
    "x-chroma-api-key",
    "x-api-key",
];

fn extract_auth_headers(headers: &HeaderMap) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for &name in AUTH_HEADERS {
        if let Some(value) = headers.get(name) {
            if let Ok(v) = value.to_str() {
                result.insert(name.to_string(), v.to_string());
            }
        }
    }
    result
}

/// Request body for authenticate-and-authorize.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
    pub headers: HashMap<String, String>,
    pub action: AuthzAction,
    pub resource: AuthzResource,
}

/// Request body for authenticate-and-authorize-collection.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthCollectionRequest {
    pub headers: HashMap<String, String>,
    pub action: AuthzAction,
    pub resource: AuthzResource,
    pub collection: Collection,
}

/// Request body for get-user-identity.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityRequest {
    pub headers: HashMap<String, String>,
}

async fn send_auth_request(
    client: &Client,
    url: &str,
    body: &impl Serialize,
) -> Result<GetUserIdentityResponse, AuthError> {
    let response = client.post(url).json(body).send().await.map_err(|e| {
        tracing::error!("Auth sidecar request failed: {}", e);
        AuthError(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    let status = response.status();
    if status.is_success() {
        response
            .json::<GetUserIdentityResponse>()
            .await
            .map_err(|e| {
                tracing::error!("Failed to parse auth response: {}", e);
                AuthError(StatusCode::INTERNAL_SERVER_ERROR)
            })
    } else {
        let auth_status = match status.as_u16() {
            401 => StatusCode::UNAUTHORIZED,
            403 => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Err(AuthError(auth_status))
    }
}

/// Auth implementation that delegates to an HTTP sidecar.
pub struct HttpAuthenticateAndAuthorize {
    client: Client,
    auth_url: String,
    auth_collection_url: String,
    identity_url: String,
}

impl HttpAuthenticateAndAuthorize {
    pub fn new(config: &HttpAuthConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .expect("Failed to build HTTP client for auth");

        let base = config.url.trim_end_matches('/');
        Self {
            client,
            auth_url: format!("{}/authenticate-and-authorize", base),
            auth_collection_url: format!("{}/authenticate-and-authorize-collection", base),
            identity_url: format!("{}/get-user-identity", base),
        }
    }
}

impl AuthenticateAndAuthorize for HttpAuthenticateAndAuthorize {
    fn authenticate_and_authorize(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let request = AuthRequest {
            headers: extract_auth_headers(headers),
            action,
            resource,
        };
        let client = self.client.clone();
        let url = self.auth_url.clone();

        Box::pin(async move { send_auth_request(&client, &url, &request).await })
    }

    fn authenticate_and_authorize_collection(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
        collection: Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let request = AuthCollectionRequest {
            headers: extract_auth_headers(headers),
            action,
            resource,
            collection,
        };
        let client = self.client.clone();
        let url = self.auth_collection_url.clone();

        Box::pin(async move { send_auth_request(&client, &url, &request).await })
    }

    fn get_user_identity(
        &self,
        headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let request = IdentityRequest {
            headers: extract_auth_headers(headers),
        };
        let client = self.client.clone();
        let url = self.identity_url.clone();

        Box::pin(async move { send_auth_request(&client, &url, &request).await })
    }
}
