//! Authentication against the single-tenant dashboard cutout.

use std::{future::Future, pin::Pin, time::Duration};

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use chroma_api_types::GetUserIdentityResponse;
use chroma_types::Collection;
use serde::Deserialize;

use super::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};

#[cfg(test)]
mod tests;

/// Resolves API keys remotely and enforces tenant and permission scopes locally.
/// Requests are not cached, so revoked keys take effect immediately.
#[derive(Clone)]
pub struct DashboardAuth {
    client: reqwest::Client,
    endpoint: reqwest::Url,
}

#[derive(Deserialize)]
struct Claims {
    identity: u64,
    team: String,
    permissions: Vec<Permission>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Permission {
    resource_type: String,
    action: String,
    database: Option<String>,
}

impl Claims {
    fn authorize(&self, action: AuthzAction, resource: &AuthzResource) -> Result<(), AuthError> {
        if resource.tenant.as_deref() != Some(&self.team)
            || !self.permissions.iter().any(|permission| {
                format!("{}:{}", permission.resource_type, permission.action) == action.to_string()
                    && (permission.database.is_none() || permission.database == resource.database)
            })
        {
            return Err(AuthError(StatusCode::FORBIDDEN));
        }
        Ok(())
    }

    fn identity(self) -> GetUserIdentityResponse {
        GetUserIdentityResponse {
            user_id: self.identity.to_string(),
            tenant: self.team,
            databases: self
                .permissions
                .into_iter()
                .filter_map(|p| p.database)
                .collect(),
        }
    }
}

impl DashboardAuth {
    /// Enable remote auth when both service environment variables are set.
    /// An incomplete configuration is an error, never a fallback to no auth.
    pub fn from_env() -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        match (
            std::env::var("CHROMA_AUTHN_CONFIG_API_HOST"),
            std::env::var("CHROMA_AUTHN_CONFIG_DATA_PLANE_API_KEY"),
        ) {
            (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => Ok(None),
            (Ok(host), Ok(key)) => Self::new(&host, &key).map(Some),
            _ => Err(
                "Set both CHROMA_AUTHN_CONFIG_API_HOST and CHROMA_AUTHN_CONFIG_DATA_PLANE_API_KEY"
                    .into(),
            ),
        }
    }

    /// Build a bounded HTTP client. Redirects are disabled to avoid forwarding
    /// tenant credentials to another service.
    pub fn new(
        api_host: &str,
        data_plane_api_key: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut endpoint = reqwest::Url::parse(api_host)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err("Auth API host must be an HTTP(S) origin".into());
        }
        if data_plane_api_key.is_empty() {
            return Err("Data-plane API key must not be empty".into());
        }
        endpoint.set_path("/api/v2/check_api_key");
        let mut credential = HeaderValue::from_str(data_plane_api_key)?;
        credential.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert("x-chroma-data-plane-api-key", credential);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client, endpoint })
    }

    async fn claims(&self, headers: HeaderMap) -> Result<Claims, AuthError> {
        let mut tokens = headers.get_all("x-chroma-token").iter();
        let token = tokens
            .next()
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
            .ok_or(AuthError(StatusCode::UNAUTHORIZED))?;
        if tokens.next().is_some() {
            return Err(AuthError(StatusCode::UNAUTHORIZED));
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&serde_json::json!({"apiKey": token}))
            .send()
            .await
            .map_err(|_| AuthError(StatusCode::SERVICE_UNAVAILABLE))?;
        match response.status() {
            StatusCode::OK => response
                .json()
                .await
                .map_err(|_| AuthError(StatusCode::SERVICE_UNAVAILABLE)),
            StatusCode::UNAUTHORIZED => Err(AuthError(StatusCode::UNAUTHORIZED)),
            StatusCode::FORBIDDEN => Err(AuthError(StatusCode::FORBIDDEN)),
            _ => Err(AuthError(StatusCode::SERVICE_UNAVAILABLE)),
        }
    }
}

impl AuthenticateAndAuthorize for DashboardAuth {
    fn authenticate_and_authorize(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let auth = self.clone();
        let headers = headers.clone();
        Box::pin(async move {
            let claims = auth.claims(headers).await?;
            claims.authorize(action, &resource)?;
            Ok(claims.identity())
        })
    }

    fn authenticate_and_authorize_collection(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
        collection: Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let auth = self.clone();
        let headers = headers.clone();
        Box::pin(async move {
            let claims = auth.claims(headers).await?;
            claims.authorize(action, &resource)?;
            // The collection UUID must belong to the tenant/database in the URL.
            if resource.tenant.as_deref() != Some(&collection.tenant)
                || resource.database.as_deref() != Some(&collection.database)
            {
                return Err(AuthError(StatusCode::FORBIDDEN));
            }
            Ok(claims.identity())
        })
    }

    fn get_user_identity(
        &self,
        headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let auth = self.clone();
        let headers = headers.clone();
        Box::pin(async move { Ok(auth.claims(headers).await?.identity()) })
    }
}
