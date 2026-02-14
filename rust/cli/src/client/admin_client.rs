use crate::client::utils::{send_request, EmptyResponse};
use crate::utils::{get_address_book, Profile};
use axum::http::{HeaderMap, HeaderValue, Method};
use chroma_api_types::GetUserIdentityResponse;
use chroma_frontend::server::CreateDatabasePayload;
use chroma_types::{Database, GetDatabaseResponse, ListDatabasesResponse};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdminClientError {
    #[error("Invalid Chroma API key {0}")]
    InvalidAPIKey(String),
    #[error("Failed to create database {0}")]
    DbCreateFailed(String),
    #[error("Failed to delete database {0}")]
    DbDeleteFailed(String),
    #[error("Failed to list databases")]
    DbListFailed,
    #[error("Failed to get tenant ID")]
    TenantIdNotFound,
    #[error("Healthcheck failed")]
    Healthcheck,
    #[error("DB {0} not found")]
    DbNotFound(String),
}

#[derive(Debug, Default, Deserialize)]
pub struct HealthcheckResponse {
    #[allow(dead_code)]
    is_executor_ready: bool,
}

#[derive(Default, Debug, Clone)]
pub struct AdminClient {
    pub host: String,
    pub tenant_id: String,
    pub api_key: Option<String>,
}

impl AdminClient {
    pub fn new(host: String, tenant_id: String, api_key: Option<String>) -> Self {
        Self {
            host,
            tenant_id,
            api_key,
        }
    }

    pub fn from_profile(host: String, profile: &Profile) -> Self {
        Self::new(
            host,
            profile.tenant_id.clone(),
            Some(profile.api_key.clone()),
        )
    }

    pub fn local_default() -> Self {
        Self {
            host: "http//localhost:8000".to_string(),
            tenant_id: "default_tenant".to_string(),
            api_key: None,
        }
    }

    pub fn local(host: String) -> Self {
        Self {
            host,
            tenant_id: "default_tenant".to_string(),
            api_key: None,
        }
    }

    pub fn headers(&self) -> Result<Option<HeaderMap>, AdminClientError> {
        match self.api_key {
            Some(ref api_key) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "X-Chroma-Token",
                    HeaderValue::from_str(api_key)
                        .map_err(|_| AdminClientError::InvalidAPIKey(api_key.to_string()))?,
                );
                Ok(Some(headers))
            }
            None => Ok(None),
        }
    }

    pub async fn list_databases(&self) -> Result<Vec<Database>, AdminClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let response = send_request::<(), ListDatabasesResponse>(
            &self.host,
            Method::GET,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| AdminClientError::DbListFailed)?;
        Ok(response)
    }

    pub async fn create_database(&self, name: String) -> Result<(), AdminClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let _response = send_request::<CreateDatabasePayload, EmptyResponse>(
            &self.host,
            Method::POST,
            &route,
            self.headers()?,
            Some(&CreateDatabasePayload { name: name.clone() }),
        )
        .await
        .map_err(|_| AdminClientError::DbCreateFailed(name));
        Ok(())
    }

    pub async fn delete_database(&self, name: String) -> Result<(), AdminClientError> {
        let route = format!("/api/v2/tenants/{}/databases/{}", self.tenant_id, name);
        let _response = send_request::<(), EmptyResponse>(
            &self.host,
            Method::DELETE,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| AdminClientError::DbDeleteFailed(name));
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_database(&self, db_name: String) -> Result<Database, AdminClientError> {
        let route = format!("/api/v2/tenants/{}/databases/{}", self.tenant_id, db_name);
        let response = send_request::<(), GetDatabaseResponse>(
            &self.host,
            Method::GET,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| AdminClientError::DbNotFound(db_name))?;
        Ok(response)
    }

    pub async fn get_tenant_id(&self) -> Result<String, AdminClientError> {
        let route = "/api/v2/auth/identity";
        let response = send_request::<(), GetUserIdentityResponse>(
            &self.host,
            Method::GET,
            route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| AdminClientError::TenantIdNotFound)?;
        Ok(response.tenant)
    }

    #[allow(dead_code)]
    pub async fn healthcheck(&self) -> Result<(), AdminClientError> {
        let route = "/api/v2/healthcheck";
        let _response = send_request::<(), HealthcheckResponse>(
            &self.host,
            Method::GET,
            route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| AdminClientError::Healthcheck)?;
        Ok(())
    }
}

pub fn get_admin_client(profile: Option<&Profile>, dev: bool) -> AdminClient {
    let address_book = get_address_book(dev);
    match profile {
        Some(profile) => AdminClient::from_profile(address_book.frontend_url, profile),
        None => AdminClient::local_default(),
    }
}
