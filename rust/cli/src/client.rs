use crate::utils::{get_address_book, send_request, Profile};
use chroma_frontend::server::CreateDatabasePayload;
use chroma_types::{Database, GetUserIdentityResponse, ListDatabasesResponse};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChromaClientError {
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
}

#[derive(Default, Debug, Clone)]
pub struct ChromaClient {
    pub api_url: String,
    pub tenant_id: String,
    #[allow(dead_code)]
    pub database: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Deserialize, Default)]
struct EmptyResponse {}

impl ChromaClient {
    fn new(
        api_url: String,
        tenant_id: String,
        database: Option<String>,
        api_key: Option<String>,
    ) -> Self {
        Self {
            api_url,
            tenant_id,
            database,
            api_key,
        }
    }

    pub fn local_default() -> Self {
        Self::new(
            "http://localhost:8000".to_string(),
            "default_tenant".to_string(),
            Some("default_database".to_string()),
            None,
        )
    }

    pub fn from_profile(profile: &Profile, api_url: String) -> Self {
        Self::new(
            api_url,
            profile.tenant_id.clone(),
            None,
            Some(profile.api_key.clone()),
        )
    }

    fn headers(&self) -> Result<Option<HeaderMap>, ChromaClientError> {
        match self.api_key {
            Some(ref api_key) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "X-Chroma-Token",
                    HeaderValue::from_str(api_key)
                        .map_err(|_| ChromaClientError::InvalidAPIKey(api_key.to_string()))?,
                );
                Ok(Some(headers))
            }
            None => Ok(None),
        }
    }

    pub async fn list_databases(&self) -> Result<Vec<Database>, ChromaClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let response = send_request::<(), ListDatabasesResponse>(
            &self.api_url,
            Method::GET,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| ChromaClientError::DbListFailed)?;
        Ok(response)
    }

    pub async fn create_database(&self, name: String) -> Result<(), ChromaClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let _response = send_request::<CreateDatabasePayload, EmptyResponse>(
            &self.api_url,
            Method::POST,
            &route,
            self.headers()?,
            Some(&CreateDatabasePayload { name: name.clone() }),
        )
        .await
        .map_err(|_| ChromaClientError::DbCreateFailed(name));
        Ok(())
    }

    pub async fn delete_database(&self, name: String) -> Result<(), ChromaClientError> {
        let route = format!("/api/v2/tenants/{}/databases/{}", self.tenant_id, name);
        let _response = send_request::<(), EmptyResponse>(
            &self.api_url,
            Method::DELETE,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| ChromaClientError::DbDeleteFailed(name));
        Ok(())
    }

    pub async fn get_tenant_id(&self) -> Result<String, ChromaClientError> {
        let route = "/api/v2/auth/identity";
        let response = send_request::<(), GetUserIdentityResponse>(
            &self.api_url,
            Method::GET,
            route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| ChromaClientError::TenantIdNotFound)?;
        Ok(response.tenant)
    }
}

pub fn get_chroma_client(profile: Option<&Profile>, dev: bool) -> ChromaClient {
    let address_book = get_address_book(dev);
    match profile {
        Some(profile) => ChromaClient::from_profile(profile, address_book.frontend_url),
        None => ChromaClient::local_default(),
    }
}
