use reqwest::header::{HeaderMap, HeaderValue};
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
}
