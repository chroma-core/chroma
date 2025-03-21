use std::error::Error;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use chroma_types::{Database, ListDatabasesResponse};
use crate::utils::Profile;
use chroma_frontend::server::CreateDatabasePayload;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Invalid Chroma API key {0}")]
    InvalidAPIKey(String),
    #[error("Failed to create database {0}")]
    DbCreateFailed(String),
    #[error("Failed to delete database {0}")]
    DbDeleteFailed(String),
    #[error("Failed to list databases")]
    DbListFailed,
}

#[derive(Default, Debug)]
pub struct ChromaClient {
    pub api_url: String,
    pub tenant_id: String,
    pub database: Option<String>,
    pub api_key: Option<String>,
}

impl ChromaClient {
    fn new(api_url: String, tenant_id: String, database: Option<String>, api_key: Option<String>) -> Self {
        Self { api_url, tenant_id, database, api_key }
    }
    
    pub fn local_default() -> Self {
        Self::new(
            "localhost:8000".to_string(),
            "default_tenant".to_string(),
            Some("default_database".to_string()),
            None
        )
    }
    
    pub fn from_profile(profile: &Profile, api_url: String) -> Self {
        Self::new(api_url, profile.tenant_id.clone(), None, Some(profile.api_key.clone()))
    }
    
    async fn send_request<T, R>(
        &self,
        method: Method,
        route: &str,
        headers: Option<HeaderMap>,
        body: Option<&T>,
    ) -> Result<R, Box<dyn Error>>
    where
        T: Serialize,
        R: DeserializeOwned + Default,
    {
        let url = format!("{}{}", self.api_url, route);
        
        let client = Client::new();
        let mut request_builder = client.request(method, url);

        if let Some(headers) = headers {
            request_builder = request_builder.headers(headers);
        }

        if let Some(b) = body {
            request_builder = request_builder.json(b);
        }
        
        let response = request_builder.send().await?.error_for_status()?;
        
        let parsed_response = response.json::<R>().await?;
        Ok(parsed_response)
    }
    
    fn headers(&self) -> Result<Option<HeaderMap>, ClientError> {
        match self.api_key { 
            Some(ref api_key) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "X-Chroma-Token",
                    HeaderValue::from_str(api_key)
                        .map_err(|_| ClientError::InvalidAPIKey(api_key.to_string()))?,
                );
                Ok(Some(headers))
            }
            None => Ok(None)
        }
    }

    pub async fn list_databases(&self) -> Result<Vec<Database>, ClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let response = self.send_request::<(), ListDatabasesResponse>(
            Method::GET, 
            &route,
            self.headers()?,
            None,
        ).await.map_err(|_| ClientError::DbListFailed)?;
        Ok(response)
    }

    pub async fn create_database(&self, name: String) -> Result<(), ClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let response = self.send_request::<CreateDatabasePayload, ()>(
            Method::POST,
            &route,
            self.headers()?,
            Some(&CreateDatabasePayload { name: name.clone() }),
        ).await.map_err(|_| ClientError::DbCreateFailed(name))?;
        Ok(response)
    }

    pub async fn delete_database(&self, name: String) -> Result<(), ClientError> {
        let route = format!("/api/v2/tenants/{}/databases/{}", self.tenant_id, name);
        let response = self.send_request::<(), ()>(
            Method::DELETE,
            &route,
            self.headers()?,
            None,
        ).await.map_err(|_| ClientError::DbDeleteFailed(name))?;
        Ok(response)
    }
}