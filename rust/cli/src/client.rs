use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use thiserror::Error;
use chroma_types::HeartbeatResponse;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Invalid Chroma API key {0}")]
    #[allow(dead_code)]
    InvalidAPIKey(String),
    #[error("Heartbeat request failed")]
    #[allow(dead_code)]
    HeartbeatFailed,
}

#[derive(Default, Debug, Clone)]
pub struct ChromaClient {
    #[allow(dead_code)]
    pub api_url: String,
    #[allow(dead_code)]
    pub tenant_id: String,
    #[allow(dead_code)]
    pub database: Option<String>,
    #[allow(dead_code)]
    pub api_key: Option<String>,
}

#[derive(Deserialize, Default)]
struct EmptyResponse {}

impl ChromaClient {
    #[allow(dead_code)]
    pub fn new(
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

    #[allow(dead_code)]
    pub fn local_default() -> Self {
        Self::new(
            "http://localhost:8000".to_string(),
            "default_tenant".to_string(),
            Some("default_database".to_string()),
            None,
        )
    }

    #[allow(dead_code)]
    pub async fn send_request<T, R>(
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

    #[allow(dead_code)]
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
            None => Ok(None),
        }
    }
    
    #[allow(dead_code)]
    pub async fn heartbeat(&self) -> Result<u128, ClientError> {
        let route = "/api/v2/heartbeat";
        let response = self.send_request::<(), HeartbeatResponse>(
            Method::GET, route, 
            self.headers()?, 
            None
        ).await.map_err(|_| ClientError::HeartbeatFailed)?;
        Ok(response.nanosecond_heartbeat)
    }

}