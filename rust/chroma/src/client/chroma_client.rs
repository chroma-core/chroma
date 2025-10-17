use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    client::ChromaClientOptions,
    types::{GetUserIdentityResponse, HeartbeatResponse},
};

#[derive(Error, Debug)]
pub enum ChromaClientError {
    #[error("Request error: {0:?}")]
    RequestError(#[from] reqwest::Error),
}

pub struct ChromaClient {
    base_url: String,
    client: reqwest::Client,
}

impl ChromaClient {
    pub fn new(options: ChromaClientOptions) -> Self {
        // todo: add user-agent
        let client = reqwest::Client::builder()
            .default_headers(options.headers())
            .build()
            .expect("Failed to initialize TLS backend");

        ChromaClient {
            base_url: options.base_url.clone(),
            client,
        }
    }

    pub async fn get_auth_identity(&self) -> Result<GetUserIdentityResponse, ChromaClientError> {
        self.send("/api/v2/auth/identity").await
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, ChromaClientError> {
        self.send("/api/v2/heartbeat").await
    }

    async fn send<Response: DeserializeOwned, Path: AsRef<str>>(
        &self,
        path: Path,
    ) -> Result<Response, ChromaClientError> {
        // todo: / normalization
        let url = format!("{}{}", self.base_url, path.as_ref());
        let response = self.client.get(&url).send().await?;
        response.error_for_status_ref()?;
        let json = response.json::<Response>().await?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChromaAuthMethod;
    use std::sync::LazyLock;

    static CHROMA_CLIENT_OPTIONS: LazyLock<ChromaClientOptions> = LazyLock::new(|| {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(err) => {
                if err.not_found() {
                    tracing::warn!("No .env file found");
                } else {
                    panic!("Error loading .env file: {}", err);
                }
            }
        };

        ChromaClientOptions {
            base_url: std::env::var("CHROMA_ENDPOINT")
                .unwrap_or_else(|_| "https://api.trychroma.com".to_string()),
            auth_method: ChromaAuthMethod::cloud_api_key(
                &std::env::var("CHROMA_CLOUD_API_KEY").unwrap(),
            )
            .unwrap(),
        }
    });

    fn test_client() -> ChromaClient {
        ChromaClient::new(CHROMA_CLIENT_OPTIONS.clone())
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_heartbeat() {
        let client = test_client();
        let heartbeat = client.heartbeat().await.unwrap();
        assert!(heartbeat.nanosecond_heartbeat > 0);
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_auth_identity() {
        let client = test_client();
        let identity = client.get_auth_identity().await.unwrap();
        assert!(!identity.tenant.is_empty());
    }
}
