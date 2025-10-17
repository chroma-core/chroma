use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::types::HeartbeatResponse;

#[derive(Error, Debug)]
pub enum ChromaClientError {
    #[error("Request error: {0:?}")]
    RequestError(#[from] reqwest::Error),
}

pub struct ChromaClientOptions {
    pub base_url: String,
}

impl Default for ChromaClientOptions {
    fn default() -> Self {
        ChromaClientOptions {
            base_url: "https://api.trychroma.com".to_string(),
        }
    }
}

pub struct ChromaClient {
    base_url: String,
    client: reqwest::Client,
}

impl ChromaClient {
    pub fn new(options: ChromaClientOptions) -> Self {
        ChromaClient {
            base_url: options.base_url,
            client: reqwest::Client::new(),
        }
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

    #[tokio::test]
    async fn test_heartbeat() {
        let client = ChromaClient::new(Default::default());
        let heartbeat = client.heartbeat().await.unwrap();
        assert!(heartbeat.nanosecond_heartbeat > 0);
    }
}
