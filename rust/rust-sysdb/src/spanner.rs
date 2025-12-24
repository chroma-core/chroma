//! Spanner client wrapper that implements the Configurable trait.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use thiserror::Error;

use crate::config::SpannerConfig;

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Failed to connect to Spanner database: {0}")]
    ConnectionError(String),
    #[error("Failed to configure Spanner client: {0}")]
    ConfigurationError(String),
}

impl ChromaError for SpannerError {
    fn code(&self) -> ErrorCodes {
        match self {
            SpannerError::ConnectionError(_) => ErrorCodes::Internal,
            SpannerError::ConfigurationError(_) => ErrorCodes::Internal,
        }
    }
}

/// Wrapper around the Spanner client.
#[derive(Clone)]
pub struct Spanner {
    client: Client,
}

impl Spanner {
    pub async fn close(self) {
        self.client.close().await;
    }
}

#[async_trait::async_trait]
impl Configurable<SpannerConfig> for Spanner {
    async fn try_from_config(
        config: &SpannerConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let client = match config {
            SpannerConfig::Emulator(emulator) => {
                // Configure client to connect to emulator directly (no env var needed)
                let client_config = ClientConfig {
                    environment: Environment::Emulator(emulator.grpc_endpoint()),
                    ..Default::default()
                };

                let client = Client::new(&emulator.database_path(), client_config)
                    .await
                    .map_err(|e| {
                        Box::new(SpannerError::ConnectionError(e.to_string()))
                            as Box<dyn ChromaError>
                    })?;

                tracing::info!("Connected to Spanne emulator: {}", emulator.database_path());

                client
            }
            SpannerConfig::Gcp(gcp) => {
                let client_config = ClientConfig::default().with_auth().await.map_err(|e| {
                    Box::new(SpannerError::ConfigurationError(e.to_string()))
                        as Box<dyn ChromaError>
                })?;

                let client = Client::new(&gcp.database_path(), client_config)
                    .await
                    .map_err(|e| {
                        Box::new(SpannerError::ConnectionError(e.to_string()))
                            as Box<dyn ChromaError>
                    })?;

                tracing::info!("Connected to Spanner GCP: {}", gcp.database_path());

                client
            }
        };

        Ok(Spanner { client })
    }
}
