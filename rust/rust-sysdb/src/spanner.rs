//! Spanner client wrapper that implements the Configurable trait.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use thiserror::Error;

use crate::config::SpannerConfig;

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Failed to connect to Spanner: {0}")]
    ConnectionError(String),
}

impl ChromaError for SpannerError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Wrapper around the Spanner client.
pub struct Spanner {
    #[allow(dead_code)]
    client: Option<Client>,
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

                tracing::info!("Connected to Spanner: {}", emulator.database_path());

                Some(client)
            }
            SpannerConfig::Gcp => None,
        };

        Ok(Spanner { client })
    }
}
