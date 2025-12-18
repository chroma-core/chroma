//! Spanner client wrapper that implements the Configurable trait.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use thiserror::Error;

use crate::config::{SpannerConfig, SpannerEmulatorConfig};

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Spanner configuration missing - either emulator or gcp config is required")]
    ConfigMissing,
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
    client: Client,
}

impl Spanner {
    /// Creates the Spanner instance and database on the emulator if they don't exist.
    /// Uses REST API calls to the emulator's REST endpoint.
    async fn bootstrap_emulator(emulator: &SpannerEmulatorConfig) {
        let http_client = reqwest::Client::new();
        let rest_url = emulator.rest_endpoint();

        // Create instance
        let instance_url = format!("{}/v1/projects/{}/instances", rest_url, emulator.project);
        let instance_body = serde_json::json!({
            "instanceId": emulator.instance,
            "instance": {
                "displayName": emulator.instance,
                "nodeCount": 1
            }
        });

        match http_client
            .post(&instance_url)
            .json(&instance_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 409 => {
                tracing::info!("Spanner instance ready: {}", emulator.instance);
            }
            Ok(resp) => {
                tracing::warn!(
                    "Failed to create Spanner instance: {} - {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
            Err(e) => tracing::warn!("Failed to create Spanner instance: {}", e),
        }

        // Create database
        let database_url = format!(
            "{}/v1/projects/{}/instances/{}/databases",
            rest_url, emulator.project, emulator.instance
        );
        let database_body = serde_json::json!({
            "createStatement": format!("CREATE DATABASE `{}`", emulator.database)
        });

        match http_client
            .post(&database_url)
            .json(&database_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 409 => {
                tracing::info!("Spanner database ready: {}", emulator.database);
            }
            Ok(resp) => {
                tracing::warn!(
                    "Failed to create Spanner database: {} - {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
            Err(e) => tracing::warn!("Failed to create Spanner database: {}", e),
        }
    }
}

#[async_trait::async_trait]
impl Configurable<SpannerConfig> for Spanner {
    async fn try_from_config(
        config: &SpannerConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (database_path, client_config) = if let Some(ref emulator) = config.emulator {
            // Bootstrap emulator with instance and database
            Self::bootstrap_emulator(emulator).await;

            // Configure client to connect to emulator directly (no env var needed)
            let client_config = ClientConfig {
                environment: Environment::Emulator(emulator.grpc_endpoint()),
                ..Default::default()
            };

            (emulator.database_path(), client_config)
        } else {
            // TODO: Add GCP config support
            return Err(Box::new(SpannerError::ConfigMissing) as Box<dyn ChromaError>);
        };

        let client = Client::new(&database_path, client_config)
            .await
            .map_err(|e| {
                Box::new(SpannerError::ConnectionError(e.to_string())) as Box<dyn ChromaError>
            })?;

        tracing::info!("Connected to Spanner: {}", database_path);

        Ok(Spanner { client })
    }
}
