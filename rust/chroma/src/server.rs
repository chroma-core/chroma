//! Server functionality for running a local Chroma instance.
//!
//! This module provides the ability to run a single-node Chroma server directly from Rust,
//! which can then be accessed via the HTTP client.
//!
//! # Example
//!
//! ```no_run
//! use chroma::{ChromaHttpClient, client::ChromaHttpClientOptions, client::ChromaAuthMethod};
//! use chroma::server::ChromaServer;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Start a local Chroma server
//! let server = ChromaServer::local().await?;
//!
//! // Connect to it with the HTTP client
//! let client = ChromaHttpClient::new(ChromaHttpClientOptions {
//!     endpoint: server.endpoint().parse()?,
//!     auth_method: ChromaAuthMethod::None,
//!     ..Default::default()
//! });
//!
//! // Use the client
//! let collections = client.list_collections(None, None).await?;
//!
//! // Server shuts down when dropped
//! drop(server);
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;

pub use chroma_frontend::config::FrontendServerConfig;

/// Errors that can occur when starting or running a Chroma server.
#[derive(Error, Debug)]
pub enum ChromaServerError {
    /// Failed to create a directory for persistence.
    #[error("Failed to create persistence directory: {0}")]
    PersistenceError(#[from] std::io::Error),
    /// Server failed to start within the timeout period.
    #[error("Server failed to start within timeout")]
    StartupTimeout,
    /// Failed to connect to the server.
    #[error("Failed to connect to server: {0}")]
    ConnectionError(String),
}

/// A running Chroma server instance.
///
/// The server runs in a background task and will be shut down when this handle is dropped.
/// Use [`endpoint()`](Self::endpoint) to get the URL to connect to with [`ChromaHttpClient`](crate::ChromaHttpClient).
pub struct ChromaServer {
    port: u16,
    persist_path: String,
    _handle: JoinHandle<()>,
}

impl ChromaServer {
    /// Start a local Chroma server with default settings.
    ///
    /// Uses port 8766 and a temporary storage directory.
    /// The server will be shut down when the returned handle is dropped.
    pub async fn local() -> Result<Self, ChromaServerError> {
        let mut config = FrontendServerConfig::single_node_default();
        config.port = 8766; // Use a fixed port to avoid conflicts with default 8000
        config.listen_address = "127.0.0.1".to_string();

        // Create a temp directory for persistence
        let temp_dir = std::env::temp_dir().join(format!("chroma_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir)?;
        config.persist_path = temp_dir.to_string_lossy().to_string();

        Self::with_config(config).await
    }

    /// Start a Chroma server with custom configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use chroma::server::{ChromaServer, FrontendServerConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut config = FrontendServerConfig::single_node_default();
    /// config.port = 8080;
    /// config.persist_path = "./my_data".to_string();
    ///
    /// let server = ChromaServer::with_config(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_config(config: FrontendServerConfig) -> Result<Self, ChromaServerError> {
        let port = config.port;
        let persist_path = config.persist_path.clone();
        let listen_address = config.listen_address.clone();

        // Ensure persist directory exists
        std::fs::create_dir_all(&persist_path)?;

        let handle = tokio::spawn(async move {
            chroma_frontend::frontend_service_entrypoint_with_config(
                Arc::new(()),
                Arc::new(()),
                &config,
                false, // Don't initialize OTel tracing
            )
            .await;
        });

        // Wait for server to be ready by polling the heartbeat endpoint
        let endpoint = format!("http://{}:{}", listen_address, port);
        Self::wait_for_ready(&endpoint).await?;

        Ok(Self {
            port,
            persist_path,
            _handle: handle,
        })
    }

    async fn wait_for_ready(endpoint: &str) -> Result<(), ChromaServerError> {
        let client = reqwest::Client::new();
        let heartbeat_url = format!("{}/api/v2/heartbeat", endpoint);

        for _ in 0..60 {
            // Wait up to 30 seconds (60 * 500ms)
            match client.get(&heartbeat_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(());
                }
                _ => {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }

        Err(ChromaServerError::StartupTimeout)
    }

    /// Get the HTTP endpoint URL for this server.
    ///
    /// Use this to configure a [`ChromaHttpClient`](crate::ChromaHttpClient) to connect to this server.
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Get the port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the persistence path where data is stored.
    pub fn persist_path(&self) -> &str {
        &self.persist_path
    }
}

impl Drop for ChromaServer {
    fn drop(&mut self) {
        // Abort the server task
        self._handle.abort();

        // Clean up temp directory if it looks like a test directory
        if self.persist_path.contains("chroma_test_") {
            let _ = std::fs::remove_dir_all(&self.persist_path);
        }
    }
}
