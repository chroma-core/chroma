//! Rust client library for the Chroma AI-native database.
//!
//! This crate provides a comprehensive, production-ready client for interacting with [Chroma](https://www.trychroma.com),
//! an AI-native open-source embedding database. Chroma transforms embeddings into queryable databases,
//! enabling similarity search, filtering, and retrieval operations over high-dimensional vector data.
//!
//! # Features
//!
//! - **Automatic retries** - Configurable exponential backoff with jitter for resilient network operations
//! - **OpenTelemetry support** - Optional metrics collection for observability (enable `opentelemetry` feature)
//! - **TLS flexibility** - Support for both native-tls and rustls backends
//!
//! # Core Types
//!
//! - [`ChromaHttpClient`] - Main client for database-level operations (create/list/delete collections)
//! - [`collection::ChromaCollection`] - Collection handle for CRUD operations on records (add/get/query/update/delete)
//! - [`ChromaHttpClientOptions`] - Configuration for client initialization including auth and retry behavior
//!
//! # Quick Start
//!
//! ## Connecting to Chroma Cloud
//!
//! ```
//! use chroma::{ChromaHttpClient, client::ChromaHttpClientOptions, client::ChromaAuthMethod};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!
//! let client = ChromaHttpClient::cloud()?;
//!
//! let heartbeat = client.heartbeat().await?;
//! println!("Connected! Heartbeat: {}", heartbeat.nanosecond_heartbeat);
//! # Ok(())
//! # }
//! ```
//!
//! ## Managing Databases
//!
//! ```
//! # use chroma::ChromaHttpClient;
//! # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
//! client.create_database("my_database".to_string()).await?;
//!
//! let databases = client.list_databases().await?;
//! for db in databases {
//!     println!("Found database: {}", db.name);
//! }
//!
//! client.delete_database("my_database".to_string()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Working with Collections
//!
//! ```
//! # use chroma::ChromaHttpClient;
//! # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
//! let collections = client
//!     .list_collections(100, None)
//!     .await?;
//!
//! for collection in collections {
//!     println!("Collection: {}", collection.name());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! The client supports multiple authentication methods:
//!
//! - **Cloud API Key** - For Chroma Cloud deployments using `ChromaAuthMethod::cloud_api_key`
//! - **Custom Headers** - For self-hosted instances with custom auth via `ChromaAuthMethod::HeaderAuth`
//! - **No Auth** - For local development with `ChromaAuthMethod::None`
//!
//! # Error Handling
//!
//! All operations return `Result<T, ChromaHttpClientError>` where [`ChromaHttpClientError`](client::ChromaHttpClientError)
//! captures network errors, serialization failures, and validation errors.
//!
//! ```
//! # use chroma::ChromaHttpClient;
//! # use chroma::client::ChromaHttpClientError;
//! # async fn example(client: ChromaHttpClient) {
//! match client.heartbeat().await {
//!     Ok(response) => println!("Heartbeat: {}", response.nanosecond_heartbeat),
//!     Err(ChromaHttpClientError::RequestError(e)) => eprintln!("Network error: {}", e),
//!     Err(e) => eprintln!("Other error: {}", e),
//! }
//! # }
//! ```
//!
//! # Feature Flags
//!
//! - `default` - Enables `native-tls` for TLS support
//! - `native-tls` - Use native system TLS (OpenSSL on Linux, Secure Transport on macOS)
//! - `rustls` - Use pure-Rust TLS implementation
//! - `opentelemetry` - Enable metrics collection for request latency and retry counts

#![deny(missing_docs)]

pub mod client;
mod collection;
pub mod embed;
pub mod types;

pub use client::ChromaHttpClient;
pub use client::ChromaHttpClientOptions;
pub use collection::ChromaCollection;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ChromaAuthMethod, ChromaHttpClientOptions};
    use std::collections::HashSet;
    use std::sync::{Arc, LazyLock, Mutex};
    use uuid::Uuid;

    static CHROMA_CLIENT_OPTIONS: LazyLock<ChromaHttpClientOptions> = LazyLock::new(|| {
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

        ChromaHttpClientOptions {
            endpoint: std::env::var("CHROMA_ENDPOINT")
                .unwrap_or_else(|_| "https://api.trychroma.com".to_string())
                .parse()
                .unwrap(),
            auth_method: ChromaAuthMethod::cloud_api_key(
                &std::env::var("CHROMA_CLOUD_API_KEY").unwrap(),
            )
            .unwrap(),
            ..Default::default()
        }
    });

    pub struct TestClient {
        client: ChromaHttpClient,
        collections: Arc<Mutex<HashSet<String>>>,
    }

    pub fn unique_collection_name(base: &str) -> String {
        format!("{}_{}", base, Uuid::new_v4())
    }

    impl TestClient {
        pub async fn new_collection(&mut self, name: &str) -> ChromaCollection {
            let name = unique_collection_name(name);
            {
                let mut collections = self.collections.lock().unwrap();
                collections.insert(name.clone());
            }
            self.client
                .get_or_create_collection(name, None, None)
                .await
                .unwrap()
        }
    }

    impl std::ops::Deref for TestClient {
        type Target = ChromaHttpClient;

        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    pub async fn with_client<F, Fut>(callback: F)
    where
        F: FnOnce(TestClient) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let client = ChromaHttpClient::new(CHROMA_CLIENT_OPTIONS.clone());
        let collections = Arc::new(Mutex::new(HashSet::default()));
        let client = TestClient {
            client,
            collections: Arc::clone(&collections),
        };
        callback(client).await;
        let client = ChromaHttpClient::new(CHROMA_CLIENT_OPTIONS.clone());
        let collections = { collections.lock().unwrap().clone() };
        for collection in collections.iter() {
            if let Err(err) = client.delete_collection(collection.clone()).await {
                tracing::error!("failed to cleanup {collection}: {err}");
            }
        }
    }
}
