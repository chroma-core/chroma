//! Client configuration and connection management for Chroma.
//!
//! This module contains:
//! - [`ChromaHttpClient`] - The main client handle for database operations
//! - [`ChromaHttpClientOptions`] - Configuration builder for client initialization
//! - [`ChromaAuthMethod`] - Authentication strategy enumeration
//! - [`ChromaRetryOptions`] - Retry behavior configuration
//! - [`ChromaHttpClientError`] - Error type for client operations
//!
//! When the `opentelemetry` feature is enabled, metrics collection is available
//! through internal instrumentation.

mod chroma_http_client;
#[cfg(feature = "opentelemetry")]
mod metrics;
mod options;

pub use chroma_http_client::*;
pub use options::*;
