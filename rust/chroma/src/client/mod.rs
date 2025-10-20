//! Client configuration and connection management for Chroma.
//!
//! This module contains:
//! - [`ChromaClient`] - The main client handle for database operations
//! - [`ChromaClientOptions`] - Configuration builder for client initialization
//! - [`ChromaAuthMethod`] - Authentication strategy enumeration
//! - [`ChromaRetryOptions`] - Retry behavior configuration
//! - [`ChromaClientError`] - Error type for client operations
//!
//! When the `opentelemetry` feature is enabled, metrics collection is available
//! through internal instrumentation.

mod chroma_http_client;
#[cfg(feature = "opentelemetry")]
mod metrics;
mod options;

pub use chroma_http_client::*;
pub use options::*;
