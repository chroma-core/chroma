pub(crate) mod config;
mod ingest;

// Re-export the ingest provider for use in the worker
pub(crate) use ingest::*;
