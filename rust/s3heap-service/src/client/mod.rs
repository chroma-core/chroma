//! gRPC client for heap tender service.
//!
//! This module provides a client for interacting with the heap tender service,
//! which is colocated with the log service on the same nodes but listens on
//! a different port (50052 vs 50051).

/// Configuration types for the heap service client.
pub mod config;
/// gRPC client implementation for the heap service.
pub mod grpc;

pub use config::{GrpcHeapServiceConfig, HeapServiceConfig};
pub use grpc::{GrpcHeapService, GrpcHeapServiceError};
