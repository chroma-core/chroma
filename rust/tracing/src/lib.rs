pub mod config;
#[cfg(feature = "grpc")]
pub mod grpc_client_trace_layer;
#[cfg(feature = "grpc")]
pub mod grpc_server_trace_layer;
pub mod init_tracer;
mod server;
#[cfg(feature = "middleware")]
mod tower_tracing;
pub mod util;

pub use config::OpenTelemetryConfig;
pub use server::init_server_otel_tracing;

#[cfg(feature = "grpc")]
pub use grpc_client_trace_layer::*;
#[cfg(feature = "grpc")]
pub use grpc_server_trace_layer::*;
pub use init_tracer::{
    init_global_filter_layer, init_otel_layer, init_otel_tracing, init_panic_tracing_hook,
    init_stdout_layer, init_tracing, OtelFilter, OtelFilterLevel,
};

#[cfg(feature = "middleware")]
pub use tower_tracing::add_tracing_middleware;
