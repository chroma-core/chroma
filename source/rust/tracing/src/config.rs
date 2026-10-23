//! Shared OpenTelemetry configuration for Chroma services.

use serde::{Deserialize, Serialize};

use crate::{OtelFilter, OtelFilterLevel};

fn default_otel_service_name() -> String {
    "chromadb".to_string()
}

fn default_otel_filters() -> Vec<OtelFilter> {
    vec![OtelFilter {
        crate_name: "chroma_frontend".to_string(),
        filter_level: OtelFilterLevel::Trace,
    }]
}

/// Shared OTLP configuration, retaining the frontend's defaults for compatibility.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    /// OTLP gRPC collector endpoint.
    pub endpoint: String,
    /// Exported service name; defaults to `chromadb`.
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
    /// Additional tracing filters; defaults to `chroma_frontend=trace`.
    #[serde(default = "default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}
