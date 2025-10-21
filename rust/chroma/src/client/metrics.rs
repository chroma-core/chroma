//! OpenTelemetry instrumentation for Chroma client operations.
//!
//! This module provides metrics collection when the `opentelemetry` feature is enabled.
//! Metrics include request latency histograms and retry counters, tagged by operation
//! name and status code for detailed observability in production deployments.

use opentelemetry::metrics::{Counter, Histogram};

/// OpenTelemetry metrics for monitoring Chroma client operations.
///
/// Tracks request latency and retry behavior to enable observability in production deployments.
/// Only available when the `opentelemetry` feature is enabled.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Histogram of request latencies in milliseconds, tagged by operation name and status code.
    pub request_latency: Histogram<f64>,
    /// Counter of retry attempts, tagged by operation name.
    pub retry_count: Counter<u64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Initializes metrics instruments using the global OpenTelemetry meter.
    ///
    /// Registers a histogram for request latency and a counter for retry attempts
    /// under the `chroma_client` meter namespace.
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("chroma_client");

        let request_latency = meter
            .f64_histogram("chroma_client_request_latency_ms")
            .with_description("Latency of requests in milliseconds")
            .build();

        let retry_count = meter
            .u64_counter("chroma_client_retry_count")
            .with_description("Total number of retries made")
            .build();

        Metrics {
            request_latency,
            retry_count,
        }
    }

    /// Records a completed request's latency and status code.
    ///
    /// Tags the measurement with the operation name and HTTP status code for detailed analysis.
    pub fn record_request(&self, operation_name: &str, status_code: u16, latency_ms: f64) {
        self.request_latency.record(
            latency_ms,
            &[
                opentelemetry::KeyValue::new("operation", operation_name.to_string()),
                opentelemetry::KeyValue::new("status_code", status_code.to_string()),
            ],
        );
    }

    /// Increments the retry counter for a specific operation.
    ///
    /// Called each time a request is retried due to a transient failure.
    pub fn increment_retry(&self, operation_name: &str) {
        self.retry_count.add(
            1,
            &[opentelemetry::KeyValue::new(
                "operation",
                operation_name.to_string(),
            )],
        );
    }
}
