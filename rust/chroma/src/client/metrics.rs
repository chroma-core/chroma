use opentelemetry::metrics::Histogram;

#[derive(Clone, Debug)]
pub struct Metrics {
    pub request_latency: Histogram<f64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("chroma_client");

        let request_latency = meter
            .f64_histogram("chroma_client_request_latency_ms")
            .with_description("Latency of requests in milliseconds")
            .build();

        Metrics { request_latency }
    }

    pub fn record_request(&self, operation_name: &str, status_code: u16, latency_ms: f64) {
        self.request_latency.record(
            latency_ms,
            &[
                opentelemetry::KeyValue::new("operation", operation_name.to_string()),
                opentelemetry::KeyValue::new("status_code", status_code.to_string()),
            ],
        );
    }
}
