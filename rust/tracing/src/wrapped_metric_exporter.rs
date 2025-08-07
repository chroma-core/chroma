use opentelemetry::{global, metrics::Counter, KeyValue};
use opentelemetry_sdk::metrics::{
    data::ResourceMetrics, exporter::PushMetricExporter, MetricResult,
};
use std::sync::LazyLock;

/// A wrapper around an OpenTelemetry MetricExporter that counts the number of export calls & tracks errors.
#[derive(Debug)]
pub struct WrappedMetricExporter<E> {
    inner: E,
    counter: LazyLock<Counter<u64>>,
}

impl<E> WrappedMetricExporter<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            // We use a LazyLock so that the counter is only initialized after the global exporter is set up.
            counter: LazyLock::new(|| {
                global::meter("chroma.tracing")
                    .u64_counter("metric_exporter_calls")
                    .with_description("Counts the number of metric exporter calls")
                    .build()
            }),
        }
    }
}

impl From<opentelemetry_otlp::MetricExporter>
    for WrappedMetricExporter<opentelemetry_otlp::MetricExporter>
{
    fn from(exporter: opentelemetry_otlp::MetricExporter) -> Self {
        WrappedMetricExporter::new(exporter)
    }
}

#[async_trait::async_trait]
impl<E> PushMetricExporter for WrappedMetricExporter<E>
where
    E: PushMetricExporter + Send + Sync + 'static,
{
    async fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        let counter = self.counter.clone();
        let result = self.inner.export(metrics).await;
        match &result {
            Ok(_) => counter.add(1, &[KeyValue::new("status", "success")]),
            Err(err) => {
                let error_name = match err {
                    opentelemetry_sdk::metrics::MetricError::Other(_) => "other",
                    opentelemetry_sdk::metrics::MetricError::Config(_) => "config",
                    opentelemetry_sdk::metrics::MetricError::ExportErr(_) => "export_failed",
                    opentelemetry_sdk::metrics::MetricError::InvalidInstrumentConfiguration(_) => {
                        "invalid_instrument_configuration"
                    }
                    _ => "unknown",
                };
                counter.add(
                    1,
                    &[
                        KeyValue::new("status", "error"),
                        KeyValue::new("error", error_name.to_string()),
                    ],
                );
            }
        }
        result
    }

    async fn force_flush(&self) -> MetricResult<()> {
        self.inner.force_flush().await
    }

    fn shutdown(&self) -> MetricResult<()> {
        self.inner.shutdown()
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        self.inner.temporality()
    }
}
