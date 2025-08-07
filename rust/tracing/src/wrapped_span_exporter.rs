use futures::future::BoxFuture;
use opentelemetry::{global, metrics::Counter, KeyValue};
use opentelemetry_sdk::export::trace::SpanExporter;
use std::sync::LazyLock;

/// A wrapper around an OpenTelemetry SpanExporter that counts the number of export calls & tracks errors.
#[derive(Debug)]
pub struct WrappedSpanExporter<E> {
    inner: E,
    // We use a LazyLock so that the counter is only initialized after the global exporter is set up.
    counter: LazyLock<Counter<u64>>,
}

impl From<opentelemetry_otlp::SpanExporter>
    for WrappedSpanExporter<opentelemetry_otlp::SpanExporter>
{
    fn from(exporter: opentelemetry_otlp::SpanExporter) -> Self {
        WrappedSpanExporter::new(exporter)
    }
}

impl<E> WrappedSpanExporter<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            counter: LazyLock::new(|| {
                global::meter("chroma.tracing")
                    .u64_counter("span_exporter_calls")
                    .with_description("Counts the number of span exporter calls")
                    .build()
            }),
        }
    }
}

impl<E> SpanExporter for WrappedSpanExporter<E>
where
    E: SpanExporter + Send + Sync + 'static,
{
    fn export(
        &mut self,
        batch: Vec<opentelemetry_sdk::export::trace::SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let fut = self.inner.export(batch);
        let counter = self.counter.clone();

        Box::pin(async move {
            let result = fut.await;
            match &result {
                Ok(_) => counter.add(1, &[KeyValue::new("status", "success")]),
                Err(err) => {
                    let error_name = match err {
                        opentelemetry::trace::TraceError::ExportFailed(_) => "export_failed",
                        opentelemetry::trace::TraceError::ExportTimedOut(_) => "timeout",
                        opentelemetry::trace::TraceError::TracerProviderAlreadyShutdown => {
                            "shutdown"
                        }
                        opentelemetry::trace::TraceError::Other(_) => "other",
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
        })
    }

    fn shutdown(&mut self) {
        self.inner.shutdown()
    }
}
