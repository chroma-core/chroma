use opentelemetry::trace::{TraceContextExt, TraceId};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use chroma_metrics::{StopWatchUnit, Stopwatch};

pub fn get_current_trace_id() -> TraceId {
    let span = tracing::Span::current();
    span.context().span().span_context().trace_id()
}

pub struct LogSlowOperation {
    start_time: std::time::Instant,
    operation_name: String,
    threshold: std::time::Duration,
}

impl LogSlowOperation {
    pub fn new(operation_name: String, threshold: std::time::Duration) -> Self {
        Self {
            start_time: std::time::Instant::now(),
            operation_name,
            threshold,
        }
    }
}

impl Drop for LogSlowOperation {
    fn drop(&mut self) {
        let elapsed = self.start_time.elapsed();
        if elapsed > self.threshold {
            tracing::warn!(
                "Operation '{}' took {:?}, which exceeds the threshold of {:?}",
                self.operation_name,
                elapsed,
                self.threshold
            );
        }
    }
}
