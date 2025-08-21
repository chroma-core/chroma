use opentelemetry::{
    trace::{TraceContextExt, TraceId},
    KeyValue,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub enum StopWatchUnit {
    Micros,
    Millis,
    Seconds,
}

pub struct Stopwatch<'a>(
    &'a opentelemetry::metrics::Histogram<u64>,
    &'a [KeyValue],
    std::time::Instant,
    StopWatchUnit,
);

impl<'a> Stopwatch<'a> {
    pub fn new(
        histogram: &'a opentelemetry::metrics::Histogram<u64>,
        attributes: &'a [KeyValue],
        unit: StopWatchUnit,
    ) -> Self {
        Self(histogram, attributes, std::time::Instant::now(), unit)
    }

    pub fn elapsed_micros(&self) -> u64 {
        self.2.elapsed().as_micros() as u64
    }
}

impl Drop for Stopwatch<'_> {
    fn drop(&mut self) {
        let elapsed = match self.3 {
            StopWatchUnit::Micros => self.2.elapsed().as_micros() as u64,
            StopWatchUnit::Millis => self.2.elapsed().as_millis() as u64,
            StopWatchUnit::Seconds => self.2.elapsed().as_secs(),
        };
        self.0.record(elapsed, self.1);
    }
}

pub fn get_current_trace_id() -> TraceId {
    let span = tracing::Span::current();
    span.context().span().span_context().trace_id()
}
