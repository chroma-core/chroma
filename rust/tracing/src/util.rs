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

/// Creates a log event with the appropriate metadata to link it to another span.
///
/// Recommended usage:
///
/// ```rust
/// let new_root_span = link_event!(
///     Level::INFO,
///     tracing::info_span!(
///         parent: None,
///         "This span will not be included in the current trace tree"
///     ),
///     "Spawning task.."
/// );
///
/// async {
///     // ...
/// }
/// .instrument(new_root_span)
/// .await;
/// ```
#[macro_export]
macro_rules! link_event {
    ($lvl:expr, $span_to_link:expr, $($arg:tt)+) => {{
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        use opentelemetry::trace::TraceContextExt as _;

        let span_to_link = $span_to_link.clone();
        span_to_link.follows_from(Span::current());

        let ctx = span_to_link.context();
        let opentelemetry_span = ctx.span();
        let span_ctx = opentelemetry_span.span_context();

        tracing::event!(
            $lvl,
            meta.annotation_type = "link",
            trace.link.span_id   = ?span_ctx.span_id(),
            trace.link.trace_id  = ?span_ctx.trace_id(),
            $($arg)+
        );

        span_to_link
    }};
}
