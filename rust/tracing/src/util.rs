use opentelemetry::{
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
    KeyValue,
};
use tonic::metadata::MetadataMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

pub fn try_parse_tracecontext(metadata: &MetadataMap) -> (Option<TraceId>, Option<SpanId>) {
    let traceid = metadata
        .get(TRACE_ID_HEADER_KEY)
        .and_then(|v| v.to_str().ok())
        .and_then(|id| TraceId::from_hex(id).ok());

    let spanid = metadata
        .get(SPAN_ID_HEADER_KEY)
        .and_then(|v| v.to_str().ok())
        .and_then(|id| SpanId::from_hex(id).ok());

    (traceid, spanid)
}

pub fn wrap_span_with_parent_context(
    request_span: tracing::Span,
    metadata: &MetadataMap,
) -> tracing::Span {
    let (traceid, spanid) = try_parse_tracecontext(metadata);
    // Attach context passed by FE as parent.
    if let (Some(traceid), Some(spanid)) = (traceid, spanid) {
        let span_context = SpanContext::new(
            traceid,
            spanid,
            TraceFlags::new(1),
            true,
            TraceState::default(),
        );
        let context = request_span
            .context()
            .with_remote_span_context(span_context)
            .clone();
        request_span.set_parent(context);
    }
    request_span
}

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
