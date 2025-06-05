use opentelemetry::{
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
    KeyValue,
};
use tonic::metadata::MetadataMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

pub fn try_parse_tracecontext(metadata: &MetadataMap) -> (Option<TraceId>, Option<SpanId>) {
    let mut traceid: Option<TraceId> = None;
    let mut spanid: Option<SpanId> = None;
    if metadata.contains_key(TRACE_ID_HEADER_KEY) {
        let id_res = metadata.get(TRACE_ID_HEADER_KEY).unwrap().to_str();
        // Failure is not fatal.
        match id_res {
            Ok(id) => {
                let trace_id = TraceId::from_hex(id);
                match trace_id {
                    Ok(id) => traceid = Some(id),
                    Err(_) => traceid = None,
                }
            }
            Err(_) => traceid = None,
        }
    }
    if metadata.contains_key(SPAN_ID_HEADER_KEY) {
        let id_res = metadata.get(SPAN_ID_HEADER_KEY).unwrap().to_str();
        // Failure is not fatal.
        match id_res {
            Ok(id) => {
                let span_id = SpanId::from_hex(id);
                match span_id {
                    Ok(id) => spanid = Some(id),
                    Err(_) => spanid = None,
                }
            }
            Err(_) => spanid = None,
        }
    }
    (traceid, spanid)
}

pub fn wrap_span_with_parent_context(
    request_span: tracing::Span,
    metadata: &MetadataMap,
) -> tracing::Span {
    let (traceid, spanid) = try_parse_tracecontext(metadata);
    // Attach context passed by FE as parent.
    if traceid.is_some() && spanid.is_some() {
        let span_context = SpanContext::new(
            traceid.unwrap(),
            spanid.unwrap(),
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

pub struct Stopwatch<'a>(
    &'a opentelemetry::metrics::Histogram<u64>,
    &'a [KeyValue],
    std::time::Instant,
);

impl<'a> Stopwatch<'a> {
    pub fn new(
        histogram: &'a opentelemetry::metrics::Histogram<u64>,
        attributes: &'a [KeyValue],
    ) -> Self {
        Self(histogram, attributes, std::time::Instant::now())
    }

    pub fn elapsed_micros(&self) -> u64 {
        self.2.elapsed().as_micros() as u64
    }
}

impl Drop for Stopwatch<'_> {
    fn drop(&mut self) {
        let elapsed = self.2.elapsed().as_micros() as u64;
        self.0.record(elapsed, self.1);
    }
}
