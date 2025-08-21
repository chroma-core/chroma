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
