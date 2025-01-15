use std::str::FromStr;

use opentelemetry::trace::TraceContextExt;
use tonic::{metadata::MetadataValue, Request, Status};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

pub(crate) fn client_interceptor(request: Request<()>) -> Result<Request<()>, Status> {
    // If span is disabled then nothing to append in the header.
    if Span::current().is_disabled() {
        return Ok(request);
    }
    let mut mut_request = request;
    let metadata = mut_request.metadata_mut();
    let trace_id = MetadataValue::from_str(
        Span::current()
            .context()
            .span()
            .span_context()
            .trace_id()
            .to_string()
            .as_str(),
    );
    let span_id = MetadataValue::from_str(
        Span::current()
            .context()
            .span()
            .span_context()
            .span_id()
            .to_string()
            .as_str(),
    );
    // Errors are not fatal.
    if let Ok(id) = trace_id {
        metadata.append(TRACE_ID_HEADER_KEY, id);
    }
    if let Ok(id) = span_id {
        metadata.append(SPAN_ID_HEADER_KEY, id);
    }
    Ok(mut_request)
}
