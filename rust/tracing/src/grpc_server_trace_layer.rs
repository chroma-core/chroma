use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use tracing::instrument::Instrumented;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

#[derive(Clone)]
pub struct GrpcServerTraceLayer;
impl<S> tower::Layer<S> for GrpcServerTraceLayer {
    type Service = GrpcServerTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcServerTraceService { inner }
    }
}

#[derive(Clone)]
pub struct GrpcServerTraceService<S> {
    inner: S,
}

impl<S, ReqBody> tower::Service<http::Request<ReqBody>> for GrpcServerTraceService<S>
where
    S: tower::Service<http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let span = tracing::trace_span!(
            "gRPC request",
            otel.name = format!("Request {}", req.uri().path())
        );

        let trace_id = req
            .headers()
            .get(TRACE_ID_HEADER_KEY)
            .and_then(|h| h.to_str().ok())
            .and_then(|id| TraceId::from_hex(id).ok());

        let span_id = req
            .headers()
            .get(SPAN_ID_HEADER_KEY)
            .and_then(|h| h.to_str().ok())
            .and_then(|id| SpanId::from_hex(id).ok());

        if let Some(trace_id) = trace_id {
            if let Some(span_id) = span_id {
                let span_context = SpanContext::new(
                    trace_id,
                    span_id,
                    TraceFlags::new(1),
                    true,
                    TraceState::default(),
                );
                let context = span
                    .context()
                    .with_remote_span_context(span_context)
                    .clone();
                span.set_parent(context);
            }
        }

        let fut = self.inner.call(req);
        fut.instrument(span)
    }
}
