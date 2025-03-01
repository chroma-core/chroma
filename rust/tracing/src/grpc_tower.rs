use http::HeaderValue;
use opentelemetry::trace::TraceContextExt;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{body::BoxBody, Code, Status};
use tower::{Layer, Service};
use tracing::{field::Empty, info_span, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

/// Propagates tracing information to gRPC requests and creates a span for each request.
#[derive(Clone)]
pub struct GrpcTraceLayer;

impl<S> Layer<S> for GrpcTraceLayer {
    type Service = GrpcTraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcTraceService { inner: service }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcTraceService<S> {
    inner: S,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for GrpcTraceService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>, Error = Status>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    // The `Future` we return is just a pinned, boxed future, but instrumented with a `Span`.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let span = info_span!(
            "grpc_request",
            otel.name = format!("Request {}", req.uri().path()),
            grpc.method = ?req.uri().path(),
            grpc.headers = ?req.headers(),
            grpc.status_code_name = Empty,
            grpc.status_code_value = Empty,
        );

        if let Ok(header) =
            HeaderValue::from_str(&span.context().span().span_context().trace_id().to_string())
        {
            req.headers_mut().insert(TRACE_ID_HEADER_KEY, header);
        }

        if let Ok(header) =
            HeaderValue::from_str(&span.context().span().span_context().span_id().to_string())
        {
            req.headers_mut().insert(SPAN_ID_HEADER_KEY, header);
        }

        let fut = self.inner.call(req);
        Box::pin(
            async move {
                let res = fut.await;
                let span = Span::current();
                if let Err(status) = res.as_ref() {
                    span.record("status_code_description", status.code().description());
                    span.record("status_code_value", status.code() as u8);
                } else {
                    span.record("status_code_description", Code::Ok.description());
                    span.record("status_code_value", Code::Ok as u8);
                }
                res
            }
            .instrument(span),
        )
    }
}
