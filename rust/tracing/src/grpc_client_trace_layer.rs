use http::HeaderValue;
use opentelemetry::trace::TraceContextExt;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{body::BoxBody, transport::Error, Code};
use tower::{Layer, Service};
use tracing::{field::Empty, info_span, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACE_ID_HEADER_KEY: &str = "chroma-traceid";
const SPAN_ID_HEADER_KEY: &str = "chroma-spanid";

/// Propagates tracing information to gRPC requests and creates a span for each request.
#[derive(Clone)]
pub struct GrpcClientTraceLayer;

impl<S> Layer<S> for GrpcClientTraceLayer {
    type Service = GrpcClientTraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcClientTraceService { inner: service }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcClientTraceService<S> {
    inner: S,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for GrpcClientTraceService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>, Error = Error>
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
            rpc.method = ?req.uri().path(),
            rpc.headers = ?req.headers(),
            rpc.status_description = Empty,
            rpc.status_code_value = Empty,
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
                let span = Span::current();
                let res = fut.await;
                match res.as_ref() {
                    Ok(resp) => match resp.headers().get("grpc-status") {
                        Some(val) => match val
                            .to_str()
                            .map_err(|e| e.to_string())
                            .and_then(|s| s.parse::<u8>().map_err(|e| e.to_string()))
                        {
                            Ok(code) => {
                                let code_enum = Code::from_i32(code as i32);
                                span.record(
                                    "rpc.status_description",
                                    format!("[{:?}] {}", code_enum, code_enum),
                                );
                                span.record("rpc.status_code_value", code);
                            }
                            Err(err) => {
                                span.record(
                                    "rpc.status_description",
                                    format!("[StatusCodeParsingError] {err}"),
                                );
                                span.record("rpc.status_code_value", Code::InvalidArgument as u8);
                            }
                        },
                        None => {
                            span.record(
                                "rpc.status_description",
                                format!("[{:?}] {}", Code::Ok, Code::Ok),
                            );
                            span.record("rpc.status_code_value", Code::Ok as u8);
                        }
                    },
                    Err(err) => {
                        span.record(
                            "rpc.status_description",
                            format!("[HttpResponseError] {err}"),
                        );
                        span.record("rpc.status_code_value", Code::Internal as u8);
                    }
                }
                res
            }
            .instrument(span),
        )
    }
}
