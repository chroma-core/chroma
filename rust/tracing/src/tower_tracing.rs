use axum::extract::MatchedPath;
use axum::http::{header, Request, Response};
use axum::Router;
use futures::future::BoxFuture;
use std::time::Duration;
use tower::Service;
use tower_http::trace::{MakeSpan, OnResponse, TraceLayer};

use crate::util::get_current_trace_id;

#[derive(Clone)]
struct RequestTracing;
impl<B> MakeSpan<B> for RequestTracing {
    fn make_span(&mut self, request: &Request<B>) -> tracing::Span {
        let http_route = request
            .extensions()
            .get::<MatchedPath>()
            .map_or_else(|| "(unknown route)", |mp| mp.as_str());

        let host = request
            .headers()
            .get(header::HOST)
            .map_or("", |h| h.to_str().unwrap_or(""));

        let user_agent = request
            .headers()
            .get(header::USER_AGENT)
            .map_or("", |h| h.to_str().unwrap_or(""));

        let name = format!("{} {}", request.method(), http_route);

        tracing::span!(
            tracing::Level::DEBUG,
            "HTTP request",
            http.method = %request.method(),
            http.uri = %request.uri(),
            http.route = http_route,
            http.version = ?request.version(),
            http.host = %host,
            http.status_code = tracing::field::Empty,
            http.user_agent = %user_agent,
            otel.name = name,
            otel.status_code = tracing::field::Empty,
        )
    }
}

impl<B> OnResponse<B> for RequestTracing {
    fn on_response(self, response: &Response<B>, _latency: Duration, span: &tracing::Span) {
        span.record("http.status_code", response.status().as_u16());
        if response.status().is_client_error() || response.status().is_server_error() {
            span.record("otel.status_code", "ERROR");
        }
    }
}

#[derive(Clone)]
pub struct TraceIdMiddleware<S> {
    inner: S,
}

impl<S, Request, Rs> Service<Request> for TraceIdMiddleware<S>
where
    S: Service<Request, Response = Response<Rs>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let future = self.inner.call(req);
        Box::pin(async move {
            let mut response: Response<Rs> = future.await?;
            if response.status().is_client_error() || response.status().is_server_error() {
                let trace_id = get_current_trace_id().to_string();
                let headers = response.headers_mut();
                let header_val = trace_id.parse::<header::HeaderValue>();
                if let Ok(val) = header_val {
                    headers.insert("chroma-trace-id", val);
                }
            }
            Ok(response)
        })
    }
}

#[derive(Debug, Clone)]
pub struct SetTraceIdLayer {}

impl SetTraceIdLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<S> tower::layer::Layer<S> for SetTraceIdLayer {
    type Service = TraceIdMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceIdMiddleware { inner }
    }
}

pub fn add_tracing_middleware(router: Router) -> Router {
    router.layer(SetTraceIdLayer::new()).layer(
        TraceLayer::new_for_http()
            .make_span_with(RequestTracing)
            .on_response(RequestTracing),
    )
}
