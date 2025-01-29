use axum::extract::MatchedPath;
use axum::http::{header, Request, Response};
use axum::Router;
use std::time::Duration;
use tower_http::trace::{MakeSpan, OnResponse, TraceLayer};

#[derive(Clone)]
struct RequestTracing;
impl<B> MakeSpan<B> for RequestTracing {
    fn make_span(&mut self, request: &Request<B>) -> tracing::Span {
        let http_route = request
            .extensions()
            .get::<MatchedPath>()
            .map_or_else(|| "", |mp| mp.as_str())
            .to_owned();

        let host = request
            .headers()
            .get(header::HOST)
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

pub(crate) fn add_tracing_middleware(router: Router) -> Router {
    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(RequestTracing)
            .on_response(RequestTracing),
    )
}
