use axum::extract::MatchedPath;
use axum::http::{header, HeaderMap, Request, Response};
use axum::Router;
use futures::future::BoxFuture;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{global, Context};
use std::time::Duration;
use tower::Service;
use tower_http::trace::{MakeSpan, OnResponse, TraceLayer};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::util::get_current_trace_id;

#[derive(Clone)]
struct RequestTracing;

struct RequestHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for RequestHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

fn extract_trace_context(headers: &HeaderMap) -> Context {
    global::get_text_map_propagator(|propagator| {
        propagator.extract(&RequestHeaderExtractor(headers))
    })
}

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

        let span = tracing::span!(
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
        );

        let parent_context = extract_trace_context(request.headers());
        if parent_context.span().span_context().is_valid() {
            span.set_parent(parent_context);
        }

        span
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

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use axum::http::{HeaderMap, HeaderValue, Request};
    use opentelemetry::trace::{SpanId, TraceContextExt, TraceFlags, TraceId, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tower_http::trace::MakeSpan;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{extract_trace_context, RequestTracing};

    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn install_trace_context_propagator() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        });
    }

    #[test]
    fn extracts_w3c_trace_context_from_headers() {
        install_trace_context_propagator();
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", HeaderValue::from_static(TRACEPARENT));
        headers.insert("tracestate", HeaderValue::from_static("vendor=value"));

        let context = extract_trace_context(&headers);
        let span_context = context.span().span_context().clone();

        assert!(span_context.is_valid());
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id(),
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
        );
        assert_eq!(
            span_context.span_id(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap()
        );
        assert_eq!(span_context.trace_flags(), TraceFlags::SAMPLED);
        assert_eq!(span_context.trace_state().header(), "vendor=value");
    }

    #[test]
    fn uses_extracted_context_as_request_span_parent() {
        install_trace_context_propagator();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder().build();
        let tracer = provider.tracer("request-parent-test");
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::OpenTelemetryLayer::new(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let request = Request::builder()
                .uri("/api/v2/heartbeat")
                .header("traceparent", TRACEPARENT)
                .body(())
                .unwrap();
            let span = RequestTracing.make_span(&request);
            let span_context = span.context().span().span_context().clone();

            assert_eq!(
                span_context.trace_id(),
                TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
            );
            assert_ne!(
                span_context.span_id(),
                SpanId::from_hex("00f067aa0ba902b7").unwrap()
            );
        });
    }

    #[test]
    fn ignores_missing_or_invalid_trace_context() {
        install_trace_context_propagator();

        for traceparent in [
            None,
            Some("not-a-traceparent"),
            Some("00-00000000000000000000000000000000-0000000000000000-01"),
        ] {
            let mut headers = HeaderMap::new();
            if let Some(traceparent) = traceparent {
                headers.insert("traceparent", HeaderValue::from_str(traceparent).unwrap());
            }

            let context = extract_trace_context(&headers);
            assert!(!context.span().span_context().is_valid());
        }
    }
}
