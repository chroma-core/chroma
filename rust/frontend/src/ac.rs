use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use mdac::{CircuitBreaker, CircuitBreakerConfig};
use tower::Service;

#[derive(Clone)]
pub struct AdmissionControlledService<S: Service<Request<Body>> + Send>
where
    S::Future: Send,
{
    circuit_breaker: Arc<CircuitBreaker<'static>>,
    service: S,
}

impl<S: Service<Request<Body>> + Send> AdmissionControlledService<S>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    pub fn new(config: CircuitBreakerConfig, s: S) -> Self {
        let circuit_breaker = Arc::new(CircuitBreaker::new(&(), config));
        Self {
            circuit_breaker,
            service: s,
        }
    }
}

impl<S: Service<Request<Body>> + Send> tower::Service<Request<Body>>
    for AdmissionControlledService<S>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = AdmissionControlledFuture<S>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let circuit_breaker = Arc::clone(&self.circuit_breaker);
        let ticket = self.circuit_breaker.admit_one();
        AdmissionControlledFuture {
            inner: self.service.call(req),
            circuit_breaker,
            ticket,
        }
    }
}

pub struct AdmissionControlledFuture<S: Service<Request<Body>>>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    inner: S::Future,
    circuit_breaker: Arc<CircuitBreaker<'static>>,
    ticket: bool,
}

impl<S: Service<Request<Body>>> Future for AdmissionControlledFuture<S>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.ticket {
            Poll::Ready(Ok(S::Response::from(
                (StatusCode::TOO_MANY_REQUESTS, "You have been rate limited.").into_response(),
            )))
        } else {
            // SAFETY(rescrv):  The `inner` future is pinned, so we can safely call `poll` on it.
            let res = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) }.poll(cx);
            if res.is_ready() {
                self.circuit_breaker.release_one();
            }
            res
        }
    }
}
