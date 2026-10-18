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
        let inner = if ticket {
            Some(self.service.call(req))
        } else {
            None
        };
        AdmissionControlledFuture {
            inner,
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
    inner: Option<S::Future>,
    circuit_breaker: Arc<CircuitBreaker<'static>>,
    ticket: bool,
}

impl<S: Service<Request<Body>>> Future for AdmissionControlledFuture<S>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.ticket {
            Poll::Ready(Ok(S::Response::from(
                (StatusCode::TOO_MANY_REQUESTS, "You have been rate limited.").into_response(),
            )))
        } else {
            // SAFETY(rescrv): We do not move `inner` out of `this`.
            let this = unsafe { self.get_unchecked_mut() };
            let inner_fut = unsafe {
                Pin::new_unchecked(
                    this.inner
                        .as_mut()
                        .expect("inner future must be Some when ticket is true"),
                )
            };
            let res = inner_fut.poll(cx);
            if res.is_ready() {
                this.circuit_breaker.release_one();
                this.ticket = false;
            }
            res
        }
    }
}

impl<S: Service<Request<Body>>> Drop for AdmissionControlledFuture<S>
where
    S::Response: From<Response>,
    S::Future: Send,
{
    fn drop(&mut self) {
        if self.ticket {
            self.circuit_breaker.release_one();
            self.ticket = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct PendingMockService;

    impl Service<Request<Body>> for PendingMockService {
        type Response = Response;
        type Error = Infallible;
        type Future = std::future::Pending<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            std::future::pending()
        }
    }

    #[derive(Clone)]
    struct ImmediateMockService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Request<Body>> for ImmediateMockService {
        type Response = Response;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(StatusCode::OK.into_response()))
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_permit_released_on_drop() {
        let config = CircuitBreakerConfig { requests: 1 };
        let mut ac_service = AdmissionControlledService::new(config, PendingMockService);

        // First request acquires the single permit
        let req1 = Request::builder().body(Body::empty()).unwrap();
        let fut1 = ac_service.call(req1);

        // Second request should be rejected (429) since capacity is 1
        let req2 = Request::builder().body(Body::empty()).unwrap();
        let fut2 = ac_service.call(req2);
        let res2 = fut2.await.unwrap();
        assert_eq!(res2.status(), StatusCode::TOO_MANY_REQUESTS);

        // Client cancels or disconnects fut1
        drop(fut1);

        // After fut1 is dropped, third request MUST be admitted if permit was released
        let req3 = Request::builder().body(Body::empty()).unwrap();
        let fut3 = ac_service.call(req3);
        assert!(fut3.ticket, "Permit was leaked when in-flight request was dropped!");
    }

    #[tokio::test]
    async fn test_completed_request_releases_permit_once() {
        let config = CircuitBreakerConfig { requests: 1 };
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = ImmediateMockService { calls: calls.clone() };
        let mut ac_service = AdmissionControlledService::new(config, mock);

        let req1 = Request::builder().body(Body::empty()).unwrap();
        let fut1 = ac_service.call(req1);
        let res1 = fut1.await.unwrap();
        assert_eq!(res1.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Next request can acquire the released permit
        let req2 = Request::builder().body(Body::empty()).unwrap();
        let fut2 = ac_service.call(req2);
        assert!(fut2.ticket);
        let res2 = fut2.await.unwrap();
        assert_eq!(res2.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_rejected_request_does_not_call_downstream_service() {
        let config = CircuitBreakerConfig { requests: 0 }; // 0 permits admitted
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = ImmediateMockService { calls: calls.clone() };
        let mut ac_service = AdmissionControlledService::new(config, mock);

        let req = Request::builder().body(Body::empty()).unwrap();
        let fut = ac_service.call(req);
        assert!(!fut.ticket);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "Rejected request should not call downstream service"
        );

        let res = fut.await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

