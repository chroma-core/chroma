use std::{
    panic::AssertUnwindSafe,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use hyper::body::Body;
use tonic::body::BoxBody;
use tower::{Layer, Service};

#[derive(Debug, Clone, Default)]
pub struct CatchPanicLayer;

impl<S> Layer<S> for CatchPanicLayer {
    type Service = CatchPanicMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        CatchPanicMiddleware { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct CatchPanicMiddleware<S> {
    inner: S,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S> Service<hyper::Request<Body>> for CatchPanicMiddleware<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary.
        // (We could also probably avoid a clone using an approach similar to https://docs.rs/tower-http/latest/tower_http/catch_panic, but wrangling the types between hyper/tower/tonic is very tricky.)
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // See https://doc.rust-lang.org/core/panic/trait.UnwindSafe.html for details on unwind safety.
            // tl;dr: it's not guaranteed to be safe to continue execution after a panic as the world may be in an inconsistent state.
            // Many types *are* unwind safe and marked as such with the UnwindSafe trait. In our case, since we want a generic wrapper around any service, we need to manually assert that the service is unwind safe.
            // Note that this can lead to unexpected behavior if the service is not actually unwind safe and it panics.
            match AssertUnwindSafe(inner.call(req)).catch_unwind().await {
                Ok(response) => response,
                Err(err) => {
                    let message = if let Some(s) = err.downcast_ref::<String>() {
                        format!("Service panicked: {}", s)
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        format!("Service panicked: {}", s)
                    } else {
                        "Service panicked but `CatchPanicMiddleware` was unable to downcast the panic info"
                            .to_string()
                    };
                    tracing::error!("{}", message);

                    let response = tonic::Status::internal(message).to_http();
                    Ok(response)
                }
            }
        })
    }
}
