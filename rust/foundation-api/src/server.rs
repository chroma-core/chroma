use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use axum::{
    extract::{DefaultBodyLimit, State},
    response::IntoResponse,
    routing::get,
    Json, Router, ServiceExt,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_tracing::add_tracing_middleware;
use mdac::{Rule, Scorecard, ScorecardGuard};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
#[cfg(windows)]
use tokio::signal::windows::ctrl_c;
use tower_http::cors::CorsLayer;

use crate::{
    ac::AdmissionControlledService,
    auth::AuthenticateAndAuthorize,
    config::FoundationApiConfig,
    routes,
    server_middleware::{always_json_errors_middleware, default_json_content_type_middleware},
};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Too many requests; backoff and try again")]
struct RateLimitError;

impl ChromaError for RateLimitError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::ResourceExhausted
    }
}

#[derive(Clone)]
pub struct FoundationApiServer {
    pub(crate) config: FoundationApiConfig,
    pub(crate) auth: Arc<dyn AuthenticateAndAuthorize>,
    pub(crate) sysdb: SysDb,
    pub(crate) scorecard_enabled: Arc<AtomicBool>,
    pub(crate) scorecard: Arc<Scorecard<'static>>,
    pub(crate) system: System,
}

impl FoundationApiServer {
    pub fn new(
        config: FoundationApiConfig,
        auth: Arc<dyn AuthenticateAndAuthorize>,
        sysdb: SysDb,
        rules: Vec<Rule>,
        system: System,
    ) -> FoundationApiServer {
        // NOTE(rescrv): Assume statically no more than 128 threads because we
        // won't deploy on hardware with that many threads anytime soon for
        // frontends, if ever. Matches chroma-frontend's choice.
        let scorecard_enabled = Arc::new(AtomicBool::new(config.base.scorecard_enabled));
        // SAFETY(rescrv): This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), rules, 128.try_into().unwrap()));
        FoundationApiServer {
            config,
            auth,
            sysdb,
            scorecard_enabled,
            scorecard,
            system,
        }
    }

    /// Track this request against the scorecard rate limiter. Returns a guard
    /// that must be held for the duration of the request; drop it and the slot
    /// is released. Returns `RateLimitError` (429) if the limiter rejects.
    pub(crate) fn scorecard_request(
        &self,
        tags: &[&str],
    ) -> Result<ScorecardGuard, Box<dyn ChromaError>> {
        if self.scorecard_enabled.load(Ordering::Relaxed) {
            self.scorecard
                .track(tags)
                .map(|ticket| ScorecardGuard::new(Arc::clone(&self.scorecard), Some(ticket)))
                .ok_or_else(|| Box::new(RateLimitError) as _)
        } else {
            Ok(ScorecardGuard::new(Arc::clone(&self.scorecard), None))
        }
    }

    /// Accepts an optional `ready_tx` channel that emits the bound port when
    /// the server is ready.
    pub async fn run(self, ready_tx: Option<tokio::sync::oneshot::Sender<u16>>) {
        let system = self.system.clone();

        let FoundationApiConfig { base, .. } = self.config.clone();
        let port = base.port;
        let listen_address = base.listen_address.clone();
        let max_payload_size_bytes = base.max_payload_size_bytes;
        let circuit_breaker = base.circuit_breaker.clone();
        let cors_allow_origins = base.cors_allow_origins.clone();

        let app = Router::new()
            .route("/api/v2/healthcheck", get(healthcheck))
            .merge(routes::router())
            .with_state(self)
            .layer(DefaultBodyLimit::max(max_payload_size_bytes))
            .layer(axum::middleware::from_fn(
                default_json_content_type_middleware,
            ))
            .layer(axum::middleware::from_fn(always_json_errors_middleware));

        let mut app = add_tracing_middleware(app);

        if let Some(cors_allow_origins) = cors_allow_origins {
            let origins = cors_allow_origins
                .into_iter()
                .map(|origin| {
                    origin
                        .parse()
                        .unwrap_or_else(|_| panic!("Invalid origin: {}", origin))
                })
                .collect::<Vec<_>>();

            let mut cors_builder = CorsLayer::new()
                .allow_headers(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any);
            if origins.len() == 1 && origins[0] == "*" {
                cors_builder = cors_builder.allow_origin(tower_http::cors::Any);
            } else {
                cors_builder = cors_builder.allow_origin(origins);
            }

            app = app.layer(cors_builder);
        }

        let addr = format!("{}:{}", listen_address, port);
        tracing::info!(%addr, "Foundation API server listening on address");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_port = listener
            .local_addr()
            .expect("Failed to get local address of server")
            .port();
        if let Some(ready_tx) = ready_tx {
            ready_tx
                .send(bound_port)
                .expect("Failed to send bound port. Receiver has been dropped.");
        }
        if circuit_breaker.enabled() {
            let service = AdmissionControlledService::new(circuit_breaker, app);
            axum::serve(listener, service.into_make_service())
                .with_graceful_shutdown(graceful_shutdown(system))
                .await
                .unwrap();
        } else {
            axum::serve(listener, app)
                .with_graceful_shutdown(graceful_shutdown(system))
                .await
                .unwrap();
        }
    }
}

async fn healthcheck(State(_server): State<FoundationApiServer>) -> impl IntoResponse {
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({ "is_executor_ready": true })),
    )
}

async fn graceful_shutdown(system: System) {
    #[cfg(unix)]
    {
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(err) => {
                tracing::error!("Failed to create SIGTERM handler: {err}");
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(err) => {
                tracing::error!("Failed to create SIGINT handler: {err}");
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, shutting down service");
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT, shutting down service");
            }
        }
    }

    #[cfg(windows)]
    {
        match ctrl_c() {
            Ok(mut ctrl_c_signal) => {
                ctrl_c_signal.recv().await;
                tracing::info!("Received Ctrl+C, shutting down service");
            }
            Err(err) => {
                tracing::error!("Failed to create Ctrl+C handler: {err}");
                return;
            }
        }
    }

    system.stop().await;
    system.join().await;
}
