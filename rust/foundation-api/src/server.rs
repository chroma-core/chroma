use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use axum::{extract::DefaultBodyLimit, Router, ServiceExt};
use chroma_api_types::HeartbeatResponse;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_tracing::add_tracing_middleware;
use chroma_types::HealthCheckResponse;
use frontend_core::routes::{system_router, SystemMetrics, SystemState};
use mdac::{Rule, Scorecard, ScorecardGuard};
use opentelemetry::global;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
#[cfg(windows)]
use tokio::signal::windows::ctrl_c;
use tower_http::cors::CorsLayer;

use crate::{
    ac::AdmissionControlledService,
    auth::AuthenticateAndAuthorize,
    config::FoundationApiConfig,
    errors::ServerError,
    routes,
    server_middleware::{always_json_errors_middleware, default_json_content_type_middleware},
};

/// Placeholder batch limit reported by `/api/v2/pre-flight-checks`.
/// foundation-api has no log backend to source a real value from, and these
/// routes don't ingest records, so this is a stub.
const STUB_MAX_BATCH_SIZE: u32 = 100;

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
    pub(crate) metrics: Arc<SystemMetrics>,
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
        let metrics = Arc::new(SystemMetrics::new(&global::meter("foundation-api")));
        FoundationApiServer {
            config,
            auth,
            sysdb,
            scorecard_enabled,
            scorecard,
            system,
            metrics,
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
            .merge(system_router::<FoundationApiServer>())
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

/// Stubbed `SystemState` so foundation-api can mount the shared `system_router`
/// from `frontend-core`. foundation-api has no executor or log client, so
/// readiness and batch size are placeholders rather than real backend probes.
#[async_trait]
impl SystemState for FoundationApiServer {
    async fn healthcheck(&self) -> HealthCheckResponse {
        // No executor or log client to probe; report ready so liveness and
        // readiness behave like the previous hardcoded healthcheck (always 200).
        HealthCheckResponse {
            is_executor_ready: true,
            is_log_client_ready: true,
        }
    }

    async fn heartbeat(&self) -> Result<HeartbeatResponse, ServerError> {
        Ok(HeartbeatResponse {
            nanosecond_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        })
    }

    fn max_batch_size(&self) -> u32 {
        STUB_MAX_BATCH_SIZE
    }

    fn version(&self) -> String {
        "1.0.0".to_string()
    }

    fn auth(&self) -> &dyn AuthenticateAndAuthorize {
        self.auth.as_ref()
    }

    fn system_metrics(&self) -> &SystemMetrics {
        self.metrics.as_ref()
    }
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
