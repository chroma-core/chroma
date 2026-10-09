//! Shared HTTP routes that any frontend binary in this workspace can embed.
//!
//! These cover the generic "System" endpoints (heartbeat, healthcheck,
//! pre-flight checks, version) and the auth identity ("whoami") endpoint —
//! the handlers that are identical across binaries. A binary supplies backing
//! state implementing [`SystemState`] and merges [`system_router`] into its
//! own `axum::Router`.

use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chroma_api_types::{GetUserIdentityResponse, HeartbeatResponse};
use chroma_types::{ChecklistResponse, HealthCheckResponse};
use opentelemetry::metrics::{Counter, Meter};

use crate::auth::AuthenticateAndAuthorize;
use crate::errors::{ErrorResponse, ServerError};

/// Per-route request counters for the shared system/auth endpoints. Held by
/// the binary's state and surfaced through [`SystemState::system_metrics`].
pub struct SystemMetrics {
    pub healthcheck: Counter<u64>,
    pub heartbeat: Counter<u64>,
    pub pre_flight_checks: Counter<u64>,
    pub version: Counter<u64>,
    pub get_user_identity: Counter<u64>,
}

impl SystemMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            healthcheck: meter.u64_counter("healthcheck").build(),
            heartbeat: meter.u64_counter("heartbeat").build(),
            pre_flight_checks: meter.u64_counter("pre_flight_checks").build(),
            version: meter.u64_counter("version").build(),
            get_user_identity: meter.u64_counter("get_user_identity").build(),
        }
    }
}

/// State a binary must provide to mount [`system_router`]. The accessors let
/// the shared handlers delegate into each binary's own backend without
/// `frontend-core` naming any product-specific types.
#[async_trait]
pub trait SystemState: Clone + Send + Sync + 'static {
    async fn healthcheck(&self) -> HealthCheckResponse;
    async fn heartbeat(&self) -> Result<HeartbeatResponse, ServerError>;
    fn max_batch_size(&self) -> u32;
    fn version(&self) -> String;
    fn auth(&self) -> &dyn AuthenticateAndAuthorize;
    fn system_metrics(&self) -> &SystemMetrics;
}

/// Health check endpoint that returns 200 if the server and executor are ready
/// Healthcheck
/// Returns the health status of the service.
#[utoipa::path(
    get,
    path = "/api/v2/healthcheck",
    summary = "Healthcheck",
    description = "Returns the health status of the service.",
    tag = "System",
    responses(
        (status = 200, description = "Success", body = String, content_type = "application/json"),
        (status = 503, description = "Service Unavailable", body = ErrorResponse),
    ),
    extensions(
        ("x-codeSamples" = json!([]))
    )
)]
pub async fn healthcheck<S: SystemState>(State(server): State<S>) -> impl IntoResponse {
    server.system_metrics().healthcheck.add(1, &[]);
    let res = server.healthcheck().await;
    let code = match res.get_status_code() {
        tonic::Code::Ok => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, Json(res))
}

/// Heartbeat
/// Returns a nanosecond timestamp of the current time.
#[utoipa::path(
    get,
    path = "/api/v2/heartbeat",
    summary = "Heartbeat",
    description = "Returns a nanosecond timestamp of the current time.",
    tag = "System",
    responses(
        (status = 200, description = "Success", body = HeartbeatResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    extensions(
        ("x-codeSamples" = json!([
            {
                "lang": "typescript",
                "label": "Heartbeat",
                "source": "const timestamp = await client.heartbeat();"
            },
            {
                "lang": "python",
                "label": "Heartbeat",
                "source": "timestamp = client.heartbeat()"
            },
            {
                "lang": "rust",
                "label": "Heartbeat",
                "source": "let timestamp = client.heartbeat().await?;"
            }
        ]))
    )
)]
pub async fn heartbeat<S: SystemState>(
    State(server): State<S>,
) -> Result<Json<HeartbeatResponse>, ServerError> {
    server.system_metrics().heartbeat.add(1, &[]);
    Ok(Json(server.heartbeat().await?))
}

/// Pre-flight checks
/// Returns basic readiness information.
#[utoipa::path(
    get,
    path = "/api/v2/pre-flight-checks",
    summary = "Pre-flight checks",
    description = "Returns basic readiness information.",
    tag = "System",
    responses(
        (status = 200, description = "Pre flight checks", body = ChecklistResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn pre_flight_checks<S: SystemState>(
    State(server): State<S>,
) -> Result<Json<ChecklistResponse>, ServerError> {
    server.system_metrics().pre_flight_checks.add(1, &[]);
    Ok(Json(ChecklistResponse {
        max_batch_size: server.max_batch_size(),
        supports_base64_encoding: true,
    }))
}

/// Get version
/// Returns the version of the server.
#[utoipa::path(
    get,
    path = "/api/v2/version",
    summary = "Get version",
    description = "Returns the version of the server.",
    tag = "System",
    responses(
        (status = 200, description = "Get server version", body = String)
    ),
    extensions(
        ("x-codeSamples" = json!([
            {
                "lang": "typescript",
                "label": "Get version",
                "source": "const version = await client.version();"
            },
            {
                "lang": "python",
                "label": "Get version",
                "source": "version = client.get_version()"
            }
        ]))
    )
)]
pub async fn version<S: SystemState>(State(server): State<S>) -> Json<String> {
    server.system_metrics().version.add(1, &[]);
    Json(server.version())
}

/// Get user identity
/// Returns the current user's identity, tenant, and databases.
#[utoipa::path(
    get,
    path = "/api/v2/auth/identity",
    summary = "Get user identity",
    description = "Returns the current user's identity, tenant, and databases.",
    tag = "Authentication",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "User identity", body = GetUserIdentityResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn get_user_identity<S: SystemState>(
    headers: HeaderMap,
    State(server): State<S>,
) -> Result<Json<GetUserIdentityResponse>, ServerError> {
    server.system_metrics().get_user_identity.add(1, &[]);
    Ok(Json(server.auth().get_user_identity(&headers).await?))
}

/// Router for the shared system and auth-identity endpoints. Merge this into a
/// binary's `axum::Router` whose state implements [`SystemState`].
pub fn system_router<S: SystemState>() -> Router<S> {
    Router::new()
        .route("/api/v2", get(heartbeat::<S>))
        .route("/api/v2/healthcheck", get(healthcheck::<S>))
        .route("/api/v2/heartbeat", get(heartbeat::<S>))
        .route("/api/v2/pre-flight-checks", get(pre_flight_checks::<S>))
        .route("/api/v2/version", get(version::<S>))
        .route("/api/v2/auth/identity", get(get_user_identity::<S>))
}
