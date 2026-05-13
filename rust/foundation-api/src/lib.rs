//! Foundation API HTTP server.
//!
//! Per ADR "Foundation API: Long-Term Home", this crate hosts the foundation
//! HTTP route surface (`/api/foundation/{ask,recall,brief,init}`, future
//! sync-domain endpoints). It depends on `frontend-core` for Axum scaffolding,
//! middleware, the `AuthenticateAndAuthorize` trait, config primitives, error
//! types, and OTEL bootstrap. It does NOT depend on `chroma-frontend`, and it
//! does NOT serve any Chroma CRUD routes.

use std::sync::Arc;

pub mod config;
pub(crate) mod routes;
pub mod server;

pub use frontend_core::{ac, auth, errors, middleware as server_middleware, traced_json};

use config::FoundationApiConfig;
use server::FoundationApiServer;

pub const CONFIG_PATH_ENV_VAR: &str = "FOUNDATION_API_CONFIG_PATH";

pub async fn foundation_service_entrypoint(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    init_otel_tracing: bool,
) {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FoundationApiConfig::load_from_path(&config_path),
        Err(_) => FoundationApiConfig::default(),
    };
    foundation_service_entrypoint_with_config(auth, &config, init_otel_tracing).await;
}

pub fn init_foundation_otel_tracing(config: &FoundationApiConfig) {
    frontend_core::tracing::init_server_otel_tracing(
        config.base.open_telemetry.as_ref(),
        config.base.stdout_tracing,
    );
}

pub async fn foundation_service_entrypoint_with_config(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    config: &FoundationApiConfig,
    init_otel_tracing: bool,
) {
    let system = chroma_system::System::new();

    if init_otel_tracing {
        init_foundation_otel_tracing(config);
    }

    FoundationApiServer::new(config.clone(), auth, system)
        .run(None)
        .await;
}
