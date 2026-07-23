//! Foundation API HTTP server.
//!
//! Per ADR "Foundation API: Long-Term Home", this crate hosts the foundation
//! HTTP route surface (`/api/{ask,recall,brief,init}`, future
//! sync-domain endpoints). It depends on `frontend-core` for Axum scaffolding,
//! middleware, the `AuthenticateAndAuthorize` trait, config primitives, error
//! types, and OTEL bootstrap. It does NOT depend on `chroma-frontend`, and it
//! does NOT serve any Chroma CRUD routes.

use async_trait::async_trait;
use std::sync::Arc;

pub(crate) mod agent_tools;
/// Idempotent database/collection creation against sysdb, exported so
/// hosted-chroma's sync service can create collections (notably `slack_raw`)
/// identically to `/init`.
pub mod collections;
pub mod config;
pub(crate) mod foundation_chroma;
pub(crate) mod routes;
pub mod server;
pub mod trajectories;
pub(crate) mod wiki;

pub use frontend_core::{ac, auth, errors, middleware as server_middleware, traced_json};

use chroma_metering::{meter_event_receiver_initialized, MeterEvent};
use chroma_system::{Component, ComponentContext, Handler};
use config::FoundationApiConfig;
use server::FoundationApiServer;

pub const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

#[derive(Debug, Default)]
struct LoggingMeterEventReceiver;

#[async_trait]
impl Component for LoggingMeterEventReceiver {
    fn get_name() -> &'static str {
        "foundation-api-meter-event-receiver"
    }

    fn queue_size(&self) -> usize {
        1024
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        MeterEvent::init_receiver(ctx.receiver());
    }
}

#[async_trait]
impl Handler<MeterEvent> for LoggingMeterEventReceiver {
    type Result = ();

    async fn handle(&mut self, message: MeterEvent, _ctx: &ComponentContext<Self>) -> Self::Result {
        tracing::warn!(
            event = ?message,
            "processed foundation-api meter event via fallback receiver; no billing ingestion receiver is installed"
        );
    }
}

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
    let registry = chroma_config::registry::Registry::new();

    foundation_service_entrypoint_with_config_system_registry(
        auth,
        config,
        init_otel_tracing,
        system,
        registry,
    )
    .await;
}

pub async fn foundation_service_entrypoint_with_config_system_registry(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    config: &FoundationApiConfig,
    init_otel_tracing: bool,
    system: chroma_system::System,
    registry: chroma_config::registry::Registry,
) {
    if init_otel_tracing {
        init_foundation_otel_tracing(config);
    }

    if !meter_event_receiver_initialized() {
        system.start_component(LoggingMeterEventReceiver);
        tracing::warn!(
            "Initialized fallback foundation-api meter-event receiver; search-agent usage events will be logged but not forwarded to billing ingestion"
        );
    }

    let sysdb = match <chroma_sysdb::SysDb as chroma_config::Configurable<(
        chroma_sysdb::SysDbConfig,
        Option<chroma_sysdb::GrpcSysDbConfig>,
    )>>::try_from_config(&(config.sysdb.clone(), None), &registry)
    .await
    {
        Ok(sysdb) => sysdb,
        Err(e) => {
            tracing::error!(
                error = %e,
                "foundation-api startup failed: could not construct SysDb client from config",
            );
            return;
        }
    };

    let rules = match build_scorecard_rules(&config.base.scorecard) {
        Ok(rules) => rules,
        Err(e) => {
            tracing::error!(
                error = %e,
                "foundation-api startup failed: invalid scorecard rule pattern",
            );
            return;
        }
    };

    FoundationApiServer::new(config.clone(), auth, sysdb, rules, system)
        .run(None)
        .await;
}

#[derive(thiserror::Error, Debug)]
pub enum ScorecardRuleError {
    #[error("Invalid scorecard pattern: {0}")]
    InvalidPattern(String),
}

fn build_scorecard_rules(
    config_rules: &[frontend_core::config::ScorecardRule],
) -> Result<Vec<mdac::Rule>, ScorecardRuleError> {
    config_rules
        .iter()
        .map(|rule| {
            let patterns = rule
                .patterns
                .iter()
                .map(|p| {
                    mdac::Pattern::new(p)
                        .ok_or_else(|| ScorecardRuleError::InvalidPattern(p.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(mdac::Rule {
                patterns,
                limit: rule.score as usize,
            })
        })
        .collect()
}
