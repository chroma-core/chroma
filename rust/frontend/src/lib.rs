use std::{path::Path, sync::Arc};

pub mod config;
#[allow(dead_code)]
pub mod executor;
pub mod get_collection_with_segments_provider;
pub mod impls;
pub mod quota;
pub mod server;
mod types;

// Re-export scaffolding from frontend-core so that internal modules can keep
// referring to `crate::ac::*`, `crate::auth::*`, `crate::traced_json::*`, and
// `crate::server_middleware::*`. External consumers also benefit from the
// re-exports for `ac`, `auth`, and `traced_json`.
pub use frontend_core::middleware as server_middleware;
pub use frontend_core::{ac, auth, traced_json};

use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use config::FrontendServerConfig;
use get_collection_with_segments_provider::*;
use mdac::{Pattern, Rule};
use quota::QuotaEnforcer;
use server::FrontendServer;

pub use config::{FrontendConfig, ScorecardRule};
pub use impls::Frontend;

pub const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

#[derive(thiserror::Error, Debug)]
pub enum ScorecardRuleError {
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
}

impl ChromaError for ScorecardRuleError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ScorecardRuleError::InvalidPattern(_) => chroma_error::ErrorCodes::InvalidArgument,
        }
    }
}

pub async fn frontend_service_entrypoint(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    init_otel_tracing: bool,
) {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendServerConfig::load_from_path(&config_path),
        Err(_) => FrontendServerConfig::load(),
    };
    frontend_service_entrypoint_with_config(auth, quota_enforcer, &config, init_otel_tracing).await;
}

pub async fn frontend_service_entrypoint_with_config_system_registry(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    system: System,
    registry: Registry,
    config: &FrontendServerConfig,
) {
    let mut fe_cfg = config.frontend.clone();
    if let (Some(sql_cfg), Some(local_segman_cfg)) =
        (fe_cfg.sqlitedb.as_mut(), fe_cfg.segment_manager.as_mut())
    {
        let persist_root = Path::new(&config.persist_path);
        let sqlite_url = persist_root.join(&config.sqlite_filename);
        local_segman_cfg.persist_path.get_or_insert(
            persist_root
                .to_str()
                .expect("Persist path should be valid")
                .to_string(),
        );
        sql_cfg.url.get_or_insert(
            sqlite_url
                .to_str()
                .expect("Sqlite path should be valid")
                .to_string(),
        );
    }

    let frontend = Frontend::try_from_config(&(fe_cfg, system.clone()), &registry)
        .await
        .expect("Error creating Frontend Config");
    fn rule_to_rule(rule: &ScorecardRule) -> Result<Rule, ScorecardRuleError> {
        let patterns = rule
            .patterns
            .iter()
            .map(|p| Pattern::new(p).ok_or_else(|| ScorecardRuleError::InvalidPattern(p.clone())))
            .collect::<Result<Vec<_>, ScorecardRuleError>>()?;
        Ok(Rule {
            patterns,
            limit: rule.score as usize,
        })
    }
    let rules = config
        .scorecard
        .iter()
        .map(rule_to_rule)
        .collect::<Result<Vec<_>, ScorecardRuleError>>()
        .expect("error creating scorecard");
    FrontendServer::new(
        config.clone(),
        frontend,
        rules,
        auth,
        quota_enforcer,
        system,
    )
    .run(None)
    .await;
}

pub fn init_frontend_otel_tracing(config: &FrontendServerConfig) {
    frontend_core::tracing::init_server_otel_tracing(
        config.open_telemetry.as_ref(),
        config.stdout_tracing,
    );
}

pub async fn frontend_service_entrypoint_with_config(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    config: &FrontendServerConfig,
    init_otel_tracing: bool,
) {
    let system = System::new();
    let registry = Registry::new();

    if init_otel_tracing {
        init_frontend_otel_tracing(config);
    }

    frontend_service_entrypoint_with_config_system_registry(
        auth,
        quota_enforcer,
        system,
        registry,
        config,
    )
    .await;
}
