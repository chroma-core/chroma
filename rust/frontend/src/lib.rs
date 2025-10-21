use std::{path::Path, sync::Arc};

mod ac;
pub mod auth;
pub mod config;
#[allow(dead_code)]
pub mod executor;
pub mod get_collection_with_segments_provider;
pub mod impls;
pub mod quota;
pub mod server;
mod server_middleware;
mod traced_json;
mod types;

use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use chroma_tracing::{
    init_global_filter_layer, init_otel_layer, init_panic_tracing_hook, init_stdout_layer,
    init_tracing,
};
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
    if let Some(config) = &config.open_telemetry {
        let tracing_layers = vec![
            init_global_filter_layer(&config.filters),
            init_otel_layer(&config.service_name, &config.endpoint),
            init_stdout_layer(),
        ];
        init_tracing(tracing_layers);
        init_panic_tracing_hook();
    } else {
        eprintln!("OpenTelemetry is not enabled because it is missing from the config.");
    }
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
