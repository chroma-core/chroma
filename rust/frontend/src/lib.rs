use std::sync::Arc;

mod ac;
pub mod auth;
pub mod config;
#[allow(dead_code)]
pub mod executor;
pub mod frontend;
pub mod get_collection_with_segments_provider;
pub mod quota;
mod server;
mod tower_tracing;
mod types;

use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use chroma_tracing::{
    init_global_filter_layer, init_otel_layer, init_panic_tracing_hook, init_stdout_layer,
    init_tracing,
};
use frontend::Frontend;
use get_collection_with_segments_provider::*;
use mdac::{Pattern, Rule};
use quota::QuotaEnforcer;
use server::FrontendServer;

pub use config::{FrontendConfig, ScorecardRule};

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

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
) {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendConfig::load_from_path(&config_path),
        Err(_) => FrontendConfig::load(),
    };
    frontend_service_entrypoint_with_config(auth, quota_enforcer, config).await;
}

pub async fn frontend_service_entrypoint_with_config(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    config: FrontendConfig,
) {
    let tracing_layers = vec![
        init_global_filter_layer(),
        init_otel_layer(&config.service_name, &config.otel_endpoint),
        init_stdout_layer(&config.service_name),
    ];
    init_tracing(tracing_layers);
    init_panic_tracing_hook();

    let system = System::new();
    let registry = Registry::new();

    let frontend = Frontend::try_from_config(&(config.clone(), system), &registry)
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
    let server = FrontendServer::new(config, frontend, rules, auth, quota_enforcer);
    FrontendServer::run(server).await;
}
