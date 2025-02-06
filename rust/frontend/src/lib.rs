mod ac;
pub mod config;
#[allow(dead_code)]
pub mod executor;
pub mod frontend;
pub mod get_collection_with_segments_provider;
mod server;
mod tower_tracing;
mod types;
mod utils;

use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_system::System;
use frontend::Frontend;
use get_collection_with_segments_provider::*;
use mdac::{Pattern, Rule};
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

pub async fn frontend_service_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendConfig::load_from_path(&config_path),
        Err(_) => FrontendConfig::load(),
    };
    chroma_tracing::init_otel_tracing(&config.service_name, &config.otel_endpoint);
    let system = System::new();
    // TODO: Initialize tracing.
    let frontend = Frontend::try_from_config(&(config.clone(), system))
        .await
        .expect("Error creating SegmentApi from config");
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
    let server = FrontendServer::new(config, frontend, rules);
    FrontendServer::run(server).await;
}
