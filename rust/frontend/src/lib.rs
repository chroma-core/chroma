mod ac;
mod config;
#[allow(dead_code)]
mod executor;
mod frontend;
mod get_collection_with_segments_provider;
mod server;
mod tower_tracing;
mod types;

use chroma_config::Configurable;
use chroma_system::System;
use frontend::Frontend;
use get_collection_with_segments_provider::*;
use server::FrontendServer;

pub use config::{FrontendConfig, ScorecardRule};

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

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
    let server = FrontendServer::new(config, frontend);
    FrontendServer::run(server).await;
}
