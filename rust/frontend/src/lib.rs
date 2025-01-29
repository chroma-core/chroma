mod ac;
mod config;
mod errors;
#[allow(dead_code)]
mod executor;
mod frontend;
mod server;
mod tower_tracing;

use chroma_config::Configurable;
use chroma_system::System;
use config::FrontendConfig;
use frontend::Frontend;
use server::FrontendServer;

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
    let server = FrontendServer::new(config.circuit_breaker, frontend);
    FrontendServer::run(server).await;
}
