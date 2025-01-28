mod ac;
mod config;
#[allow(dead_code)]
mod executor;
mod frontend;
mod server;

use chroma_config::Configurable;
use config::FrontendConfig;
use frontend::Frontend;
use server::FrontendServer;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn frontend_service_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendConfig::load_from_path(&config_path),
        Err(_) => FrontendConfig::load(),
    };
    // TODO: Initialize tracing.
    let segment_api = Frontend::try_from_config(&config)
        .await
        .expect("Error creating SegmentApi from config");
    let server = FrontendServer::new(config.circuit_breaker, segment_api);
    FrontendServer::run(server).await;
}
