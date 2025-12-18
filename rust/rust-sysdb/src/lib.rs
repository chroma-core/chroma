use chroma_config::Configurable;

use crate::config::RootConfig;

pub mod config;
pub mod server;
pub mod spanner;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn sysdb_service_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => RootConfig::load_from_path(&config_path),
        Err(_) => RootConfig::load(),
    };
    let config = config.sysdb_service;
    let registry = chroma_config::registry::Registry::new();
    chroma_tracing::init_otel_tracing(
        &config.service_name,
        &config.otel_filters,
        &config.otel_endpoint,
    );
    let sysdb_server = match server::SysdbService::try_from_config(&config, &registry).await {
        Ok(sysdb_server) => sysdb_server,
        Err(err) => {
            tracing::error!("Failed to create sysdb server component: {:?}", err);
            return;
        }
    };

    // Server task will run until it receives a shutdown signal
    let _ = tokio::spawn(async move {
        let _ = crate::server::SysdbService::run(sysdb_server).await;
    })
    .await;
}
