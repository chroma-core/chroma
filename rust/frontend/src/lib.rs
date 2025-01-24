mod api;
mod config;
mod server;
use api::SegmentApi;
use chroma_config::Configurable;
use config::FrontEndConfig;
use server::FrontendServer;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn frontend_service_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontEndConfig::load_from_path(&config_path),
        Err(_) => FrontEndConfig::load(),
    };
    // TODO: Initialize tracing.
    let segment_api = SegmentApi::try_from_config(&config)
        .await
        .expect("Error creating SegmentApi from config");
    let server = FrontendServer::new(segment_api);
    FrontendServer::run(server).await;
}
