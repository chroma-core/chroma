use chroma_frontend::{
    config::FrontendServerConfig, frontend_service_entrypoint_from_config, CONFIG_PATH_ENV_VAR,
};

#[tokio::main]
async fn main() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendServerConfig::load_from_path(&config_path),
        Err(_) => FrontendServerConfig::load(),
    };
    frontend_service_entrypoint_from_config(&config).await;
}
