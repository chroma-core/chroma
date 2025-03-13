use crate::utils::LOGO;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(name = "config", default_value = None)]
    config: Option<String>,
}

pub fn run(args: RunArgs) {
    println!("{}", LOGO);

    let config = match &args.config {
        Some(path) => FrontendServerConfig::load_from_path(path),
        None => FrontendServerConfig::single_node_default(),
    };

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
    });
}
