use crate::utils::LOGO;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(
        index = 1,
        conflicts_with_all = &["path", "host", "port"],
        help = "The path to the Chroma config file"
    )]
    config_path: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The persistence path to your Chroma DB"
    )]
    path: Option<String>,

    #[clap(
        long,
        default_value = "localhost",
        conflicts_with = "config_path",
        help = "The host to listen to. Default: localhost"
    )]
    host: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The port to run the server on"
    )]
    port: Option<u16>,
}

pub fn run(args: RunArgs) {
    println!("{}", LOGO);

    let mut config = match &args.config_path {
        Some(config_path) => FrontendServerConfig::load_from_path(config_path),
        None => FrontendServerConfig::single_node_default(),
    };

    if let Some(path) = args.path {
        config.persist_path = Some(path);
    }

    if let Some(port) = args.port {
        config.port = port;
    }

    if let Some(host) = args.host {
        config.listen_address = host;
    }

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
    });
}
