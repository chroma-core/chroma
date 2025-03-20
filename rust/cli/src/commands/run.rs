use crate::utils::{CliError, LOGO};
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::net::TcpListener;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Config file {0} does not exists")]
    ConfigFileNotFound(String),
    #[error("Address {0}:{1} is not available")]
    AddressUnavailable(String, u16),
    #[error("Failed to start a Chroma server")]
    ServerStartFailed,
}

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
        conflicts_with = "path",
        help = "Run the server in ephemeral mode (no persistence)",
        hide = true,
        default_value_t = false
    )]
    ephemeral: bool,

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

fn validate_host(address: &String, port: u16) -> bool {
    let socket = format!("{}:{}", address, port);
    TcpListener::bind(&socket).is_ok()
}

fn override_default_config_with_args(args: RunArgs) -> Result<FrontendServerConfig, CliError> {
    let mut config = FrontendServerConfig::single_node_default();

    if args.ephemeral {
        config.persist_path = None;
    } else {
        config.persist_path = Some(args.path.clone().unwrap_or("./chroma".to_string()));
    }

    if let Some(port) = args.port {
        config.port = port;
    }

    if let Some(host) = args.host {
        config.listen_address = host;
    }

    if !validate_host(&config.listen_address, config.port) {
        return Err(RunError::AddressUnavailable(config.listen_address, config.port).into());
    }

    Ok(config)
}

fn display_run_message(config: &FrontendServerConfig) {
    println!("{}", LOGO);

    if let Some(persist_path) = &config.persist_path {
        println!("Persisting data to: {}", persist_path.bold());
    } else {
        println!(
            "Running in ephemeral mode ({})",
            "no persistence".red().bold()
        );
    }

    println!(
        "Connect to Chroma at: {}",
        format!("http://localhost:{}", config.port)
            .underline()
            .blue()
    );
    println!(
        "Getting started guide: {}",
        "https://docs.trychroma.com/docs/overview/getting-started\n"
            .underline()
            .blue()
    );
}

pub fn run(args: RunArgs) -> Result<(), CliError> {
    let config = match &args.config_path {
        Some(config_path) => {
            if !std::path::Path::new(config_path).exists() {
                return Err(RunError::ConfigFileNotFound(config_path.to_string()).into());
            }
            FrontendServerConfig::load_from_path(config_path)
        }
        None => override_default_config_with_args(args)?,
    };

    display_run_message(&config);

    let runtime = tokio::runtime::Runtime::new().map_err(|_| RunError::ServerStartFailed)?;
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
    });
    Ok(())
}
