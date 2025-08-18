use crate::ui_utils::LOGO;
use crate::utils::CliError;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::net::TcpListener;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Config file {0} does not exist")]
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

    if let Some(path) = args.path {
        config.persist_path = path;
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
    println!("Saving data to: {}", config.persist_path.bold());
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
    println!(
        "☁️ To deploy your DB - try Chroma Cloud!\n- Sign up: {}\n- Docs: {}\n- Copy your data to Cloud: {}\n",
        "https://trychroma.com/signup".underline().blue(),
        "https://docs.trychroma.com/cloud/getting-started".underline().blue(),
        "chroma copy --to-cloud --all".yellow()
    );
}

pub fn run(args: RunArgs) -> Result<(), CliError> {
    let config = match &args.config_path {
        Some(config_path) => {
            if !std::path::Path::new(config_path).exists() {
                eprintln!(
                    "Could not find {config_path:?} in {:?}",
                    std::env::current_dir()
                        .map(
                            |p| String::from_utf8_lossy(p.as_os_str().as_encoded_bytes())
                                .to_string()
                        )
                        .unwrap_or("<unknown>".to_string())
                );
                return Err(RunError::ConfigFileNotFound(config_path.to_string()).into());
            }
            FrontendServerConfig::load_from_path(config_path)
        }
        None => override_default_config_with_args(args)?,
    };

    display_run_message(&config);

    let runtime = tokio::runtime::Runtime::new().map_err(|_| RunError::ServerStartFailed)?;
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config, true).await;
    });
    Ok(())
}
