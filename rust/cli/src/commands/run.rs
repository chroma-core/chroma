use crate::utils::LOGO;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::net::TcpListener;
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

    #[clap(long, hide = true)]
    test: Option<bool>,
}

fn validate_host(address: &String, port: u16) -> bool {
    let socket = format!("{}:{}", address, port);
    TcpListener::bind(&socket).is_ok()
}

fn override_default_config_with_args(args: RunArgs) -> Result<FrontendServerConfig, String> {
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
        return Err(format!(
            "Address {}:{} is not available",
            config.listen_address, config.port
        ));
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
}

pub fn run(args: RunArgs) {
    let test = args.test.unwrap_or(false);

    let config = match &args.config_path {
        Some(config_path) => {
            if !std::path::Path::new(config_path).exists() {
                eprintln!("Config file {} does not exists", config_path);
                return;
            }
            FrontendServerConfig::load_from_path(config_path)
        }
        None => match override_default_config_with_args(args) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("{}", e);
                return;
            }
        },
    };

    display_run_message(&config);

    if test {
        return;
    }

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
    });
}
