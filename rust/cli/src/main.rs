use std::sync::Arc;

use chroma_frontend::{config::FrontendConfig, frontend_service_entrypoint_with_config};
use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug)]
enum Command {
    Docs,
    Run {
        #[arg(short, long)]
        path: Option<String>,
    },
    Support,
}

#[derive(Parser, Debug)]
#[command(name = "chroma")]
#[command(version = "0.0.1")]
#[command(about = "A CLI for Chroma", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

impl Cli {
    fn run() {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        let default_config = FrontendConfig::single_node_default();
        runtime.block_on(async {
            frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), default_config)
                .await;
        });
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Docs => {
            let url = "https://docs.trychroma.com";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}.", url);
            }
        }
        Command::Run {path: _} => {
            // TODO: Allow user to specify a config file
            Cli::run();
        }
        Command::Support => {
            let url = "https://discord.gg/MMeYNTmh3x";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}.", url);
            }
        }
    }
}
