use chroma_frontend::{config::FrontendConfig, frontend_service_entrypoint_with_config};
use clap::{Parser, Subcommand};
use std::sync::Arc;

#[derive(Parser, Debug)]
struct RunArgs {
    #[clap(name = "config", default_value = None)]
    config: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Docs,
    Run(RunArgs),
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

fn run(args: RunArgs) {
    let config = match &args.config {
        Some(path) => FrontendConfig::load_from_path(path),
        None => FrontendConfig::single_node_default(),
    };

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), config).await;
    });
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
        Command::Run(args) => {
            run(args);
        }
        Command::Support => {
            let url = "https://discord.gg/MMeYNTmh3x";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}.", url);
            }
        }
    }
}
