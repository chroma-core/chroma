use chroma_frontend::{config::FrontendConfig, frontend_service_entrypoint_with_config};
use clap::{Parser, Subcommand};
use std::sync::Arc;
use colored::*;

const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

const DEFAULT_PATH: &str = "./chroma";
const SQLITE_FILENAME: &str = "chroma.sqlite3";

#[derive(Parser, Debug)]
struct RunArgs {
    #[arg(long)]
    config: Option<String>,
    #[arg(long, default_value = DEFAULT_PATH)]
    path: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    Docs,
    Run(RunArgs),
    Support,
}

#[derive(Parser, Debug)]
#[command(name = "chroma")]
#[command(version = "0.1.0")]
#[command(about = "A CLI for Chroma", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn run(args: RunArgs) {
    println!("{}", LOGO);
    println!("\n{}\n", "Running Chroma".bold());

    let mut config = match &args.config {
        Some(config_path) => FrontendConfig::load_from_path(config_path),
        None => FrontendConfig::single_node_default(),
    };

    if let Some(sqlite_config) = &mut config.sqlitedb {
        sqlite_config.url = Some(format!("{}/{}", args.path, SQLITE_FILENAME));
    }

    let data_path = config.sqlitedb
        .as_ref()
        .and_then(|sqlite_config| sqlite_config.url.as_deref())
        .unwrap_or(args.path.as_str()).replace(format!("/{}", SQLITE_FILENAME).as_str(), "");

    println!("Saving data to: {}",data_path.bold());
    println!("Connect to Chroma at: {}", "http://localhost:3000".underline().blue());
    println!("Getting started guide: {}", "https://docs.trychroma.com/docs/overview/getting-started".underline().blue());

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
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
