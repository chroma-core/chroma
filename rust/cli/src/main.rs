mod commands;
mod utils;

use chroma_frontend::{config::FrontendConfig, frontend_service_entrypoint_with_config};
use clap::{Parser, Subcommand};
use colored::*;
use dialoguer::Confirm;
use std::path::Path;
use std::sync::Arc;
use chroma_frontend::frontend::Frontend;

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
    #[arg(long = "config-path")]
    config_path: Option<String>,
    #[arg(long, default_value = DEFAULT_PATH)]
    path: String,
}

#[derive(Parser, Debug)]
struct VacuumArgs {
    #[arg(long)]
    force: bool,
    #[arg(long, default_value = DEFAULT_PATH)]
    path: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    Docs,
    Run(RunArgs),
    Support,
    Vacuum(VacuumArgs),
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

    let mut config = match &args.config_path {
        Some(config_path) => FrontendConfig::load_from_path(config_path),
        None => FrontendConfig::single_node_default(),
    };

    if let Some(sqlite_config) = &mut config.sqlitedb {
        sqlite_config.url = Some(format!("{}/{}", args.path, SQLITE_FILENAME));
    }

    let data_path = config
        .sqlitedb
        .as_ref()
        .and_then(|sqlite_config| sqlite_config.url.as_deref())
        .unwrap_or(args.path.as_str())
        .replace(format!("/{}", SQLITE_FILENAME).as_str(), "");

    println!("Saving data to: {}", data_path.bold());
    println!(
        "Connect to Chroma at: {}",
        "http://localhost:3000".underline().blue()
    );
    println!(
        "Getting started guide: {}",
        "https://docs.trychroma.com/docs/overview/getting-started"
            .underline()
            .blue()
    );

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), config).await;
    });
}

fn vacuum(args: VacuumArgs) {
    // Vacuum the database. This may result in a small increase in performance.
    // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
    // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    if (!Path::new(args.path.as_str()).exists()) {
        println!("{}", format!("Path does not exist: {}", args.path).red());
        return;
    }

    if (!Path::new(format!("{}/{}", args.path, SQLITE_FILENAME).as_str()).exists()) {
        println!("{}", format!("Not a Chroma path: {}", args.path).red());
        return;
    }

    let proceed = Confirm::new()
        .with_prompt("Are you sure you want to vacuum the database? This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?")
        .default(false)
        .interact()
        .unwrap_or_else(|e| {
            eprintln!("Failed to get confirmation: {}", e);
            false
        });

    if (!proceed) {
        println!("{}", "Vacuum cancelled".red());
        return;
    }

    let frontend = Frontend::try_from_config(args.)

    println!();
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
        Command::Vacuum(args) => {
            vacuum(args);
        }
    }
}
