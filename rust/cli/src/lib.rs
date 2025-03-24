mod commands;
mod utils;
mod client;

use clap::{Parser, Subcommand};
use thiserror::Error;
use crate::commands::browser::{BrowserCommandHandler, BrowserError, DISCORD_URL, DOCS_URL};
use crate::commands::run::{RunArgs, RunCommandHandler, RunError};
use crate::commands::vacuum::{VacuumArgs, VacuumCommandHandler, VacuumError};
use crate::utils::UtilsError;

#[derive(Subcommand, Debug)]
pub enum Command {
    Docs,
    Run(RunArgs),
    Support,
    Vacuum(VacuumArgs),
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Browser(#[from] BrowserError),
    #[error("{0}")]
    Run(#[from] RunError),
    #[error("{0}")]
    Utils(#[from] UtilsError),
    #[error("{0}")]
    Vacuum(#[from] VacuumError),
}

#[async_trait::async_trait]
pub trait Handler {
    async fn run(&mut self) -> Result<(), CliError>;
}

#[derive(Parser, Debug)]
#[command(name = "chroma")]
#[command(version = "1.0.0")]
#[command(about = "A CLI for Chroma", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

pub fn chroma_cli(args: Vec<String>) {
    let cli = Cli::parse_from(args);
    
    let mut handler: Box<dyn Handler> = match cli.command {
        Command::Docs => Box::new(BrowserCommandHandler::new(DOCS_URL)),
        Command::Run(args) => Box::new(RunCommandHandler::default(args)),
        Command::Support => Box::new(BrowserCommandHandler::new(DISCORD_URL)),
        Command::Vacuum(args) => Box::new(VacuumCommandHandler::default(args)),
    };

    let runtime = tokio::runtime::Runtime::new().map_err(|_| RunError::ServerStartFailed).unwrap();
    runtime.block_on(async {
        handler.run().await.unwrap()
    });
}
