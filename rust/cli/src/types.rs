use clap::Subcommand;
use thiserror::Error;
use crate::commands::browser::BrowserError;
use crate::commands::run::RunArgs;
use crate::commands::vacuum::VacuumArgs;

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
}

pub trait Handler {
    fn run(&mut self) -> Result<(), CliError>;
}
