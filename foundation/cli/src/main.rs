mod cli;
mod client;
mod commands;
mod config_store;
mod error;
mod terminal;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let exit_code = match cli.command {
        Commands::Version => cli::version::execute(),
        Commands::Completion { shell } => cli::completion::execute(shell),
        Commands::Login(args) => commands::login::login(args).await,
        Commands::Logout(args) => commands::login::logout(args).await,
        Commands::Whoami => commands::login::whoami(),
    };

    std::process::exit(exit_code);
}
