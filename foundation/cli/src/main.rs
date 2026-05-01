mod cli;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let exit_code = match cli.command {
        Commands::Version => cli::version::execute(),
        Commands::Completion { shell } => cli::completion::execute(shell),
    };

    std::process::exit(exit_code);
}
