mod commands;
mod utils;
mod types;

use crate::commands::run::run;
use crate::commands::vacuum::vacuum;
use clap::{Parser, Subcommand};
use crate::commands::browser::BrowserCommandHandler;
use crate::types::Command;

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
