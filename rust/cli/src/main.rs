mod commands;
mod utils;

use clap::{Parser, Subcommand};
use crate::commands::run::{run, RunArgs};
use crate::commands::vacuum::{vacuum, VacuumArgs};
use crate::utils::LocalFrontendCommandArgs;

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

fn main() {
    // let cli = Cli::parse();
    
    // remove pub
    vacuum(VacuumArgs { 
        frontend_args: LocalFrontendCommandArgs { 
            config_path: None, 
            persistent_path: Some("/Users/itaismith/Developer/playground/simple-chroma/chroma_data".to_string())
        },
        force: false,
    });

    // match cli.command {
    //     Command::Docs => {
    //         let url = "https://docs.trychroma.com";
    //         if webbrowser::open(url).is_err() {
    //             eprintln!("Error: Failed to open the browser. Visit {}.", url);
    //         }
    //     }
    //     Command::Run(args) => {
    //         run(args);
    //     }
    //     Command::Support => {
    //         let url = "https://discord.gg/MMeYNTmh3x";
    //         if webbrowser::open(url).is_err() {
    //             eprintln!("Error: Failed to open the browser. Visit {}.", url);
    //         }
    //     }
    //     Command::Vacuum(args) => {
    //         vacuum(args);
    //     }
    // }
}
