mod client;
mod commands;
mod utils;

use crate::commands::db::{db_command, DbCommand};
use crate::commands::profile::{profile_command, ProfileCommand};
use crate::commands::run::{run, RunArgs};
use crate::commands::vacuum::{vacuum, VacuumArgs};
use clap::{Parser, Subcommand};
use std::io;

#[derive(Subcommand, Debug)]
enum Command {
    #[command(subcommand)]
    DB(DbCommand),
    Docs,
    #[command(subcommand)]
    Profile(ProfileCommand),
    Run(RunArgs),
    Support,
    Vacuum(VacuumArgs),
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
    println!();
    
    match cli.command {
        Command::DB(db_subcommand) => {
            let stdout = io::stdout();
            let mut out = stdout.lock();
            db_command(&mut out, db_subcommand).expect("Failed to write output");
        }
        Command::Docs => {
            let url = "https://docs.trychroma.com";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}\n.", url);
            }
        }
        Command::Profile(profile_subcommand) => {
            let stdout = io::stdout();
            let mut out = stdout.lock();
            profile_command(&mut out, profile_subcommand).expect("Failed to write output");
        }
        Command::Run(args) => {
            run(args);
        }
        Command::Support => {
            let url = "https://discord.gg/MMeYNTmh3x";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}\n.", url);
            }
        }
        Command::Vacuum(args) => {
            vacuum(args);
        }
    }
    println!();
}
