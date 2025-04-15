mod client;
mod commands;
mod dashboard_client;
mod utils;

use crate::commands::db::{db_command, DbCommand};
use crate::commands::install::{install, InstallArgs};
use crate::commands::login::{login, LoginArgs};
use crate::commands::profile::{profile_command, ProfileCommand};
use crate::commands::run::{run, RunArgs};
use crate::commands::update::update;
use crate::commands::vacuum::{vacuum, VacuumArgs};
use clap::{Parser, Subcommand};
use colored::Colorize;
use utils::CliError;
use utils::UtilsError;

#[derive(Subcommand, Debug)]
enum Command {
    #[command(subcommand, hide = true)]
    Db(DbCommand),
    Docs,
    Install(InstallArgs),
    #[command(hide = true)]
    Login(LoginArgs),
    #[command(subcommand, hide = true)]
    Profile(ProfileCommand),
    Run(RunArgs),
    Support,
    Update,
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

fn open_browser(url: &str) -> Result<(), CliError> {
    let error_message = format!("Visit {}", url);
    webbrowser::open(url).map_err(|_| UtilsError::BrowserOpenFailed(error_message))?;
    Ok(())
}

pub fn chroma_cli(args: Vec<String>) {
    let cli = Cli::parse_from(args);

    println!();

    let result = match cli.command {
        Command::Db(db_subcommand) => db_command(db_subcommand),
        Command::Docs => {
            let url = "https://docs.trychroma.com";
            open_browser(url)
        }
        Command::Install(args) => install(args),
        Command::Login(args) => login(args),
        Command::Profile(profile_subcommand) => profile_command(profile_subcommand),
        Command::Run(args) => run(args),
        Command::Support => {
            let url = "https://discord.gg/MMeYNTmh3x";
            open_browser(url)
        }
        Command::Update => update(),
        Command::Vacuum(args) => vacuum(args),
    };

    if result.is_err() {
        let error_message = result.err().unwrap().to_string();
        eprintln!("{}", error_message.red());
    }

    println!();
}
