mod client;
mod commands;
mod tui;
mod ui_utils;
mod utils;

use crate::commands::browse::{browse, BrowseArgs};
use crate::commands::copy::{copy, CopyArgs};
use crate::commands::db::{db_command, DbCommand};
use crate::commands::install::{install, InstallArgs};
use crate::commands::login::{login, LoginArgs};
use crate::commands::profile::{profile_command, ProfileCommand};
use crate::commands::run::{run, RunArgs};
use crate::commands::update::update;
use crate::commands::vacuum::{vacuum, VacuumArgs};
use crate::commands::webpage::{open_browser, WebPageCommand};
use clap::{Parser, Subcommand};
use colored::Colorize;

#[derive(Subcommand, Debug)]
enum Command {
    Browse(BrowseArgs),
    Copy(CopyArgs),
    #[command(subcommand)]
    Db(DbCommand),
    Docs,
    Install(InstallArgs),
    Login(LoginArgs),
    #[command(subcommand)]
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

pub fn chroma_cli(args: Vec<String>) {
    let cli = Cli::parse_from(args);

    println!();

    let result = match cli.command {
        Command::Browse(args) => browse(args),
        Command::Copy(args) => copy(args),
        Command::Db(db_subcommand) => db_command(db_subcommand),
        Command::Docs => open_browser(WebPageCommand::Docs),
        Command::Install(args) => install(args),
        Command::Login(args) => login(args),
        Command::Profile(profile_subcommand) => profile_command(profile_subcommand),
        Command::Run(args) => run(args),
        Command::Support => open_browser(WebPageCommand::Discord),
        Command::Update => update(),
        Command::Vacuum(args) => vacuum(args),
    };

    if result.is_err() {
        let error_message = result.err().unwrap().to_string();
        eprintln!("{}", error_message.red());
    }

    println!();
}
