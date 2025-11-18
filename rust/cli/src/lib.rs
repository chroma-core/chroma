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
    #[command(about = "Browse Chroma collections", long_about = None)]
    Browse(BrowseArgs),
    #[command(about = "Copy collection between local and Chroma Cloud", long_about = None)]
    Copy(CopyArgs),
    #[command(about = "Manage Chroma Cloud databases", long_about = None)]
    #[command(subcommand)]
    Db(DbCommand),
    #[command(about = "Open Chroma online documentation", long_about = None)]
    Docs,
    #[command(about = "Install sample applications", long_about = None)]
    Install(InstallArgs),
    #[command(about = "Log in to Chroma Cloud", long_about = None)]
    Login(LoginArgs),
    #[command(about = "Manage Chroma Cloud profiles", long_about = None)]
    #[command(subcommand)]
    Profile(ProfileCommand),
    #[command(about = "Start a local Chroma server", long_about = None)]
    Run(RunArgs),
    #[command(about = "Open the Chroma Discord", long_about = None)]
    Support,
    #[command(about = "Check for Chroma CLI updates", long_about = None)]
    Update,
    #[command(about = "Vacuum a local Chroma persistent directory", long_about = None)]
    Vacuum(VacuumArgs),
}

#[derive(Parser, Debug)]
#[command(name = "chroma")]
#[command(version = "1.2.2")]
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
