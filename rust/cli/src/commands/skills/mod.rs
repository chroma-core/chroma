mod agents;
mod install;
mod registry;

use crate::terminal::{SystemTerminal, Terminal};
use crate::tui::style;
use crate::tui::widgets::{print_command_hint, print_section_header};
use crate::utils::CliError;
use clap::{Parser, Subcommand};
use colored::Colorize;
use thiserror::Error;

pub(crate) use install::{install_skill, InstallSkillArgs};
use registry::fetch_skills_registry;

#[derive(Debug, Error)]
pub enum SkillsError {
    #[error("Failed to get runtime for skills commands")]
    RuntimeError,
    #[error("Failed to fetch the skills registry")]
    RegistryFetchFailed,
    #[error("Failed to parse the skills registry")]
    RegistryParseFailed,
    #[error("No such skill {0}")]
    NoSuchSkill(String),
    #[error("The skills registry entry for {0} is invalid")]
    InvalidRegistryEntry(String),
    #[error("Failed to download skill {skill} (file {file})")]
    SkillDownloadFailed { skill: String, file: String },
    #[error("Downloaded skill {0} did not contain any files")]
    EmptySkill(String),
    #[error("No supported agents were detected on this machine")]
    NoDetectedAgents,
    #[error("No agents were selected for installation")]
    NoAgentsSelected,
    #[error("No such agent {0}")]
    NoSuchAgent(String),
    #[error("Global installation is not supported for agent {0}")]
    GlobalInstallUnsupported(String),
    #[error("Failed to read the current working directory")]
    CurrentDirUnavailable,
    #[error("Computed install path escaped its base directory")]
    UnsafeInstallPath,
    #[error("Failed to create skills directory")]
    CreateDirFailed,
    #[error("Failed to remove existing skill installation")]
    RemoveExistingFailed,
    #[error("Failed to write skill files")]
    WriteSkillFailed,
    #[error("Failed to create skill symlink")]
    SymlinkFailed,
}

#[derive(Parser, Debug)]
pub struct SkillsArgs {
    #[command(subcommand)]
    command: Option<SkillsSubcommand>,
}

#[derive(Subcommand, Debug)]
enum SkillsSubcommand {
    #[command(about = "List available Chroma skills")]
    List,
    #[command(about = "Install a Chroma skill")]
    Install(InstallSkillArgs),
}

pub fn skills(args: SkillsArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| SkillsError::RuntimeError)?;
    runtime.block_on(async {
        match args.command {
            None | Some(SkillsSubcommand::List) => list_skills(&mut term).await,
            Some(SkillsSubcommand::Install(install_args)) => {
                install_skill(install_args, &mut term).await
            }
        }
    })
}

async fn list_skills(term: &mut dyn Terminal) -> Result<(), CliError> {
    let client = reqwest::Client::new();
    let registry = fetch_skills_registry(&client).await?;
    print_section_header(term, "Available Chroma skills:");
    for skill in registry.skills {
        term.println(&format!(
            "{} {} - {}",
            style::list_marker(),
            skill.name.bold(),
            skill.description
        ));
    }
    term.println("");
    print_command_hint(
        term,
        "Install a skill with:",
        "chroma skills install <skill-name>",
    );
    Ok(())
}
