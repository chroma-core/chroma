use crate::utils::{
    read_config, read_profiles, write_config, write_profiles, CliConfig, CliError, Profiles,
    UtilsError,
};
use clap::{Args, Subcommand};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("Profile {0} not found")]
    ProfileNotFound(String),
    #[error("No current profile found.\nTo set a new profile use: chroma login")]
    NoActiveProfile,
    #[error("Profile {0} already exists.\nTo delete it use: chroma profile delete {0}")]
    ProfileAlreadyExists(String),
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(index = 1, help = "The name of the profile to delete")]
    name: String,
    #[clap(
        long,
        default_value_t = false,
        help = "Skip delete confirmation for the active profile"
    )]
    force: bool,
}

#[derive(Args, Debug)]
pub struct RenameArgs {
    #[clap(index = 1, help = "The name of the profile to rename")]
    name: String,
    #[clap(index = 2, help = "The new name for the profile to rename")]
    new_name: String,
}

#[derive(Args, Debug)]
pub struct UseArgs {
    #[clap(help = "The name of the profile to use as the active profile")]
    name: String,
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    #[command(about = "Delete profiles")]
    Delete(DeleteArgs),
    #[clap(about = "List all available profiles")]
    List,
    #[clap(about = "Show the current active profile")]
    Show,
    #[clap(about = "Rename a profile")]
    Rename(RenameArgs),
    #[clap(about = "Set the profile to use as the active profile")]
    Use(UseArgs),
}

fn confirm_profile_delete_message(profile_name: &str) -> String {
    format!(
        "{}\n{}\n{} {}, {}\n\nDo you want to delete profile {}? (Y/n)",
        "Warning! You are deleting the currently active profile"
            .yellow()
            .bold(),
        "All Chroma Cloud CLI operations will fail without an active profile.",
        "If you wish to proceed, please use:",
        "chroma profile use <profile name>".yellow(),
        "to set a new profile",
        profile_name
    )
}

fn profile_delete_abort_message() -> String {
    format!("{}", "\nDelete cancelled!".green())
}

fn profile_delete_success_message(profile_name: &str) -> String {
    format!(
        "{} {} {}",
        "Profile".green(),
        profile_name.green(),
        "successfully removed".green()
    )
}

fn no_profiles_found_message() -> String {
    format!(
        "No profiles defined at the moment. To add a new profile use {}",
        "chroma login".yellow()
    )
}

fn current_profile_set_message(profile_name: &str) -> String {
    format!("Current profile set to {}", profile_name)
        .green()
        .to_string()
}

fn no_current_profile_message() -> String {
    format!(
        "No profile set currently. Please use {} to add a profile, or {} to set an existing profile",
        "chroma login".yellow(),
        "chroma profile use <profile name>".yellow()
    )
}

fn current_profile_message(profile_name: &str) -> String {
    format!("{}\n{}", "Current profile: ".blue().bold(), profile_name)
}

fn rename_success_message(old_name: &str, new_name: &str) -> String {
    format!("Successfully renamed profile {} to {}", old_name, new_name)
        .green()
        .to_string()
}

fn confirm_deletion(profile_name: &str) -> Result<bool, CliError> {
    println!("{}", confirm_profile_delete_message(profile_name));

    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .map_err(|_| UtilsError::UserInputFailed)?;

    let confirmed = confirm.to_lowercase() == "y" || confirm.to_lowercase() == "yes";
    if confirmed {
        println!();
    }

    Ok(confirmed)
}

fn delete_profile(
    args: DeleteArgs,
    profiles: &mut Profiles,
    config: &mut CliConfig,
) -> Result<(), CliError> {
    let delete_profile_name = args.name;
    if !profiles.contains_key(&delete_profile_name) {
        return Err(ProfileError::ProfileNotFound(delete_profile_name).into());
    }

    if config.current_profile == delete_profile_name {
        let confirmed = args.force || confirm_deletion(&delete_profile_name)?;
        if confirmed {
            config.current_profile = "".to_string();
            write_config(config)?
        } else {
            println!("{}", profile_delete_abort_message());
            return Ok(());
        }
    };

    profiles.remove(&delete_profile_name);
    write_profiles(profiles)?;

    println!("{}", profile_delete_success_message(&delete_profile_name));
    Ok(())
}

fn list_profiles(profiles: Profiles, config: CliConfig) -> Result<(), CliError> {
    if profiles.is_empty() {
        println!("{}", no_profiles_found_message());
        return Ok(());
    }

    println!("{}", "Available profiles:".blue().bold());

    if !config.current_profile.is_empty() {
        let current_profile_label = format!("{} (current)", config.current_profile).bold();
        println!("{} {}", ">".yellow(), current_profile_label);
    }

    for key in profiles.keys() {
        if *key != config.current_profile {
            println!("{} {}", ">".yellow(), key);
        }
    }

    Ok(())
}

fn rename(
    args: RenameArgs,
    profiles: &mut Profiles,
    config: &mut CliConfig,
) -> Result<(), CliError> {
    let rename_profile_name = args.name;
    let new_name = args.new_name;

    if !profiles.contains_key(&rename_profile_name) {
        return Err(ProfileError::ProfileNotFound(rename_profile_name).into());
    }

    if profiles.contains_key(&new_name) {
        return Err(ProfileError::ProfileAlreadyExists(new_name).into());
    }

    let is_current = rename_profile_name.eq(&config.current_profile);

    profiles.insert(
        new_name.clone(),
        profiles.get(&rename_profile_name).unwrap().clone(),
    );
    write_profiles(profiles)?;

    if is_current {
        config.current_profile = new_name.clone();
        write_config(config)?;
    }

    println!(
        "{}",
        rename_success_message(&rename_profile_name, &new_name)
    );

    Ok(())
}

fn show(config: CliConfig) -> Result<(), CliError> {
    if config.current_profile.is_empty() {
        println!("{}", no_current_profile_message());
        return Ok(());
    }

    println!("{}", current_profile_message(&config.current_profile));
    Ok(())
}

fn use_profile(args: UseArgs, profiles: Profiles, config: &mut CliConfig) -> Result<(), CliError> {
    if !profiles.contains_key(&args.name) {
        return Err(ProfileError::ProfileNotFound(args.name.clone()).into());
    }

    config.current_profile = args.name;
    write_config(config)?;
    println!("{}", current_profile_set_message(&config.current_profile));
    Ok(())
}

pub fn profile_command(command: ProfileCommand) -> Result<(), CliError> {
    let mut profiles = read_profiles()?;
    let mut config = read_config()?;

    match command {
        ProfileCommand::Delete(args) => delete_profile(args, &mut profiles, &mut config),
        ProfileCommand::List => list_profiles(profiles, config),
        ProfileCommand::Rename(args) => rename(args, &mut profiles, &mut config),
        ProfileCommand::Show => show(config),
        ProfileCommand::Use(args) => use_profile(args, profiles, &mut config),
    }
}
