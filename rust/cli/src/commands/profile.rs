use std::io::Write;
use crate::utils::{get_config, get_profiles, save_config, save_profiles, CliConfig, Profiles};
use clap::{Args, Subcommand};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;

#[derive(Args, Debug)]
pub struct DeleteArgs {
    name: String,
}

#[derive(Args, Debug)]
pub struct UseArgs {
    name: String,
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    Delete(DeleteArgs),
    List,
    Show,
    Use(UseArgs),
}

fn profile_exists<W: Write>(writer: &mut W, profile_name: &str, profiles: &Profiles)-> Result<bool, std::io::Error> {
    if !profiles.contains_key(profile_name) {
        let message = format!("Profile {} not found", profile_name);
        writeln!(writer, "{}", message.red())?;
        Ok(false)
    } else {
        Ok(true)
    }
}

fn confirm_deletion<W: Write>(writer: &mut W, profile_name: &str) -> Result<bool, std::io::Error> {
    let message = format!(
        "{}\n{}\n{} {}, {}\n\nDo you want to delete profile {}? (Y/n)",
        "Warning! You are deleting the currently active profile".yellow().bold(),
        "All Chroma Cloud CLI operations will fail without an active profile.",
        "If you wish to proceed, please use:",
        "chroma profile use <profile name>".yellow(),
        "to set a new profile",
        profile_name
    );
    writeln!(writer, "{}", message)?;

    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();
    
    Ok(confirm.to_lowercase() == "y" || confirm.to_lowercase() == "yes")
}

fn delete_profile<W: Write>(writer: &mut W,args: DeleteArgs, profiles: &mut Profiles, config: &mut CliConfig) -> Result<(), std::io::Error> {
    let profile = args.name;
    if !profile_exists(writer, &profile, profiles)? { return Ok(()) }
    
    if config.current_profile == profile {
        let confirmed = confirm_deletion(writer, &profile)?;
        if confirmed {
            println!();
            config.current_profile = "".to_string();
            if save_config(config).is_err() { return Ok(()) }
        } else {
            return Ok(());
        }
    };

    profiles.remove(&profile);
    if save_profiles(profiles).is_err() { return Ok(()) }

    writeln!(writer,
        "{} {} {}",
        "Profile".green(),
        profile.green(),
        "successfully removed".green()
    )?;
    Ok(())
}

fn list_profiles<W: Write>(writer: &mut W, profiles: Profiles, config: CliConfig) -> Result<(), std::io::Error> {
    if profiles.is_empty() {
        writeln!(writer,
            "No profiles defined at the moment. To add a new profile use {}",
            "chroma login".yellow()
        )?;
        return Ok(())
    }

    writeln!(writer, "{}", "Available profiles:".blue().bold())?;

    if !config.current_profile.is_empty() {
        let current_profile_label = format!("{} (current)", config.current_profile).bold();
        writeln!(writer, "{} {}", ">".yellow(), current_profile_label)?;
    }

    for key in profiles.keys() {
        if *key != config.current_profile {
            writeln!(writer, "{} {}", ">".yellow(), key)?;
        }
    }
    Ok(())
}

fn use_profile<W: Write>(writer: &mut W, args: UseArgs, profiles: Profiles, config: &mut CliConfig) -> Result<(), std::io::Error> {
    let exists = profile_exists(writer, &args.name, &profiles)?;
    if !exists { return Ok(()) }
    
    config.current_profile = args.name;
    _ = save_config(config);
    let message = format!("Current profile set to {}", config.current_profile);
    writeln!(writer, "{}", message.green())?;
    Ok(())
}

fn show<W: Write>(writer: &mut W,config: CliConfig) -> Result<(), std::io::Error> {
    if config.current_profile.is_empty() {
        writeln!(
            writer,
            "No profile set currently. Please use {} to add a profile, or {} to set an existing profile",
            "chroma login".yellow(),
            "chroma use <profile name>".yellow()
        )?;
    }

    writeln!(writer, "{}", "Current profile: ".blue().bold())?;
    writeln!(writer, "{}", config.current_profile)?;
    Ok(())
}

pub fn profile_command<W: Write>(writer: &mut W, command: ProfileCommand) -> Result<(), std::io::Error> {
    let mut profiles = match get_profiles() {
        Some(p) => p,
        None => return Ok(()),
    };

    let mut config = match get_config() {
        Some(c) => c,
        None => return Ok(()),
    };
    
    match command {
        ProfileCommand::Delete(args) => delete_profile(writer, args, &mut profiles, &mut config),
        ProfileCommand::List => list_profiles(writer, profiles, config),
        ProfileCommand::Show => show(writer, config),
        ProfileCommand::Use(args) => use_profile(writer, args, profiles, &mut config),
    }
}

#[cfg(test)]
mod tests {
   
}