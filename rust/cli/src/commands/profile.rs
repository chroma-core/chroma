use clap::{Args, Subcommand};
use colored::Colorize;
use crate::utils::{get_profiles, read_config, write_config, write_profiles};

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

#[allow(dead_code)]
fn delete_profile(args: DeleteArgs) {
    let mut profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };
    let profile = args.name;

    if !profiles.contains_key(&profile) {
        let message = format!("Profile {} not found", profile);
        eprintln!("\n{}\n", message.red());
        return;
    }

    let mut config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };

    if config.current_profile == profile {
        config.current_profile = "".to_string();
        match write_config(&config) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("\n{}\n", "Failed to save CLI config".red());
                return;
            }
        };
    }

    profiles.remove(&profile);
    match write_profiles(&profiles) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("\n{}\n", "Failed to save credentials file".red());
            return; 
        }
    }

    println!(
        "{} {} {}",
        "\nProfile".green(),
        profile.green(),
        "successfully removed\n".green()
    );
}

#[allow(dead_code)]
fn list_profiles() {
    println!("{}", "\nAvailable profiles:".blue().bold());
    let profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };
    for key in profiles.keys() {
        println!("{} {}", ">".yellow(), key)
    }
    println!();
}

#[allow(dead_code)]
fn use_profile(args: UseArgs) {
    let profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };
    
    if !profiles.contains_key(&args.name) {
        let message = format!("Profile {} not found", args.name);
        eprintln!("\n{}\n", message.red());
        return;
    }

    let mut config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };
    
    config.current_profile = args.name;
    match write_config(&config) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("\n{}\n", "Failed to save CLI config".red());
        }
    };
    
}

fn show() {
    let config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };
    
    println!("\n{}", "Current profile: ".blue().bold());
    if config.current_profile.is_empty() {
       println!("\nNo profile set currently. Please use {} to add a profile\n", "chroma login".yellow());
        return;
    }
    println!("{}\n", config.current_profile);

}

#[allow(dead_code)]
pub fn profile_command(command: ProfileCommand) {
    match command {
        ProfileCommand::Delete(args) => delete_profile(args),
        ProfileCommand::List => list_profiles(),
        ProfileCommand::Show => show(),
        ProfileCommand::Use(args) => use_profile(args),
    }
}
