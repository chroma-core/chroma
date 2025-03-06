use crate::utils::{
    get_or_create_credentials_file, parse_credentials_file, read_cli_config, write_cli_config,
    write_credentials_file,
};
use clap::{Args, Subcommand};
use colored::Colorize;

#[derive(Args, Debug)]
struct DeleteArgs {
    name: String,
}

#[derive(Args, Debug)]
struct UseArgs {
    name: String,
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    Delete(DeleteArgs),
    List,
    Use(UseArgs),
}

fn delete_profile(args: DeleteArgs) {
    let credentials_file = get_or_create_credentials_file();
    let mut profiles =
        parse_credentials_file(&credentials_file).expect("\nCould not parse credentials file");
    let profile = args.name;

    if !profiles.contains_key(&profile) {
        eprintln!("\nProfile {} not found\n", profile);
        return;
    }

    let config = read_cli_config();

    if config.current_profile == profile {
        write_cli_config("".to_string())
    }

    profiles.remove(&profile);
    write_credentials_file(&profiles, &credentials_file).expect("Could not write credentials file");

    println!("\nProfile {} successfully removed\n", profile);
}

fn list_profiles() {
    println!("{}", "\nAvailable profiles:\n".blue().bold());
    let credentials_file = get_or_create_credentials_file();
    let profiles =
        parse_credentials_file(&credentials_file).expect("\nCould not parse credentials file");
    for key in profiles.keys() {
        println!("{} {}", ">".yellow(), key)
    }
    println!();
}

fn use_profile(args: UseArgs) {
    write_cli_config(args.name)
}

pub fn profile_command(command: ProfileCommand) {
    match command {
        ProfileCommand::Delete(args) => delete_profile(args),
        ProfileCommand::List => list_profiles(),
        ProfileCommand::Use(args) => use_profile(args),
    }
}
