use crate::client::get_tenant_id;
use crate::utils::{
    get_or_create_credentials_file, read_credentials_file, write_cli_config,
    write_credentials_file, Profile,
};
use clap::Parser;
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::io;
use std::io::Write;

#[derive(Parser, Debug)]
pub struct LoginArgs {
    #[arg(long)]
    key: Option<String>,
    #[arg(long)]
    profile: Option<String>,
    #[arg(long)]
    team: Option<String>,
}

#[allow(dead_code)]
pub fn login(args: LoginArgs) {
    let credentials_file = get_or_create_credentials_file();
    let mut profiles =
        read_credentials_file(&credentials_file).expect("\nCould not parse credentials file\n");

    let api_key = args.key.unwrap_or_else(|| {
        println!("{}", "\nWhat is your API key?".blue().bold());
        print!("{} ", ">".yellow());
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read API key");
        input.trim().to_string()
    });

    let team = args.team.unwrap_or_else(|| {
        println!("{}", "\nWhat is your team name?".blue().bold());
        print!("{} ", ">".yellow());
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read team name");
        input.trim().to_string()
    });

    let tenant_id = get_tenant_id(&api_key).expect("Invalid API key");

    let has_default = profiles.contains_key("default");

    let profile = args.profile.unwrap_or_else(|| {
        if has_default {
            let options = vec![
                format!("{} {}", ">".yellow(), "default (override)"),
                format!("{} {}", ">".yellow(), "Create a new profile"),
            ];

            println!("{}", "\nChoose a profile name".blue().bold());
            let selection = Select::with_theme(&ColorfulTheme::default())
                .items(&options)
                .default(0)
                .interact()
                .unwrap();

            if selection == 0 {
                println!("{}", "Updated your default profile".yellow());
                "default".to_string()
            } else {
                println!("{}", "Creating a new profile".yellow());
                println!("{}", "\nInput your profile name".blue().bold());
                Input::with_theme(&ColorfulTheme::default())
                    .interact_text()
                    .unwrap()
            }
        } else {
            println!("{}", "\nInput your profile name".blue().bold());
            write_cli_config("default".to_string());
            Input::with_theme(&ColorfulTheme::default())
                .default("default".to_string())
                .interact_text()
                .unwrap()
        }
    });

    profiles.insert(
        profile.clone(),
        Profile {
            api_key: api_key.clone(),
            tenant_id: tenant_id.clone(),
            team: team.clone()
        },
    );

    if !has_default && profile != "default" {
        println!(
            "{} {} {}",
            "\nProfile".blue().bold(),
            profile.clone().bold().blue(),
            "saved as your default".blue().bold()
        );
        profiles.insert(
            "default".to_string(),
            Profile {
                api_key: api_key.clone(),
                tenant_id: tenant_id.clone(),
                team: team.clone()
            },
        );
    }

    println!();

    write_credentials_file(&profiles, &credentials_file)
        .expect("Failed to write Chrome credentials");
}
