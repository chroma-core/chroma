use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use clap::Parser;
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;
use rand::Rng;
use thiserror::Error;
use crate::clients::dashboard_client::Team;
use crate::utils::{validate_name, CliError, Profiles, UtilsError};

const CLI_QUERY_PARAMETER: &str = "cli_redirect";

#[derive(Parser, Debug)]
pub struct LoginArgs {
    #[clap(long, help = "Profile name to associate with auth credentials")]
    profile: Option<String>,
    #[clap(long = "api-key", help = "API key")]
    api_key: Option<String>,
    #[clap(long, hide = true, help = "Flag to use during development")]
    dev: bool,
}

#[derive(Debug, Error)]
pub enum LoginError {
    #[error("Profile {0}")]
    InvalidProfileName(#[from] UtilsError),
    #[error("No teams found for user")]
    NoTeamsFound,
    #[error("Could not start server for auth redirect")]
    ServerStartFailed,
    #[error("Browser auth failed")]
    BrowserAuthFailed,
    #[error("Team {0} not found")]
    TeamNotFound(String),
}

fn team_selection_prompt() -> String {
    "Which team would you like to log in with?".blue().bold().to_string()
}

fn profile_name_input_prompt() -> String {
    "Input a profile name".blue().bold().to_string()
}

fn waiting_for_cli_host_message() -> String {
    "\nWaiting for browser authentication...\n(Ctrl-C to quit)\n".to_string()
}

fn validate_profile_name(profile_name: String) -> Result<String, CliError> {
    Ok(validate_name(profile_name).map_err(LoginError::InvalidProfileName)?)
}

fn select_team(teams: &[Team]) -> Result<&Team, CliError> {
    match teams.len() {
        0 => Err(LoginError::NoTeamsFound.into()),
        1 => Ok(teams.iter().next().unwrap()),
        _ => {
            let team_names: Vec<String> = teams.iter().map(|team| team.name.clone()).collect();
            println!("{}", team_selection_prompt());
            let selection = Select::with_theme(&ColorfulTheme::default())
                .items(&team_names)
                .default(0)
                .interact().map_err(|_| UtilsError::UserInputFailed)?;
            let selected = teams.into_iter().nth(selection).unwrap();
            println!("{}\n", selected.name.green());
            Ok(selected)
        }
    }
}

fn find_random_available_port(start: u16, end: u16, attempts: u32) -> Result<u16, CliError> {
    let mut rng = rand::thread_rng();
    for _ in 0..attempts {
        let port = rng.gen_range(start..=end);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port)
        }
    }
    Err(LoginError::ServerStartFailed.into())
}

fn filter_team(team_id: &str, teams: Vec<Team>) -> Result<Team, LoginError> {
    teams.into_iter()
        .find(|team| team.uuid.eq(team_id))
        .ok_or_else(|| LoginError::TeamNotFound(team_id.to_string()).into())
}

fn select_profile(team: &Team, profiles: &Profiles) -> Result<String, Box<dyn Error>> {
    let team_name = match team.name.as_str() {
        "default" => "default",
        _ => team.slug.as_str(),
    };

    let suggestion = match profiles.contains_key(team_name) {
        true => format!("{} (override)", team_name),
        false => team.slug.clone(),
    };

    println!("{}", profile_name_input_prompt());
    let profile = Input::with_theme(&ColorfulTheme::default())
        .default(suggestion)
        .interact_text()?;
    println!();

    Ok(profile.replace(" (override)", ""))
}

fn browser_login(api_url: &str, frontend_url: &str) -> Result<String, Box<dyn Error>> {
    let port = find_random_available_port(8050, 9000, 100)?;
    let login_url = format!("{}/login?{}=http://localhost:{}", api_url, CLI_QUERY_PARAMETER, port);

    webbrowser::open(&login_url)?;
    println!("{}", waiting_for_cli_host_message());

    let cli_host = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&cli_host)?;

    let (mut stream, _) = listener.accept()?;
    let mut buffer = [0; 1024];
    let _ = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..]);

    let cookies = request.lines().find(|line| line.starts_with("Cookie:"));
    if cookies.is_none() {
        return Err(LoginError::BrowserAuthFailed.into());
    }
    let cookies = cookies.unwrap().trim_start_matches("Cookie:").trim();

    let redirect_url = format!("{}/cli", frontend_url);
    let response = format!(
        "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\n\r\n",
        redirect_url
    );
    stream.write_all(response.as_bytes())?;

    Ok(cookies.to_string())
}

pub fn login(args: LoginArgs) -> Result<(), CliError> {
    let env_config = load_cli_env_config(args.dev);
    let session_cookies = match browser_login(env_config.dashboard_url, args.redirect_port) {
        Ok(cookies) => cookies,
        Err(e) => {
            let message = format!("Failed to authenticate user: {}", e);
            eprintln!("{}\n", message.red());
            return;
        }
    };

    let teams = match get_teams(env_config.dashboard_api_url, &session_cookies) {
        Ok(teams) => teams,
        Err(e) => {
            let message = format!("Failed to fetch teams for user: {}", e);
            eprintln!("{}\n", message.red());
            return;
        }
    };

    let (api_key, team) = match args.api_key {
        Some(api_key) => {
            let team_id = match get_tenant_id(env_config.frontend_url, api_key.as_str()) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("{}\n", "Failed to find team ID for API key".red());
                    return;
                }
            };
            let team = match filter_team(team_id.as_str(), teams) {
                Ok(team) => team,
                Err(_) => {
                    eprintln!("{}\n", "Failed to find team ID for API key".red());
                    return;
                }
            };
            (api_key, team)
        },
        None => {
            let team = match select_team(teams) {
                Ok(team) => team,
                Err(_) => {
                    eprintln!("{}\n", "Failed to find teams for user".red());
                    return;
                }
            };
            let api_key = match get_api_key(env_config.dashboard_api_url, &team.slug, &session_cookies) {
                Ok(api_key) => api_key,
                Err(_) => {
                    eprintln!("{}\n", "Failed to generate API key for user".red());
                    return;
                }
            };
            (api_key, team)
        }
    };

    let mut profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("{}\n", "Failed to load Chroma credentials file".red());
            return;
        }
    };

    let profile_name = match args.profile {
        Some(profile) => profile,
        None => {
            match select_profile(&team, &profiles) {
                Ok(profile) => profile,
                Err(_) => {
                    eprintln!("{}\n", "Failed to get profile name".red());
                    return;
                }
            }
        }
    };

    if validate_profile(&profile_name).is_err() {
        eprintln!("{}\n", "Profile name is not valid".red());
        return;
    }

    let profile = Profile { name: profile_name.clone(), api_key, team_id: team.uuid.clone() };
    let set_current = profiles.is_empty();
    profiles.insert(profile_name.clone(), profile.clone());

    match write_credentials(&profiles) {
        Ok(_) => {},
        Err(_) => {
            eprintln!("{}\n", "Failed to write credentials with new profile".red());
            return;
        }
    }

    if set_current {
        let config = CliConfig { current_profile: profile.name };
        match write_config(&config) {
            Ok(_) => {},
            Err(_) => {
                eprintln!("{}\n", "Failed to write config file".red());
                return;
            }
        }
    }

    println!("{}", format!("Login successful for team {}!", team.name).green().bold());
    println!("Credentials saved to ~/.chroma/credentials under the profile {}", profile_name.clone());

    let cli_config = match read_config() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("{}\n", "Failed to read config file".red());
            return;
        }
    };
    if cli_config.current_profile != profile_name {
        let command = format!("chroma profile use {}", profile_name);
        println!("\nTo set this profile as the current profile: {}", command.yellow());
    }

    println!("\nTry this next:");
    println!("  Create a database");
    println!("    {}", "chroma db create <db_name>".yellow());

    println!("\nFor a full list of commands:");
    println!("  {}\n", "chroma --help".yellow());
}

