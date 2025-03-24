use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use clap::Parser;
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;
use rand::Rng;
use thiserror::Error;
use crate::clients::chroma_client::get_chroma_client;
use crate::clients::dashboard_client::{get_dashboard_client, Team};
use crate::commands::db::DbError;
use crate::utils::{read_config, read_profiles, validate_name, write_config, write_profiles, CliError, Profile, Profiles, UtilsError, CHROMA_DIR, CREDENTIALS_FILE};

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

fn login_success_message(team_name: &str, profile_name: &str) -> String {
    format!(
        "{} {}\nCredentials saved to ~/{}/{} under the profile {}\n", 
        "Login successful for team".green().bold(),
        team_name.green().bold(),
        CHROMA_DIR,
        CREDENTIALS_FILE,
        profile_name
    )
}

fn set_profile_message(profile_name: &str) -> String {
    format!(
        "To set this profile as the current profile: {} {}",
        "chroma profile use".yellow(),
        profile_name.yellow(),
    )
}

fn next_steps_message() -> String {
    println!("\nTry this next:");
    println!("  Create a database");
    println!("    {}", "chroma db create <db_name>".yellow());

    println!("\nFor a full list of commands:");
    println!("  {}\n", "chroma --help".yellow());
    format!(
        "Try this next:\n   Create a database\n    {}\n\nFor a full list of commands:\n   {}",
        "chroma db create <db_name>".yellow(),
        "chroma --help".yellow()
    )
}

fn validate_profile_name(profile_name: String) -> Result<String, CliError> {
    Ok(validate_name(profile_name).map_err(LoginError::InvalidProfileName)?)
}

fn select_team(teams: Vec<Team>) -> Result<Team, CliError> {
    match teams.len() {
        0 => Err(LoginError::NoTeamsFound.into()),
        1 => Ok(teams.into_iter().next().unwrap()),
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
        .ok_or_else(|| LoginError::TeamNotFound(team_id.to_string()))
}

fn select_profile(team: &Team, profiles: &Profiles) -> Result<String, CliError> {
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
        .interact_text().map_err(|_| UtilsError::UserInputFailed)?;
    println!();

    Ok(profile.replace(" (override)", ""))
}

fn browser_auth(api_url: &str, frontend_url: &str) -> Result<String, Box<dyn Error>> {
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

pub async fn browser_login(args: LoginArgs) -> Result<(), CliError> {
    let dashboard_client = get_dashboard_client(args.dev);
    let session_cookies = browser_auth(&dashboard_client.api_url, &dashboard_client.frontend_url).map_err(|_| LoginError::BrowserAuthFailed)?;
    let teams = dashboard_client.get_teams(&session_cookies).await?;
    
    let (api_key, team) = match args.api_key { 
        Some(api_key) => {
            let chroma_client = get_chroma_client(
                Some(&Profile::new(api_key.clone(), "default".to_string())), args.dev
            );
            let team_id = chroma_client.get_tenant_id().await?;
            let team = filter_team(&team_id, teams)?;
            (api_key, team)
        },
        None => {
            let team = select_team(teams)?;
            let api_key = dashboard_client.get_api_key(&team.slug, &session_cookies).await?;
            (api_key, team)
        }
    };
    
    let mut profiles = read_profiles()?;
    let mut profile_name = match args.profile { 
        Some(name) => name,
        None => {
            select_profile(&team, &profiles)?
        }
    };
    profile_name = validate_profile_name(profile_name)?;
    let profile = Profile::new(api_key, team.uuid);
    
    let set_current = profiles.is_empty();
    profiles.insert(profile_name.clone(), profile);
    write_profiles(&profiles)?;

    let mut config = read_config()?;
    
    if set_current {
        config.current_profile = profile_name.clone();
        write_config(&config)?;
    }
    
    println!("{}", login_success_message(&team.name, &profile_name));
    
    if !config.current_profile.eq(&profile_name) {
        println!("{}", set_profile_message(&profile_name));
    }
    
    println!("{}", next_steps_message());
    
    Ok(())
}

pub fn login(args: LoginArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
       browser_login(args).await
    })?;
    Ok(())
}

