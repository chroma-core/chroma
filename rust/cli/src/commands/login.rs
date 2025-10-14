use crate::client::admin_client::get_admin_client;
use crate::client::dashboard_client::{
    get_dashboard_client, DashboardClient, DashboardClientError, Team,
};
use crate::commands::db::DbError;
use crate::commands::login::LoginError::BrowserAuthFailed;
use crate::ui_utils::validate_uri;
use crate::utils::{
    read_config, read_profiles, write_config, write_profiles, CliError, Profile, Profiles,
    UtilsError, CHROMA_DIR, CREDENTIALS_FILE,
};
use clap::Parser;
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::error::Error;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

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
    #[error("Browser auth failed")]
    BrowserAuthFailed,
    #[error("Team {0} not found")]
    TeamNotFound(String),
    #[error("Profile {0} already exists")]
    ProfileAlreadyExists(String),
}

fn team_selection_prompt() -> String {
    "Which team would you like to log in with?"
        .blue()
        .bold()
        .to_string()
}

fn profile_name_input_prompt(profile_name: &str) -> String {
    format!(
        "{} {}\nPress Return to override it, or input a new profile name",
        "You already have a profile with team name".yellow().bold(),
        profile_name.yellow().bold()
    )
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
    format!(
        "Try this next:\n   Create a database\n    {}\n\nFor a full list of commands:\n   {}",
        "chroma db create <db_name>".yellow(),
        "chroma --help".yellow()
    )
}

fn validate_profile_name(profile_name: String) -> Result<String, LoginError> {
    validate_uri(profile_name).map_err(LoginError::InvalidProfileName)
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
                .interact()
                .map_err(|_| UtilsError::UserInputFailed)?;
            let selected = teams.into_iter().nth(selection).unwrap();
            println!("{}\n", selected.name.green());
            Ok(selected)
        }
    }
}

fn filter_team(team_id: &str, teams: Vec<Team>) -> Result<Team, LoginError> {
    teams
        .into_iter()
        .find(|team| team.uuid.eq(team_id))
        .ok_or_else(|| LoginError::TeamNotFound(team_id.to_string()))
}

fn get_profile_from_team(team: &Team, profiles: &Profiles) -> Result<String, CliError> {
    let team_name = match team.name.as_str() {
        "default" => "default",
        _ => team.slug.as_str(),
    };

    if !profiles.contains_key(team_name) {
        return Ok(team_name.to_string());
    }

    println!("{}", profile_name_input_prompt(team_name));
    let profile_name: String = Input::with_theme(&ColorfulTheme::default())
        .allow_empty(true)
        .report(false)
        .interact_text()
        .map_err(|_| UtilsError::UserInputFailed)?;

    match profile_name.as_str() {
        "" => {
            println!("{} {}\n", "Overriding profile".green(), team_name.green());
            Ok(team_name.to_string())
        }
        _ => {
            println!("{}\n", profile_name.green());
            Ok(profile_name)
        }
    }
}

async fn verify_token(
    dashboard_client: &DashboardClient,
    token: String,
) -> Result<Option<String>, DashboardClientError> {
    let timeout = Duration::from_secs(120); // 2 minutes
    let interval = Duration::from_secs(1);
    let start = tokio::time::Instant::now();

    while tokio::time::Instant::now().duration_since(start) < timeout {
        if let Ok(response) = dashboard_client.verify_cli_token(token.clone()).await {
            if response.success {
                return Ok(Some(response.session_id));
            }
        }
        sleep(interval).await;
    }
    Ok(None)
}

async fn browser_auth(dashboard_client: &DashboardClient) -> Result<String, Box<dyn Error>> {
    let token = dashboard_client.get_cli_token().await?;

    let login_url = format!(
        "{}/cli?cli_redirect={}",
        dashboard_client.frontend_url, token
    );
    webbrowser::open(&login_url)?;

    println!("Waiting for browser authentication...\nCtrl+C to quit\n");

    let session_id = verify_token(dashboard_client, token).await?;
    match session_id {
        Some(session_id) => Ok(session_id),
        None => Err(BrowserAuthFailed.into()),
    }
}

pub async fn browser_login(args: LoginArgs) -> Result<(), CliError> {
    let dashboard_client = get_dashboard_client(args.dev);

    let session_id = browser_auth(&dashboard_client)
        .await
        .map_err(|_| BrowserAuthFailed)?;

    let teams = dashboard_client.get_teams(&session_id).await?;

    let (api_key, team) = match args.api_key {
        Some(api_key) => {
            let admin_client = get_admin_client(
                Some(&Profile::new(api_key.clone(), "default".to_string())),
                args.dev,
            );
            let team_id = admin_client.get_tenant_id().await?;
            let team = filter_team(&team_id, teams)?;
            (api_key, team)
        }
        None => {
            let team = select_team(teams)?;
            let api_key = dashboard_client
                .get_api_key(&team.slug, &session_id)
                .await?;
            (api_key, team)
        }
    };

    let mut profiles = read_profiles()?;
    let mut profile_name = match args.profile {
        Some(name) => name,
        None => get_profile_from_team(&team, &profiles)?,
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

pub async fn headless_login(args: LoginArgs) -> Result<(), CliError> {
    let api_key = args.api_key.unwrap_or_default();

    let mut profile_name = args.profile.unwrap_or_default();
    profile_name = validate_profile_name(profile_name)?;

    let mut profiles = read_profiles()?;

    if profiles.contains_key(&profile_name) {
        return Err(LoginError::ProfileAlreadyExists(profile_name).into());
    }

    let admin_client = get_admin_client(
        Some(&Profile::new(api_key.clone(), profile_name.clone())),
        args.dev,
    );

    let team_id = admin_client.get_tenant_id().await?;

    let profile = Profile::new(api_key, team_id.clone());

    let set_current = profiles.is_empty();
    profiles.insert(profile_name.clone(), profile);
    write_profiles(&profiles)?;

    let mut config = read_config()?;

    if set_current {
        config.current_profile = profile_name.clone();
        write_config(&config)?;
    }

    if !config.current_profile.eq(&profile_name) {
        println!("{}", set_profile_message(&profile_name));
    }

    println!("{}", next_steps_message());

    Ok(())
}

pub fn login(args: LoginArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        match (&args.api_key, &args.profile) {
            (Some(_), Some(_)) => headless_login(args).await,
            _ => browser_login(args).await,
        }
    })?;
    Ok(())
}
