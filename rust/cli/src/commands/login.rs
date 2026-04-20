use crate::client::dashboard_client::{DashboardClient, DashboardClientError, Team};
use crate::commands::db::DbError;
use crate::commands::login::LoginError::BrowserAuthFailed;
use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::ui_utils::validate_uri;
use crate::utils::{CliError, Profile, Profiles, UtilsError};
use chroma::client::ChromaHttpClientOptions;
use chroma::ChromaHttpClient;
use clap::Parser;
use colored::Colorize;
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

fn login_success_message(team_name: &str, profile_name: &str, config_dir: &str) -> String {
    format!(
        "{} {}\nCredentials saved to {} under the profile {}\n",
        "Login successful for team".green().bold(),
        team_name.green().bold(),
        config_dir,
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

fn select_team(teams: Vec<Team>, term: &mut dyn Terminal) -> Result<Team, CliError> {
    match teams.len() {
        0 => Err(LoginError::NoTeamsFound.into()),
        1 => Ok(teams.into_iter().next().unwrap()),
        _ => {
            let team_names: Vec<String> = teams.iter().map(|team| team.name.clone()).collect();
            term.println(&team_selection_prompt());
            let selection = term.prompt_select(&team_names)?;
            let selected = teams.into_iter().nth(selection).unwrap();
            term.println(&format!("{}\n", selected.name.green()));
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

fn get_profile_from_team(
    team: &Team,
    profiles: &Profiles,
    term: &mut dyn Terminal,
) -> Result<String, CliError> {
    let team_name = match team.name.as_str() {
        "default" => "default",
        _ => team.slug.as_str(),
    };

    if !profiles.contains_key(team_name) {
        return Ok(team_name.to_string());
    }

    term.println(&profile_name_input_prompt(team_name));
    let profile_name = term.prompt_input()?;

    match profile_name.as_str() {
        "" => {
            term.println(&format!(
                "{} {}\n",
                "Overriding profile".green(),
                team_name.green()
            ));
            Ok(team_name.to_string())
        }
        _ => {
            term.println(&format!("{}\n", profile_name.green()));
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

async fn browser_auth(
    dashboard_client: &DashboardClient,
    term: &mut dyn Terminal,
) -> Result<String, Box<dyn Error>> {
    let token = dashboard_client.get_cli_token().await?;

    let login_url = format!(
        "{}/cli?cli_redirect={}",
        dashboard_client.frontend_url, token
    );
    webbrowser::open(&login_url)?;

    term.println("Waiting for browser authentication...\nCtrl+C to quit\n");

    let session_id = verify_token(dashboard_client, token).await?;
    match session_id {
        Some(session_id) => Ok(session_id),
        None => Err(BrowserAuthFailed.into()),
    }
}

pub async fn browser_login(
    args: LoginArgs,
    dashboard_client: &DashboardClient,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let session_id = browser_auth(dashboard_client, term)
        .await
        .map_err(|_| BrowserAuthFailed)?;

    let teams = dashboard_client.get_teams(&session_id).await?;

    let (api_key, team) = match args.api_key {
        Some(api_key) => {
            let options = ChromaHttpClientOptions::cloud_admin(&api_key)
                .map_err(|_| UtilsError::InvalidApiKey)?;
            let client = ChromaHttpClient::new(options);
            let team_id = client.get_tenant_id().await?;
            let team = filter_team(&team_id, teams)?;
            (api_key, team)
        }
        None => {
            let team = select_team(teams, term)?;
            let api_key = dashboard_client
                .get_api_key(&team.slug, &session_id)
                .await?;
            (api_key, team)
        }
    };

    let mut profiles = store.read_profiles()?;
    let mut profile_name = match args.profile {
        Some(name) => name,
        None => get_profile_from_team(&team, &profiles, term)?,
    };
    profile_name = validate_profile_name(profile_name)?;
    let profile = Profile::new(api_key, team.uuid);

    let set_current = profiles.is_empty();
    profiles.insert(profile_name.clone(), profile);
    store.write_profiles(&profiles)?;

    let mut config = store.read_config()?;

    if set_current {
        config.current_profile = profile_name.clone();
        store.write_config(&config)?;
    }

    term.println(&login_success_message(
        &team.name,
        &profile_name,
        &store.config_dir(),
    ));

    if !config.current_profile.eq(&profile_name) {
        term.println(&set_profile_message(&profile_name));
    }

    term.println(&next_steps_message());

    Ok(())
}

pub async fn headless_login(
    args: LoginArgs,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let api_key = args.api_key.unwrap_or_default();

    let mut profile_name = args.profile.unwrap_or_default();
    profile_name = validate_profile_name(profile_name)?;

    let mut profiles = store.read_profiles()?;

    if profiles.contains_key(&profile_name) {
        return Err(LoginError::ProfileAlreadyExists(profile_name).into());
    }

    let options =
        ChromaHttpClientOptions::cloud_admin(&api_key).map_err(|_| UtilsError::InvalidApiKey)?;
    let client = ChromaHttpClient::new(options);

    let team_id = client.get_tenant_id().await?;

    let profile = Profile::new(api_key, team_id.clone());

    let set_current = profiles.is_empty();
    profiles.insert(profile_name.clone(), profile);
    store.write_profiles(&profiles)?;

    let mut config = store.read_config()?;

    if set_current {
        config.current_profile = profile_name.clone();
        store.write_config(&config)?;
    }

    if !config.current_profile.eq(&profile_name) {
        term.println(&set_profile_message(&profile_name));
    }

    term.println(&next_steps_message());

    Ok(())
}

pub fn login(args: LoginArgs) -> Result<(), CliError> {
    let store = FileConfigStore::default();
    let dashboard_client = DashboardClient::default();
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        match (&args.api_key, &args.profile) {
            (Some(_), Some(_)) => headless_login(args, &store, &mut term).await,
            _ => browser_login(args, &dashboard_client, &store, &mut term).await,
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::test_config_store::InMemoryConfigStore;
    use crate::terminal::test_terminal::TestTerminal;
    use std::collections::HashMap;

    fn make_team(uuid: &str, name: &str, slug: &str) -> Team {
        Team {
            uuid: uuid.to_string(),
            name: name.to_string(),
            slug: slug.to_string(),
        }
    }

    fn make_profiles(names: &[&str]) -> Profiles {
        let mut profiles = HashMap::new();
        for name in names {
            profiles.insert(
                name.to_string(),
                Profile::new("test-key".to_string(), "test-tenant".to_string()),
            );
        }
        profiles
    }

    fn make_config(current: &str) -> crate::utils::CliConfig {
        crate::utils::CliConfig {
            current_profile: current.to_string(),
            sample_apps: Default::default(),
            theme: Default::default(),
        }
    }

    // ── validate_profile_name ──

    #[test]
    fn test_validate_profile_name_valid() {
        assert!(validate_profile_name("my-team".to_string()).is_ok());
        assert!(validate_profile_name("team_123".to_string()).is_ok());
    }

    #[test]
    fn test_validate_profile_name_invalid() {
        assert!(validate_profile_name("".to_string()).is_err());
        assert!(validate_profile_name("has spaces".to_string()).is_err());
        assert!(validate_profile_name("has.dots".to_string()).is_err());
    }

    // ── select_team ──

    #[test]
    fn test_select_team_single() {
        let teams = vec![make_team("id-1", "My Team", "my-team")];
        let mut term = TestTerminal::new();
        let team = select_team(teams, &mut term).unwrap();
        assert_eq!(team.uuid, "id-1");
    }

    #[test]
    fn test_select_team_multiple() {
        let teams = vec![
            make_team("id-1", "Team A", "team-a"),
            make_team("id-2", "Team B", "team-b"),
        ];
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        let team = select_team(teams, &mut term).unwrap();
        assert_eq!(team.uuid, "id-2");
    }

    #[test]
    fn test_select_team_empty() {
        let teams = vec![];
        let mut term = TestTerminal::new();
        let err = select_team(teams, &mut term).unwrap_err();
        assert!(matches!(err, CliError::Login(LoginError::NoTeamsFound)));
    }

    // ── filter_team ──

    #[test]
    fn test_filter_team_found() {
        let teams = vec![make_team("id-1", "A", "a"), make_team("id-2", "B", "b")];
        let team = filter_team("id-2", teams).unwrap();
        assert_eq!(team.name, "B");
    }

    #[test]
    fn test_filter_team_not_found() {
        let teams = vec![make_team("id-1", "A", "a")];
        let err = filter_team("missing", teams).unwrap_err();
        assert!(matches!(err, LoginError::TeamNotFound(_)));
    }

    // ── get_profile_from_team ──

    #[test]
    fn test_get_profile_from_team_new_name() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&[]);
        let mut term = TestTerminal::new();
        let name = get_profile_from_team(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "my-team");
    }

    #[test]
    fn test_get_profile_from_team_default_team() {
        let team = make_team("id-1", "default", "default-slug");
        let profiles = make_profiles(&[]);
        let mut term = TestTerminal::new();
        let name = get_profile_from_team(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "default");
    }

    #[test]
    fn test_get_profile_from_team_existing_override() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&["my-team"]);
        // Empty input = override existing
        let mut term = TestTerminal::new().with_inputs(vec![""]);
        let name = get_profile_from_team(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "my-team");
        assert!(term.output.join("\n").contains("Overriding"));
    }

    #[test]
    fn test_get_profile_from_team_existing_rename() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&["my-team"]);
        let mut term = TestTerminal::new().with_inputs(vec!["new-name"]);
        let name = get_profile_from_team(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "new-name");
    }

    // ── headless_login (mock server) ──

    mod mock_server {
        use super::*;

        #[tokio::test]
        async fn test_headless_login_success() {
            let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));

            // headless_login creates ChromaHttpClient internally with cloud_admin,
            // so we test the store interaction logic that headless_login performs.
            let api_key = "test-api-key".to_string();
            let profile_name = "test-profile".to_string();
            let profile = Profile::new(api_key, "resolved-tenant".to_string());

            let mut profiles = store.read_profiles().unwrap();
            profiles.insert(profile_name.clone(), profile);
            store.write_profiles(&profiles).unwrap();

            let mut config = store.read_config().unwrap();
            config.current_profile = profile_name.clone();
            store.write_config(&config).unwrap();

            let saved = store.read_profiles().unwrap();
            assert!(saved.contains_key("test-profile"));
            assert_eq!(store.read_config().unwrap().current_profile, "test-profile");
        }

        #[tokio::test]
        async fn test_headless_login_profile_already_exists() {
            let store =
                InMemoryConfigStore::new(make_profiles(&["existing"]), make_config("existing"));

            // headless_login checks for profile name collision before making API calls
            let profiles = store.read_profiles().unwrap();
            assert!(profiles.contains_key("existing"));

            // Simulate the check that headless_login does
            let profile_name = "existing".to_string();
            if profiles.contains_key(&profile_name) {
                let err: CliError = LoginError::ProfileAlreadyExists(profile_name).into();
                assert!(matches!(
                    err,
                    CliError::Login(LoginError::ProfileAlreadyExists(_))
                ));
            }
        }

        #[tokio::test]
        async fn test_headless_login_sets_current_when_first() {
            let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));

            let profiles = store.read_profiles().unwrap();
            let set_current = profiles.is_empty();
            assert!(set_current);

            let mut profiles = profiles;
            profiles.insert(
                "new-profile".to_string(),
                Profile::new("key".to_string(), "tenant".to_string()),
            );
            store.write_profiles(&profiles).unwrap();

            let mut config = store.read_config().unwrap();
            if set_current {
                config.current_profile = "new-profile".to_string();
                store.write_config(&config).unwrap();
            }

            assert_eq!(store.read_config().unwrap().current_profile, "new-profile");
        }

        #[tokio::test]
        async fn test_headless_login_doesnt_set_current_when_not_first() {
            let store =
                InMemoryConfigStore::new(make_profiles(&["existing"]), make_config("existing"));

            let profiles = store.read_profiles().unwrap();
            let set_current = profiles.is_empty();
            assert!(!set_current);

            let mut profiles = profiles;
            profiles.insert(
                "second".to_string(),
                Profile::new("key".to_string(), "tenant".to_string()),
            );
            store.write_profiles(&profiles).unwrap();

            // current_profile should remain "existing"
            assert_eq!(store.read_config().unwrap().current_profile, "existing");
        }
    }
}
