use crate::client::dashboard_client::{DashboardClient, Team};
use crate::config_store::{ConfigStore, FileConfigStore, Profile, Profiles};
use crate::error::FoundationError;
use crate::terminal::{SystemTerminal, Terminal};
use clap::Args;
use colored::Colorize;
use std::time::Duration;
use tokio::time::sleep;

// ── Args ────────────────────────────────────────────────────────────────────

#[derive(Args, Debug)]
pub struct LoginArgs {
    /// Profile name to associate with credentials (defaults to team slug)
    #[clap(long)]
    pub profile: Option<String>,
}

#[derive(Args, Debug)]
pub struct LogoutArgs {
    /// Profile name to log out (defaults to current active profile)
    #[clap(long)]
    pub profile: Option<String>,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn validate_profile_name(name: String) -> Result<String, FoundationError> {
    if name.is_empty() {
        return Err(FoundationError::InvalidProfileName);
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(FoundationError::InvalidProfileName);
    }
    Ok(name)
}

fn select_team(teams: Vec<Team>, term: &mut dyn Terminal) -> Result<Team, FoundationError> {
    match teams.len() {
        0 => Err(FoundationError::NoTeamsFound),
        1 => Ok(teams.into_iter().next().unwrap()),
        _ => {
            let team_names: Vec<String> = teams.iter().map(|t| t.name.clone()).collect();
            term.println(
                &"Which team would you like to log in with?"
                    .blue()
                    .bold()
                    .to_string(),
            );
            let selection = term.prompt_select(&team_names)?;
            let selected = teams.into_iter().nth(selection).unwrap();
            term.println(&format!("{}\n", selected.name.green()));
            Ok(selected)
        }
    }
}

fn resolve_profile_name(
    team: &Team,
    profiles: &Profiles,
    term: &mut dyn Terminal,
) -> Result<String, FoundationError> {
    let default_name = match team.name.as_str() {
        "default" => "default".to_string(),
        _ => team.slug.clone(),
    };

    if !profiles.contains_key(&default_name) {
        return Ok(default_name);
    }

    term.println(&format!(
        "{} {}\nPress Return to override it, or input a new profile name",
        "You already have a profile with team name".yellow().bold(),
        default_name.yellow().bold()
    ));
    let input = term.prompt_input()?;
    match input.as_str() {
        "" => {
            term.println(&format!(
                "{} {}\n",
                "Overriding profile".green(),
                default_name.green()
            ));
            Ok(default_name)
        }
        _ => {
            term.println(&format!("{}\n", input.green()));
            Ok(input)
        }
    }
}

async fn poll_for_session(
    client: &DashboardClient,
    token: String,
) -> Result<Option<String>, FoundationError> {
    let timeout = Duration::from_secs(120);
    let interval = Duration::from_secs(1);
    let start = tokio::time::Instant::now();

    while tokio::time::Instant::now().duration_since(start) < timeout {
        if let Ok(resp) = client.verify_cli_token(token.clone()).await {
            if resp.success {
                return Ok(Some(resp.session_id));
            }
        }
        sleep(interval).await;
    }
    Ok(None)
}

// ── Login ────────────────────────────────────────────────────────────────────

pub async fn run_login(
    args: LoginArgs,
    client: &DashboardClient,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), FoundationError> {
    let token = client.get_cli_token().await?;

    let login_url = format!("{}/cli?cli_redirect={}", client.frontend_url, token);

    // Best-effort browser open; fall back to printing the URL.
    if open::that(&login_url).is_err() {
        term.println(&format!(
            "Open this URL in your browser to authenticate:\n  {}",
            login_url.cyan()
        ));
    } else {
        term.println(&format!(
            "Opening browser for authentication...\n  {}\n{}",
            login_url.cyan(),
            "Waiting for browser authentication... (Ctrl+C to quit)\n".dimmed()
        ));
    }

    let session_id = poll_for_session(client, token)
        .await?
        .ok_or(FoundationError::BrowserAuthFailed)?;

    let teams = client.get_teams(&session_id).await?;
    let team = select_team(teams, term)?;
    let api_key = client.get_api_key(&team.slug, &session_id).await?;

    let mut profiles = store.read_profiles()?;
    let mut profile_name = match args.profile {
        Some(name) => name,
        None => resolve_profile_name(&team, &profiles, term)?,
    };
    profile_name = validate_profile_name(profile_name)?;

    let set_current = profiles.is_empty();
    profiles.insert(
        profile_name.clone(),
        Profile::new(api_key, team.uuid.clone()),
    );
    store.write_profiles(&profiles)?;

    let mut config = store.read_config()?;
    if set_current {
        config.current_profile = profile_name.clone();
        store.write_config(&config)?;
    }

    term.println(&format!(
        "{} {}\nCredentials saved to {} under the profile {}",
        "Login successful for team".green().bold(),
        team.name.green().bold(),
        store.config_dir(),
        profile_name.green()
    ));

    if !config.current_profile.eq(&profile_name) {
        term.println(&format!(
            "\nTo set this as the active profile:\n   {} {}",
            "foundation profile use".yellow(),
            profile_name.yellow()
        ));
    }

    term.println(&format!(
        "\nTry it out:\n   {}",
        "foundation ask \"What is our deployment process?\"".yellow()
    ));

    Ok(())
}

pub async fn login(args: LoginArgs) -> i32 {
    let client = DashboardClient::default();
    let store = FileConfigStore::default();
    let mut term = SystemTerminal;
    match run_login(args, &client, &store, &mut term).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{} {}", "Error:".red().bold(), e);
            1
        }
    }
}

// ── Logout ───────────────────────────────────────────────────────────────────

pub async fn run_logout(
    args: LogoutArgs,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), FoundationError> {
    let mut profiles = store.read_profiles()?;
    let mut config = store.read_config()?;

    let profile_name = match args.profile {
        Some(name) => name,
        None => {
            if config.current_profile.is_empty() {
                return Err(FoundationError::NoActiveProfile);
            }
            config.current_profile.clone()
        }
    };

    if !profiles.contains_key(&profile_name) {
        return Err(FoundationError::ProfileNotFound(profile_name));
    }

    profiles.remove(&profile_name);
    store.write_profiles(&profiles)?;

    if config.current_profile == profile_name {
        config.current_profile = String::new();
        store.write_config(&config)?;
    }

    term.println(&format!(
        "{} {}",
        "Logged out of profile".green().bold(),
        profile_name.green()
    ));

    if config.current_profile.is_empty() && !profiles.is_empty() {
        let remaining: Vec<&String> = profiles.keys().collect();
        term.println(&format!(
            "\nTo set a new active profile:\n   {} {}",
            "foundation profile use".yellow(),
            remaining[0].yellow()
        ));
    }

    Ok(())
}

pub async fn logout(args: LogoutArgs) -> i32 {
    let store = FileConfigStore::default();
    let mut term = SystemTerminal;
    match run_logout(args, &store, &mut term).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{} {}", "Error:".red().bold(), e);
            1
        }
    }
}

// ── Whoami ───────────────────────────────────────────────────────────────────

pub fn whoami() -> i32 {
    let store = FileConfigStore::default();
    let mut term = SystemTerminal;
    match run_whoami(&store, &mut term) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{} {}", "Error:".red().bold(), e);
            1
        }
    }
}

pub fn run_whoami(store: &dyn ConfigStore, term: &mut dyn Terminal) -> Result<(), FoundationError> {
    let (profile_name, profile) = store.get_current_profile()?;

    // Mask the API key — show prefix + last 4 chars
    let masked_key = mask_api_key(&profile.api_key);

    term.println(&format!(
        "{}\n  Profile:   {}\n  API key:   {}\n  Tenant ID: {}",
        "Current profile:".bold(),
        profile_name.green(),
        masked_key.dimmed(),
        profile.tenant_id.dimmed()
    ));

    Ok(())
}

fn mask_api_key(key: &str) -> String {
    if key.len() <= 8 {
        return "****".to_string();
    }
    // Keep up to the first dash-segment prefix (e.g. "chr-") + stars + last 4
    let last4 = &key[key.len() - 4..];
    let prefix: String = key.chars().take(4).collect();
    format!("{}...{}", prefix, last4)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::test_config_store::InMemoryConfigStore;
    use crate::config_store::FoundationConfig;
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
        let mut map = HashMap::new();
        for name in names {
            map.insert(
                name.to_string(),
                Profile::new("test-key".to_string(), "test-tenant".to_string()),
            );
        }
        map
    }

    fn make_config(current: &str) -> FoundationConfig {
        FoundationConfig {
            current_profile: current.to_string(),
        }
    }

    // ── validate_profile_name ──

    #[test]
    fn test_validate_profile_name_valid() {
        assert!(validate_profile_name("my-team".to_string()).is_ok());
        assert!(validate_profile_name("team_123".to_string()).is_ok());
        assert!(validate_profile_name("Acme".to_string()).is_ok());
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
        assert!(matches!(err, FoundationError::NoTeamsFound));
    }

    // ── resolve_profile_name ──

    #[test]
    fn test_resolve_profile_name_new() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&[]);
        let mut term = TestTerminal::new();
        let name = resolve_profile_name(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "my-team");
    }

    #[test]
    fn test_resolve_profile_name_default_team() {
        let team = make_team("id-1", "default", "default-slug");
        let profiles = make_profiles(&[]);
        let mut term = TestTerminal::new();
        let name = resolve_profile_name(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "default");
    }

    #[test]
    fn test_resolve_profile_name_existing_override() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&["my-team"]);
        let mut term = TestTerminal::new().with_inputs(vec![""]);
        let name = resolve_profile_name(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "my-team");
        assert!(term.output.join("\n").contains("Overriding"));
    }

    #[test]
    fn test_resolve_profile_name_existing_rename() {
        let team = make_team("id-1", "My Team", "my-team");
        let profiles = make_profiles(&["my-team"]);
        let mut term = TestTerminal::new().with_inputs(vec!["new-name"]);
        let name = resolve_profile_name(&team, &profiles, &mut term).unwrap();
        assert_eq!(name, "new-name");
    }

    // ── store interaction tests ──

    #[test]
    fn test_login_sets_current_when_first_profile() {
        let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));
        let mut profiles = store.read_profiles().unwrap();
        let set_current = profiles.is_empty();
        assert!(set_current);

        profiles.insert(
            "new".to_string(),
            Profile::new("key".to_string(), "tenant".to_string()),
        );
        store.write_profiles(&profiles).unwrap();

        let mut config = store.read_config().unwrap();
        if set_current {
            config.current_profile = "new".to_string();
            store.write_config(&config).unwrap();
        }
        assert_eq!(store.read_config().unwrap().current_profile, "new");
    }

    #[test]
    fn test_login_does_not_override_current_profile() {
        let store = InMemoryConfigStore::new(make_profiles(&["existing"]), make_config("existing"));
        let profiles = store.read_profiles().unwrap();
        let set_current = profiles.is_empty();
        assert!(!set_current);
        // current_profile should remain "existing"
        assert_eq!(store.read_config().unwrap().current_profile, "existing");
    }

    // ── logout tests ──

    #[tokio::test]
    async fn test_logout_removes_profile() {
        let store = InMemoryConfigStore::new(make_profiles(&["my-team"]), make_config("my-team"));
        let mut term = TestTerminal::new();
        run_logout(
            LogoutArgs {
                profile: Some("my-team".to_string()),
            },
            &store,
            &mut term,
        )
        .await
        .unwrap();
        assert!(!store.read_profiles().unwrap().contains_key("my-team"));
    }

    #[tokio::test]
    async fn test_logout_clears_current_profile() {
        let store = InMemoryConfigStore::new(make_profiles(&["my-team"]), make_config("my-team"));
        let mut term = TestTerminal::new();
        run_logout(LogoutArgs { profile: None }, &store, &mut term)
            .await
            .unwrap();
        assert_eq!(store.read_config().unwrap().current_profile, "");
    }

    #[tokio::test]
    async fn test_logout_nonexistent_profile_errors() {
        let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));
        let mut term = TestTerminal::new();
        let err = run_logout(
            LogoutArgs {
                profile: Some("ghost".to_string()),
            },
            &store,
            &mut term,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, FoundationError::ProfileNotFound(_)));
    }

    #[tokio::test]
    async fn test_logout_no_active_profile_errors() {
        let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));
        let mut term = TestTerminal::new();
        let err = run_logout(LogoutArgs { profile: None }, &store, &mut term)
            .await
            .unwrap_err();
        assert!(matches!(err, FoundationError::NoActiveProfile));
    }

    // ── whoami tests ──

    #[test]
    fn test_whoami_shows_profile() {
        let store = InMemoryConfigStore::new(make_profiles(&["my-team"]), make_config("my-team"));
        let mut term = TestTerminal::new();
        run_whoami(&store, &mut term).unwrap();
        let output = term.output.join("\n");
        assert!(output.contains("my-team"));
    }

    #[test]
    fn test_whoami_no_active_profile_errors() {
        let store = InMemoryConfigStore::new(make_profiles(&[]), make_config(""));
        let mut term = TestTerminal::new();
        let err = run_whoami(&store, &mut term).unwrap_err();
        assert!(matches!(err, FoundationError::NoActiveProfile));
    }

    // ── mask_api_key ──

    #[test]
    fn test_mask_api_key_normal() {
        let masked = mask_api_key("chr-abcdefghij1234");
        assert!(masked.contains("..."));
        assert!(masked.ends_with("1234"));
    }

    #[test]
    fn test_mask_api_key_short() {
        assert_eq!(mask_api_key("abc"), "****");
    }
}
