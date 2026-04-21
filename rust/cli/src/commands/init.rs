use crate::client::dashboard_client::DashboardClient;
use crate::commands::db::{create_env_connection, validate_db_name, DbError};
use crate::commands::login::{browser_login, LoginArgs};
use crate::commands::profile::ProfileError;
use crate::commands::skills::{install_skill, InstallSkillArgs};
use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::ui::{
    print_section_header, print_status_line, print_summary_panel, FilterableSelectItem,
    PanelSelectPrompt,
};
use crate::utils::{cloud_client, CliError, Profile, Profiles};
use chroma::ChromaHttpClient;
use clap::Parser;
use colored::Colorize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("Failed to get runtime for init command")]
    RuntimeError,
}

#[derive(Parser, Debug)]
pub struct InitArgs {}

pub fn init(_args: InitArgs) -> Result<(), CliError> {
    let store = FileConfigStore::default();
    let dashboard_client = DashboardClient::default();
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InitError::RuntimeError)?;
    runtime.block_on(async { run_init(&dashboard_client, &store, &mut term).await })
}

async fn run_init(
    dashboard_client: &DashboardClient,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    print_section_header(term, "Initialize Chroma workspace");

    let (profile_name, profile) = resolve_profile(dashboard_client, store, term).await?;
    let client = cloud_client(&profile)?;

    let db_name = resolve_database(&client, term).await?;

    create_env_connection(profile.clone(), db_name.clone()).map_err(|_| DbError::EnvFile)?;
    print_summary_panel(
        term,
        "Environment",
        &format!(".env written with credentials for {}", db_name),
    );

    if prompt_install_skills(term)? {
        install_skill(InstallSkillArgs::for_skill("chroma-cloud"), term).await?;
    }

    term.println(&format!(
        "\nWorkspace initialized with profile {}",
        profile_name
    ));
    term.println("Next, ask your agent: Let's add chroma cloud to this project for search");
    Ok(())
}

async fn resolve_profile(
    dashboard_client: &DashboardClient,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(String, Profile), CliError> {
    let existing = store.read_profiles()?;
    if existing.is_empty() {
        print_status_line(
            term,
            "Login",
            "No profile found. Opening your browser to log in...",
        );
        browser_login(LoginArgs::default(), dashboard_client, store, term, false).await?;
    }

    let profiles = store.read_profiles()?;
    let mut config = store.read_config()?;

    let profile_name = if profiles.len() == 1 {
        profiles.keys().next().unwrap().clone()
    } else {
        select_profile_name(&profiles, &config.current_profile, term)?
    };

    if config.current_profile != profile_name {
        config.current_profile = profile_name.clone();
        store.write_config(&config)?;
    }

    let profile = profiles
        .get(&profile_name)
        .cloned()
        .ok_or_else(|| ProfileError::ProfileNotFound(profile_name.clone()))?;

    print_summary_panel(term, "Profile", &profile_name);
    Ok((profile_name, profile))
}

fn select_profile_name(
    profiles: &Profiles,
    current: &str,
    term: &mut dyn Terminal,
) -> Result<String, CliError> {
    let mut names: Vec<String> = profiles.keys().cloned().collect();
    names.sort();
    let items: Vec<FilterableSelectItem> = names
        .iter()
        .map(|name| FilterableSelectItem {
            label: if name == current {
                format!("{} (current)", name)
            } else {
                name.clone()
            },
            summary: name.clone(),
        })
        .collect();
    let default_index = names.iter().position(|name| name == current).unwrap_or(0);
    let selection = term.prompt_panel_select(&PanelSelectPrompt {
        tag: "init",
        title: "Choose a profile",
        context_lines: &[],
        items: &items,
        default_selected_index: default_index,
        empty_message: "No profiles available.",
    })?;
    Ok(names[selection].clone())
}

async fn resolve_database(
    client: &ChromaHttpClient,
    term: &mut dyn Terminal,
) -> Result<String, CliError> {
    let dbs = client.list_databases().await?;
    let name = if dbs.is_empty() {
        term.println(
            &"No databases found. Let's create one."
                .blue()
                .bold()
                .to_string(),
        );
        term.println("What is the name of your new DB?");
        let raw = term.prompt_input()?;
        let name = validate_db_name(&raw)?;
        client.create_database(name.clone()).await?;
        name
    } else {
        let items: Vec<FilterableSelectItem> = dbs
            .iter()
            .map(|db| FilterableSelectItem {
                label: db.name.clone(),
                summary: db.name.clone(),
            })
            .collect();
        let selection = term.prompt_panel_select(&PanelSelectPrompt {
            tag: "init",
            title: "Choose a database",
            context_lines: &[],
            items: &items,
            default_selected_index: 0,
            empty_message: "No databases available.",
        })?;
        dbs[selection].name.clone()
    };

    print_summary_panel(term, "Database", &name);
    Ok(name)
}

fn prompt_install_skills(term: &mut dyn Terminal) -> Result<bool, CliError> {
    let items = vec![
        FilterableSelectItem {
            label: "Yes - install Chroma skills for your agent".to_string(),
            summary: "Install skills".to_string(),
        },
        FilterableSelectItem {
            label: "Skip - install later with chroma skills install".to_string(),
            summary: "Skip".to_string(),
        },
    ];
    let context_lines = vec!["Skills teach agents how to use Chroma effectively.".to_string()];
    let selection = term.prompt_panel_select(&PanelSelectPrompt {
        tag: "init",
        title: "Install Chroma agent skills?",
        context_lines: &context_lines,
        items: &items,
        default_selected_index: 0,
        empty_message: "No choices available.",
    })?;
    Ok(selection == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::test_config_store::InMemoryConfigStore;
    use crate::terminal::test_terminal::TestTerminal;
    use crate::utils::CliConfig;
    use std::collections::HashMap;

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

    fn make_config(current: &str) -> CliConfig {
        CliConfig {
            current_profile: current.to_string(),
            sample_apps: Default::default(),
            theme: Default::default(),
        }
    }

    #[test]
    fn select_profile_name_prefers_current_by_default() {
        let profiles = make_profiles(&["alpha", "beta", "gamma"]);
        // sorted alpha=0, beta=1, gamma=2 — simulate user accepting the default
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);

        let name = select_profile_name(&profiles, "beta", &mut term).unwrap();

        assert_eq!(name, "beta");
    }

    #[test]
    fn select_profile_name_honors_selection() {
        let profiles = make_profiles(&["alpha", "beta", "gamma"]);
        let mut term = TestTerminal::new().with_inputs(vec!["2"]);

        let name = select_profile_name(&profiles, "alpha", &mut term).unwrap();

        assert_eq!(name, "gamma");
    }

    #[test]
    fn prompt_install_skills_true_when_selecting_yes() {
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);
        assert!(prompt_install_skills(&mut term).unwrap());
    }

    #[test]
    fn prompt_install_skills_false_when_selecting_skip() {
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        assert!(!prompt_install_skills(&mut term).unwrap());
    }

    mod mock_server {
        use super::*;
        use chroma::client::{ChromaAuthMethod, ChromaHttpClientOptions, ChromaRetryOptions};
        use httpmock::MockServer;

        fn mock_client(server: &MockServer) -> ChromaHttpClient {
            ChromaHttpClient::new(ChromaHttpClientOptions {
                endpoint: server.base_url().parse().unwrap(),
                auth_method: ChromaAuthMethod::None,
                retry_options: ChromaRetryOptions::default(),
                tenant_id: Some("test-tenant".to_string()),
                database_name: Some("default_database".to_string()),
            })
        }

        fn mock_list_databases(server: &MockServer, dbs: &[&str]) {
            let body: Vec<serde_json::Value> = dbs
                .iter()
                .enumerate()
                .map(|(i, name)| serde_json::json!({"id": format!("id-{}", i), "name": name}))
                .collect();
            server.mock(|when, then| {
                when.method("GET")
                    .path("/api/v2/tenants/test-tenant/databases");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!(body));
            });
        }

        #[tokio::test]
        async fn resolve_database_picks_from_existing() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["alpha", "beta"]);

            let mut term = TestTerminal::new().with_inputs(vec!["1"]);
            let name = resolve_database(&client, &mut term).await.unwrap();
            assert_eq!(name, "beta");
        }

        #[tokio::test]
        async fn resolve_database_creates_when_empty() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &[]);
            server.mock(|when, then| {
                when.method("POST")
                    .path("/api/v2/tenants/test-tenant/databases");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({}));
            });

            let mut term = TestTerminal::new().with_inputs(vec!["my-new-db"]);
            let name = resolve_database(&client, &mut term).await.unwrap();
            assert_eq!(name, "my-new-db");
        }
    }

    #[test]
    fn resolve_profile_uses_only_profile_as_current() {
        let store = InMemoryConfigStore::new(make_profiles(&["solo"]), make_config(""));
        let dashboard_client = DashboardClient::default();
        let mut term = TestTerminal::new();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (name, _profile) = runtime
            .block_on(async { resolve_profile(&dashboard_client, &store, &mut term).await })
            .unwrap();

        assert_eq!(name, "solo");
        assert_eq!(store.read_config().unwrap().current_profile, "solo");
    }

    #[test]
    fn resolve_profile_prompts_for_selection_when_multiple() {
        let store = InMemoryConfigStore::new(make_profiles(&["alpha", "beta"]), make_config(""));
        let dashboard_client = DashboardClient::default();
        // "alpha" is index 0 when sorted
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (name, _profile) = runtime
            .block_on(async { resolve_profile(&dashboard_client, &store, &mut term).await })
            .unwrap();

        assert_eq!(name, "alpha");
        assert_eq!(store.read_config().unwrap().current_profile, "alpha");
    }
}
