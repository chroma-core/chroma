use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::ui_utils::copy_to_clipboard;
use crate::utils::{
    cloud_client, CliError, Profile, CHROMA_API_KEY_ENV_VAR, CHROMA_DATABASE_ENV_VAR,
    CHROMA_TENANT_ENV_VAR, SELECTION_LIMIT,
};
use chroma::client::Database;
use chroma::ChromaHttpClient;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use std::error::Error;
use std::path::Path;
use std::{fmt, fs};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("No databases found")]
    NoDBs,
    #[error("Database name cannot be empty")]
    EmptyDbName,
    #[error("Database name must contain only alphanumeric characters, hyphens, or underscores")]
    InvalidDbName,
    #[error("DB {0} not found")]
    DbNotFound(String),
    #[error("Failed to get runtime for DB commands")]
    RuntimeError,
    #[error("Failed to create or update .env file with Chroma environment variables")]
    EnvFile,
}

#[derive(Debug, Clone, ValueEnum, EnumIter)]
pub enum Language {
    Python,
    JavaScript,
    TypeScript,
}

impl Language {
    fn get_connection(&self, tenant_id: String, db_name: String, api_key: String) -> String {
        match self {
            Language::Python => get_python_connection(tenant_id, db_name, api_key),
            Language::JavaScript => get_js_connection(tenant_id, db_name, api_key),
            Language::TypeScript => get_js_connection(tenant_id, db_name, api_key),
        }
    }
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Language::Python => write!(f, "python"),
            Language::JavaScript => write!(f, "javascript"),
            Language::TypeScript => write!(f, "typescript"),
        }
    }
}

#[derive(Args, Debug)]
pub struct CreateArgs {
    #[clap(index = 1, help = "The name of the DB to create")]
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(index = 1, help = "The name of the DB to delete")]
    name: Option<String>,
    #[clap(
        long,
        hide = true,
        default_value_t = false,
        help = "Delete without confirmation"
    )]
    force: bool,
}

#[derive(Args, Debug)]
pub struct ConnectArgs {
    #[clap(index = 1, help = "The name of the DB to get a connection snippet for")]
    name: Option<String>,
    #[clap(
        long,
        help = "The programming language to use for the connection snippet"
    )]
    language: Option<Language>,
    #[clap(long = "env-file", default_value_t = false, conflicts_with_all = ["language", "env_vars"], help = "Add Chroma environment variables to a .env file in the current directory")]
    env_file: bool,
    #[clap(long = "env-vars", default_value_t = false, conflicts_with_all = ["language", "env_file"], help = "Output Chroma environment variables")]
    env_vars: bool,
}

#[derive(Subcommand, Debug)]
pub enum DbCommand {
    #[command(about = "Generate a connection snippet to a DB")]
    Connect(ConnectArgs),
    #[command(about = "Create a new DB")]
    Create(CreateArgs),
    #[command(about = "Delete a DB")]
    Delete(DeleteArgs),
    #[command(about = "List all available DBs")]
    List,
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    if let Some(first) = c.next() {
        first.to_uppercase().chain(c).collect()
    } else {
        String::new()
    }
}

fn connect_db_name_prompt() -> String {
    "Which DB would you like to connect to?".to_string()
}

fn create_db_name_prompt() -> String {
    "What is the name of your new DB?".to_string()
}

fn delete_db_name_prompt() -> String {
    "What is the name of the DB you want to delete?".to_string()
}

fn no_dbs_message(profile_name: &str) -> String {
    format!(
        "Profile {} has 0 DBs. To create a new Chroma DB use: {}",
        profile_name,
        "chroma db create <db name>".yellow()
    )
}

fn env_file_created_message() -> String {
    format!(
        "{}",
        "Chroma environment variables set in .env!".blue().bold()
    )
}

fn select_language_message() -> String {
    "Which language would you like to use?".to_string()
}

fn create_db_already_exists_message(name: &str) -> String {
    format!(
        "{} {} {}\nIf you want to delete it, use: {} {}",
        "DB with name".red(),
        name.red(),
        "already exists!".red(),
        "chroma db delete".yellow(),
        name.yellow()
    )
}

fn creating_db_message(name: &str) -> String {
    format!(
        "\n{} {}...",
        "Creating database".bold().blue(),
        name.bold().blue()
    )
}

fn create_db_success_message(name: &str) -> String {
    format!(
        "{} {} {}\nTo get a connection string, run:\n   {} {}",
        "\nDatabase".green(),
        name.green(),
        "created successfully!".green(),
        "chroma db connect".yellow(),
        name.yellow()
    )
}

fn db_delete_confirm_message() -> String {
    format!("{}", "Are you sure you want to delete this DB?\nThis action cannot be reverted and you will lose all the data in this DB.\n\nIf you want to continue please type the name of DB to confirm.".red().bold())
}

fn db_delete_success_message(name: &str) -> String {
    format!("\nDeleted DB {} successfully!", name)
}

fn list_dbs_message(dbs: &[Database]) -> String {
    format!(
        "{} {} {}",
        "Listing".blue().bold(),
        dbs.len().to_string().blue().bold(),
        "databases".blue().bold()
    )
}

fn db_delete_cancelled() -> String {
    format!(
        "\n{}",
        "DB deletion cancelled. Confirmation input did not match DB name to delete".yellow()
    )
}

fn get_python_connection(tenant_id: String, db_name: String, api_key: String) -> String {
    format!(
        "Python connection snippet:
    import chromadb
    client = chromadb.CloudClient(
        api_key='{}',
        tenant='{}',
        database='{}'
    )",
        api_key, tenant_id, db_name
    )
}

fn get_js_connection(tenant_id: String, db_name: String, api_key: String) -> String {
    format!(
        "Javascript/Typescript connection snippet:
    import {{ CloudClient }} from 'chromadb';
    const client = new CloudClient({{
        apiKey: '{}',
        tenant: '{}',
        database: '{}'
    }});
",
        api_key, tenant_id, db_name
    )
}

fn prompt_db_name(term: &mut dyn Terminal) -> Result<String, CliError> {
    term.prompt_input()
}

fn validate_db_name(db_name: &str) -> Result<String, CliError> {
    if db_name.is_empty() {
        return Err(CliError::Db(DbError::EmptyDbName));
    }

    if !db_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(CliError::Db(DbError::InvalidDbName));
    }

    Ok(db_name.to_string())
}

fn select_db(dbs: &[Database], term: &mut dyn Terminal) -> Result<String, CliError> {
    let db_names: Vec<String> = dbs.iter().map(|db| db.name.clone()).collect();
    let selection = term.prompt_select(&db_names)?;
    let name = db_names[selection].clone();
    term.println(&format!("{}\n", name));
    Ok(name)
}

pub fn get_db_name(
    dbs: &[Database],
    prompt: &str,
    term: &mut dyn Terminal,
) -> Result<String, CliError> {
    if dbs.is_empty() {
        return Err(CliError::Db(DbError::NoDBs));
    }

    term.println(&format!("{}", prompt.blue().bold()));
    let name = match dbs.len() {
        0..=SELECTION_LIMIT => select_db(dbs, term),
        _ => prompt_db_name(term),
    }?;

    validate_db_name(name.as_str())
}

fn select_language(term: &mut dyn Terminal) -> Result<Language, CliError> {
    let languages: Vec<Language> = Language::iter().collect();
    let options: Vec<String> = languages
        .iter()
        .map(|language| format!("{} {}", ">".yellow(), capitalize(&language.to_string())))
        .collect();

    term.println(&format!("{}", select_language_message().blue().bold()));
    let selection = term.prompt_select(&options)?;

    let language = languages[selection].clone();
    term.println(&format!("{}", language.to_string().green()));

    Ok(language)
}

fn confirm_db_deletion(name: &str, term: &mut dyn Terminal) -> Result<bool, CliError> {
    term.println(&db_delete_confirm_message());
    let confirm = term.prompt_input()?;
    Ok(confirm.eq(name))
}

fn create_env_connection(current_profile: Profile, db_name: String) -> Result<(), Box<dyn Error>> {
    let env_path = ".env";
    let chroma_keys = [
        CHROMA_API_KEY_ENV_VAR,
        CHROMA_TENANT_ENV_VAR,
        CHROMA_DATABASE_ENV_VAR,
    ];

    let mut lines = Vec::new();

    if Path::new(env_path).exists() {
        let content = fs::read_to_string(env_path)?;

        for line in content.lines() {
            let keep = if let Some(eq_pos) = line.find('=') {
                let key = line[..eq_pos].trim();
                !chroma_keys.contains(&key)
            } else {
                true
            };

            if keep {
                lines.push(line.to_string());
            }
        }
    }

    lines.push(format!(
        "{}={}",
        CHROMA_API_KEY_ENV_VAR, current_profile.api_key
    ));
    lines.push(format!(
        "{}={}",
        CHROMA_TENANT_ENV_VAR, current_profile.tenant_id
    ));
    lines.push(format!("{}={}", CHROMA_DATABASE_ENV_VAR, db_name));

    fs::write(env_path, lines.join("\n") + "\n")?;

    Ok(())
}

pub async fn connect(
    args: ConnectArgs,
    current_profile: Profile,
    client: &ChromaHttpClient,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let dbs = client.list_databases().await?;

    let name = match args.name {
        Some(name) => validate_db_name(&name),
        None => get_db_name(&dbs, &connect_db_name_prompt(), term),
    }?;

    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(DbError::DbNotFound(name)));
    }

    if args.env_file {
        if create_env_connection(current_profile.clone(), name).is_err() {
            return Err(DbError::EnvFile.into());
        }
        term.println(&env_file_created_message());
        return Ok(());
    }

    if args.env_vars {
        term.println(&format!(
            "{}={}",
            CHROMA_API_KEY_ENV_VAR, current_profile.api_key
        ));
        term.println(&format!(
            "{}={}",
            CHROMA_TENANT_ENV_VAR, current_profile.tenant_id
        ));
        term.println(&format!("{}={}", CHROMA_DATABASE_ENV_VAR, name));
        return Ok(());
    }

    let language = match args.language {
        Some(language) => language,
        None => select_language(term)?,
    };

    let connection_string =
        language.get_connection(current_profile.tenant_id, name, current_profile.api_key);
    term.println(&connection_string);

    copy_to_clipboard(&connection_string)?;

    Ok(())
}

pub async fn create(
    args: CreateArgs,
    client: &ChromaHttpClient,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let dbs = client.list_databases().await?;

    let mut name = match args.name {
        Some(name) => name,
        None => {
            term.println(&create_db_name_prompt());
            prompt_db_name(term)?
        }
    };
    name = validate_db_name(&name)?;

    if dbs.iter().any(|db| db.name == name) {
        term.println(&create_db_already_exists_message(&name));
        return Ok(());
    }

    term.println(&creating_db_message(&name));

    client.create_database(name.clone()).await?;

    term.println(&create_db_success_message(&name));

    Ok(())
}

pub async fn delete(
    args: DeleteArgs,
    client: &ChromaHttpClient,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let dbs = client.list_databases().await?;

    let name = match args.name {
        Some(name) => validate_db_name(&name),
        None => get_db_name(&dbs, &delete_db_name_prompt(), term),
    }?;

    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(DbError::DbNotFound(name)));
    }

    if args.force || confirm_db_deletion(&name, term)? {
        client.delete_database(name.clone()).await?;
        term.println(&db_delete_success_message(&name));
    } else {
        term.println(&db_delete_cancelled())
    }

    Ok(())
}

pub async fn list(
    profile_name: String,
    client: &ChromaHttpClient,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let dbs = client.list_databases().await?;

    if dbs.is_empty() {
        term.println(&no_dbs_message(&profile_name));
        return Ok(());
    }

    term.println(&list_dbs_message(&dbs));
    for db in dbs {
        term.println(&format!("{} {}", ">".yellow(), db.name));
    }

    Ok(())
}

pub fn db_command(command: DbCommand) -> Result<(), CliError> {
    let store = FileConfigStore::default();
    let (profile_name, current_profile) = store.get_current_profile()?;
    let client = cloud_client(&current_profile)?;
    let mut term = SystemTerminal;

    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        match command {
            DbCommand::Connect(args) => connect(args, current_profile, &client, &mut term).await,
            DbCommand::Create(args) => create(args, &client, &mut term).await,
            DbCommand::Delete(args) => delete(args, &client, &mut term).await,
            DbCommand::List => list(profile_name, &client, &mut term).await,
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;
    use chroma::client::Database;

    fn make_dbs(names: &[&str]) -> Vec<Database> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| Database {
                id: format!("id-{}", i),
                name: name.to_string(),
            })
            .collect()
    }

    // ── validate_db_name ──

    #[test]
    fn test_validate_db_name_valid() {
        assert_eq!(validate_db_name("my-db_01").unwrap(), "my-db_01");
    }

    #[test]
    fn test_validate_db_name_empty() {
        let err = validate_db_name("").unwrap_err();
        assert!(matches!(err, CliError::Db(DbError::EmptyDbName)));
    }

    #[test]
    fn test_validate_db_name_special_chars() {
        assert!(validate_db_name("my db").is_err());
        assert!(validate_db_name("my.db").is_err());
        assert!(validate_db_name("my/db").is_err());
    }

    // ── get_db_name ──

    #[test]
    fn test_get_db_name_empty_list() {
        let dbs = make_dbs(&[]);
        let mut term = TestTerminal::new();
        let err = get_db_name(&dbs, "pick a db", &mut term).unwrap_err();
        assert!(matches!(err, CliError::Db(DbError::NoDBs)));
    }

    #[test]
    fn test_get_db_name_selection_mode() {
        let dbs = make_dbs(&["alpha", "beta", "gamma"]);
        // prompt_select receives index "1" -> selects "beta"
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        let name = get_db_name(&dbs, "pick a db", &mut term).unwrap();
        assert_eq!(name, "beta");
    }

    #[test]
    fn test_get_db_name_input_mode() {
        let dbs = make_dbs(&["a", "b", "c", "d", "e", "f"]); // > SELECTION_LIMIT
        let mut term = TestTerminal::new().with_inputs(vec!["my-db"]);
        let name = get_db_name(&dbs, "pick a db", &mut term).unwrap();
        assert_eq!(name, "my-db");
    }

    // ── select_db ──

    #[test]
    fn test_select_db() {
        let dbs = make_dbs(&["first", "second"]);
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);
        let name = select_db(&dbs, &mut term).unwrap();
        assert_eq!(name, "first");
    }

    // ── select_language ──

    #[test]
    fn test_select_language() {
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);
        let lang = select_language(&mut term).unwrap();
        assert!(matches!(lang, Language::Python));

        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        let lang = select_language(&mut term).unwrap();
        assert!(matches!(lang, Language::JavaScript));
    }

    // ── confirm_db_deletion ──

    #[test]
    fn test_confirm_db_deletion_confirmed() {
        let mut term = TestTerminal::new().with_inputs(vec!["my-db"]);
        assert!(confirm_db_deletion("my-db", &mut term).unwrap());
    }

    #[test]
    fn test_confirm_db_deletion_denied() {
        let mut term = TestTerminal::new().with_inputs(vec!["wrong-name"]);
        assert!(!confirm_db_deletion("my-db", &mut term).unwrap());
    }

    // ── mock server tests ──

    mod mock_server {
        use super::*;
        use chroma::client::{ChromaAuthMethod, ChromaHttpClientOptions, ChromaRetryOptions};
        use httpmock::MockServer;

        fn mock_client(server: &MockServer) -> ChromaHttpClient {
            ChromaHttpClient::new(ChromaHttpClientOptions {
                endpoint: server.base_url().parse().unwrap(),
                endpoints: Vec::new(),
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

        // ── list ──

        #[tokio::test]
        async fn test_list_shows_dbs() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["db1", "db2"]);

            let mut term = TestTerminal::new();
            list("my-profile".to_string(), &client, &mut term)
                .await
                .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("db1"));
            assert!(output.contains("db2"));
            assert!(output.contains("2"));
        }

        #[tokio::test]
        async fn test_list_empty_dbs() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &[]);

            let mut term = TestTerminal::new();
            list("my-profile".to_string(), &client, &mut term)
                .await
                .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("0 DBs"));
        }

        // ── create ──

        #[tokio::test]
        async fn test_create_db_success() {
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

            let mut term = TestTerminal::new();
            create(
                CreateArgs {
                    name: Some("new-db".to_string()),
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("new-db"));
            assert!(output.contains("created successfully"));
        }

        #[tokio::test]
        async fn test_create_db_already_exists() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["existing-db"]);

            let mut term = TestTerminal::new();
            create(
                CreateArgs {
                    name: Some("existing-db".to_string()),
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("already exists"));
        }

        #[tokio::test]
        async fn test_create_db_prompted_name() {
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

            let mut term = TestTerminal::new().with_inputs(vec!["prompted-db"]);
            create(CreateArgs { name: None }, &client, &mut term)
                .await
                .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("prompted-db"));
        }

        // ── delete ──

        #[tokio::test]
        async fn test_delete_db_confirmed() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["target-db"]);
            server.mock(|when, then| {
                when.method("DELETE")
                    .path("/api/v2/tenants/test-tenant/databases/target-db");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({}));
            });

            let mut term = TestTerminal::new().with_inputs(vec!["target-db"]);
            delete(
                DeleteArgs {
                    name: Some("target-db".to_string()),
                    force: false,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("Deleted"));
        }

        #[tokio::test]
        async fn test_delete_db_cancelled() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["target-db"]);

            let mut term = TestTerminal::new().with_inputs(vec!["wrong-name"]);
            delete(
                DeleteArgs {
                    name: Some("target-db".to_string()),
                    force: false,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("cancelled"));
        }

        #[tokio::test]
        async fn test_delete_db_force() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["target-db"]);
            server.mock(|when, then| {
                when.method("DELETE")
                    .path("/api/v2/tenants/test-tenant/databases/target-db");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({}));
            });

            let mut term = TestTerminal::new();
            delete(
                DeleteArgs {
                    name: Some("target-db".to_string()),
                    force: true,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("Deleted"));
        }

        #[tokio::test]
        async fn test_delete_db_not_found() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["other-db"]);

            let mut term = TestTerminal::new();
            let err = delete(
                DeleteArgs {
                    name: Some("missing-db".to_string()),
                    force: true,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap_err();

            assert!(matches!(err, CliError::Db(DbError::DbNotFound(_))));
        }

        // ── connect ──

        #[tokio::test]
        async fn test_connect_env_vars() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["my-db"]);

            let profile = Profile::new("test-key".to_string(), "test-tenant".to_string());
            let mut term = TestTerminal::new();
            connect(
                ConnectArgs {
                    name: Some("my-db".to_string()),
                    language: None,
                    env_file: false,
                    env_vars: true,
                },
                profile,
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains("CHROMA_API_KEY=test-key"));
            assert!(output.contains("CHROMA_TENANT=test-tenant"));
            assert!(output.contains("CHROMA_DATABASE=my-db"));
        }

        #[tokio::test]
        async fn test_connect_language_snippet() {
            // Test the snippet generation directly to avoid clipboard dependency
            // (copy_to_clipboard fails in CI without a display server)
            let snippet = Language::Python.get_connection(
                "test-tenant".to_string(),
                "my-db".to_string(),
                "test-key".to_string(),
            );
            assert!(snippet.contains("chromadb"));
            assert!(snippet.contains("test-key"));
            assert!(snippet.contains("test-tenant"));
            assert!(snippet.contains("my-db"));

            let snippet = Language::JavaScript.get_connection(
                "tenant".to_string(),
                "db".to_string(),
                "key".to_string(),
            );
            assert!(snippet.contains("CloudClient"));
            assert!(snippet.contains("key"));
        }

        #[tokio::test]
        async fn test_connect_db_not_found() {
            let server = MockServer::start();
            let client = mock_client(&server);
            mock_list_databases(&server, &["other-db"]);

            let profile = Profile::new("key".to_string(), "tenant".to_string());
            let mut term = TestTerminal::new();
            let err = connect(
                ConnectArgs {
                    name: Some("missing".to_string()),
                    language: None,
                    env_file: false,
                    env_vars: true,
                },
                profile,
                &client,
                &mut term,
            )
            .await
            .unwrap_err();

            assert!(matches!(err, CliError::Db(DbError::DbNotFound(_))));
        }
    }

    // ── integration tests ──

    mod integration {
        use super::*;
        use chroma::client::{ChromaAuthMethod, ChromaHttpClientOptions, ChromaRetryOptions};
        use uuid::Uuid;

        fn local_client() -> ChromaHttpClient {
            ChromaHttpClient::new(ChromaHttpClientOptions {
                endpoint: "http://localhost:8000".parse().unwrap(),
                endpoints: Vec::new(),
                auth_method: ChromaAuthMethod::None,
                retry_options: ChromaRetryOptions::default(),
                tenant_id: Some("default_tenant".to_string()),
                database_name: Some("default_database".to_string()),
            })
        }

        fn unique_db_name() -> String {
            format!("test_db_{}", Uuid::new_v4().to_string().replace('-', "_"))
        }

        #[tokio::test]
        async fn test_k8s_integration_db_create_and_list() {
            let client = local_client();
            let db_name = unique_db_name();
            let mut term = TestTerminal::new();

            create(
                CreateArgs {
                    name: Some(db_name.clone()),
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let mut term = TestTerminal::new();
            list("default".to_string(), &client, &mut term)
                .await
                .unwrap();
            let output = term.output.join("\n");
            assert!(output.contains(&db_name));

            // Cleanup
            client.delete_database(&db_name).await.unwrap();
        }

        #[tokio::test]
        async fn test_k8s_integration_db_create_already_exists() {
            let client = local_client();
            let db_name = unique_db_name();
            let mut term = TestTerminal::new();

            create(
                CreateArgs {
                    name: Some(db_name.clone()),
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let mut term = TestTerminal::new();
            create(
                CreateArgs {
                    name: Some(db_name.clone()),
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();
            let output = term.output.join("\n");
            assert!(output.contains("already exists"));

            // Cleanup
            client.delete_database(&db_name).await.unwrap();
        }

        #[tokio::test]
        async fn test_k8s_integration_db_delete() {
            let client = local_client();
            let db_name = unique_db_name();

            client.create_database(&db_name).await.unwrap();

            let mut term = TestTerminal::new();
            delete(
                DeleteArgs {
                    name: Some(db_name.clone()),
                    force: true,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let dbs = client.list_databases().await.unwrap();
            assert!(!dbs.iter().any(|db| db.name == db_name));
        }

        #[tokio::test]
        async fn test_k8s_integration_db_delete_not_found() {
            let client = local_client();
            let mut term = TestTerminal::new();

            let err = delete(
                DeleteArgs {
                    name: Some("nonexistent_db_12345".to_string()),
                    force: true,
                },
                &client,
                &mut term,
            )
            .await
            .unwrap_err();

            assert!(matches!(err, CliError::Db(DbError::DbNotFound(_))));
        }

        #[tokio::test]
        async fn test_k8s_integration_db_connect_env_vars() {
            let client = local_client();
            let db_name = unique_db_name();

            client.create_database(&db_name).await.unwrap();

            let profile = Profile::new("test-key".to_string(), "default_tenant".to_string());
            let mut term = TestTerminal::new();
            connect(
                ConnectArgs {
                    name: Some(db_name.clone()),
                    language: None,
                    env_file: false,
                    env_vars: true,
                },
                profile,
                &client,
                &mut term,
            )
            .await
            .unwrap();

            let output = term.output.join("\n");
            assert!(output.contains(&format!("CHROMA_DATABASE={}", db_name)));

            // Cleanup
            client.delete_database(&db_name).await.unwrap();
        }
    }
}
