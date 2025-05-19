use crate::client::admin_client::get_admin_client;
use crate::ui_utils::copy_to_clipboard;
use crate::utils::{get_current_profile, CliError, Profile, UtilsError, SELECTION_LIMIT};
use chroma_types::Database;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::fmt;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("")]
    NoDBs,
    #[error("Database name cannot be empty")]
    EmptyDbName,
    #[error("Database name must contain only alphanumeric characters, hyphens, or underscores")]
    InvalidDbName,
    #[error("DB {0} not found")]
    DbNotFound(String),
    #[error("Failed to get runtime for DB commands")]
    RuntimeError,
}

#[derive(Debug, Clone, ValueEnum, EnumIter)]
pub enum Language {
    Python,
    JavaScript,
    TypeScript,
}

impl Language {
    fn get_connection(
        &self,
        url: String,
        tenant_id: String,
        db_name: String,
        api_key: String,
    ) -> String {
        match self {
            Language::Python => get_python_connection(url, tenant_id, db_name, api_key),
            Language::JavaScript => get_js_connection(url, tenant_id, db_name, api_key),
            Language::TypeScript => get_js_connection(url, tenant_id, db_name, api_key),
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
pub struct DbArgs {
    #[clap(long, hide = true, help = "Flag to use during development")]
    dev: bool,
}

#[derive(Args, Debug)]
pub struct CreateArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    #[clap(index = 1, help = "The name of the DB to create")]
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(flatten)]
    db_args: DbArgs,
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
    #[clap(flatten)]
    db_args: DbArgs,
    #[clap(index = 1, help = "The name of the DB to get a connection snippet for")]
    name: Option<String>,
    #[clap(
        long,
        help = "The programming language to use for the connection snippet"
    )]
    language: Option<Language>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[clap(flatten)]
    db_args: DbArgs,
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
    List(ListArgs),
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

fn get_python_connection(
    url: String,
    tenant_id: String,
    db_name: String,
    api_key: String,
) -> String {
    format!(
        "Python connection snippet:
    import chromadb
    client = chromadb.HttpClient(
        ssl=True,
        host={},
        tenant='{}',
        database='{}',
        headers={{
            'x-chroma-token': '{}'
        }}
    )",
        url, tenant_id, db_name, api_key
    )
}

fn get_js_connection(url: String, tenant_id: String, db_name: String, api_key: String) -> String {
    format!(
        "Javascript/Typescript connection snippet:
    import {{ ChromaClient }} from 'chromadb';
    const client = new ChromaClient({{
        path: '{}',
        auth: {{ provider: 'token', credentials: '{}', tokenHeaderType: 'X_CHROMA_TOKEN' }},
        tenant: '{}',
        database: '{}'
    }});
",
        url, api_key, tenant_id, db_name
    )
}

fn prompt_db_name() -> Result<String, CliError> {
    let input = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .map_err(|_| UtilsError::UserInputFailed)?;
    Ok(input)
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

fn select_db(dbs: &[Database]) -> Result<String, CliError> {
    let db_names: Vec<String> = dbs.iter().map(|db| db.name.clone()).collect();
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&db_names)
        .default(0)
        .interact()
        .map_err(|_| UtilsError::UserInputFailed)?;
    let name = db_names[selection].clone();
    println!("{}\n", name);
    Ok(name)
}

pub fn get_db_name(dbs: &[Database], prompt: &str) -> Result<String, CliError> {
    if dbs.is_empty() {
        return Err(CliError::Db(DbError::NoDBs));
    }

    println!("{}", prompt.blue().bold());
    let name = match dbs.len() {
        0..=SELECTION_LIMIT => select_db(dbs),
        _ => prompt_db_name(),
    }?;

    validate_db_name(name.as_str())
}

fn select_language() -> Result<Language, CliError> {
    let languages: Vec<Language> = Language::iter().collect();
    let options: Vec<String> = languages
        .iter()
        .map(|language| format!("{} {}", ">".yellow(), capitalize(&language.to_string())))
        .collect();

    println!("{}", select_language_message().blue().bold());
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&options)
        .default(0)
        .interact()
        .map_err(|_| CliError::Utils(UtilsError::UserInputFailed))?;

    let language = languages[selection].clone();
    println!("{}", language.to_string().green());

    Ok(language)
}

fn confirm_db_deletion(name: &str) -> Result<bool, CliError> {
    println!("{}", db_delete_confirm_message());
    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .map_err(|_| UtilsError::UserInputFailed)?;
    Ok(confirm.eq(name))
}

pub async fn connect(args: ConnectArgs, current_profile: Profile) -> Result<(), CliError> {
    let admin_client = get_admin_client(Some(&current_profile), args.db_args.dev);
    let dbs = admin_client.list_databases().await?;

    let name = match args.name {
        Some(name) => validate_db_name(&name),
        None => get_db_name(&dbs, &connect_db_name_prompt()),
    }?;

    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(DbError::DbNotFound(name)));
    }

    let language = match args.language {
        Some(language) => language,
        None => select_language()?,
    };

    let connection_string = language.get_connection(
        admin_client.host,
        current_profile.tenant_id,
        name,
        admin_client.api_key.unwrap_or("".to_string()),
    );
    println!("{}", connection_string);

    copy_to_clipboard(&connection_string)?;

    Ok(())
}

pub async fn create(args: CreateArgs, current_profile: Profile) -> Result<(), CliError> {
    let admin_client = get_admin_client(Some(&current_profile), args.db_args.dev);
    let dbs = admin_client.list_databases().await?;

    let mut name = match args.name {
        Some(name) => name,
        None => {
            println!("{}", create_db_name_prompt());
            prompt_db_name()?
        }
    };
    name = validate_db_name(&name)?;

    if dbs.iter().any(|db| db.name == name) {
        println!("{}", create_db_already_exists_message(&name));
        return Ok(());
    }

    println!("{}", creating_db_message(&name));

    admin_client.create_database(name.clone()).await?;

    println!("{}", create_db_success_message(&name));

    Ok(())
}

pub async fn delete(args: DeleteArgs, current_profile: Profile) -> Result<(), CliError> {
    let admin_client = get_admin_client(Some(&current_profile), args.db_args.dev);
    let dbs = admin_client.list_databases().await?;

    let name = match args.name {
        Some(name) => validate_db_name(&name),
        None => get_db_name(&dbs, &delete_db_name_prompt()),
    }?;

    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(DbError::DbNotFound(name)));
    }

    if args.force || confirm_db_deletion(&name)? {
        admin_client.delete_database(name.clone()).await?;
        println!("{}", db_delete_success_message(&name));
    } else {
        println!("{}", db_delete_cancelled())
    }

    Ok(())
}

pub async fn list(
    args: ListArgs,
    profile_name: String,
    current_profile: Profile,
) -> Result<(), CliError> {
    let admin_client = get_admin_client(Some(&current_profile), args.db_args.dev);
    let dbs = admin_client.list_databases().await?;

    if dbs.is_empty() {
        println!("{}", no_dbs_message(&profile_name));
        return Ok(());
    }

    println!("{}", list_dbs_message(&dbs));
    for db in dbs {
        println!("{} {}", ">".yellow(), db.name);
    }

    Ok(())
}

pub fn db_command(command: DbCommand) -> Result<(), CliError> {
    let (profile_name, current_profile) = get_current_profile()?;

    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        match command {
            DbCommand::Connect(args) => connect(args, current_profile).await,
            DbCommand::Create(args) => create(args, current_profile).await,
            DbCommand::Delete(args) => delete(args, current_profile).await,
            DbCommand::List(args) => list(args, profile_name, current_profile).await,
        }
    })?;
    Ok(())
}
