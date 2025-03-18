use std::error::Error;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::fmt;
use crate::utils::{get_current_profile, load_cli_env_config, Profile};
use arboard::Clipboard;
use serde::de::DeserializeOwned;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Serialize;
use crate::client::{create_database, delete_database, list_databases};

#[derive(Debug, Clone, ValueEnum)]
pub enum Language {
    Python,
    JavaScript,
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Language::Python => write!(f, "python"),
            Language::JavaScript => write!(f, "javascript"),
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
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ConnectArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    name: String,
    language: Option<Language>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[clap(flatten)]
    db_args: DbArgs,
}

#[derive(Subcommand, Debug)]
pub enum DbCommand {
    Connect(ConnectArgs),
    Create(CreateArgs),
    Delete(DeleteArgs),
    List(ListArgs),
}

fn chroma_server_get_request<T: DeserializeOwned>(
    api_url: &str,
    route: &str,
    api_key: &str,
) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", api_url, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    let response = client.get(url).headers(headers).send()?.json::<T>()?;

    Ok(response)
}

fn chroma_server_delete_request(api_url: &str, route: &str, api_key: &str) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", api_url, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    let _response = client.delete(url).headers(headers).send();

    Ok(())
}

pub fn chroma_server_post_request<T: DeserializeOwned, U: Serialize>(
    api_url: &str,
    route: &str,
    api_key: &str,
    body: &U,
) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", api_url, route);
    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    let response = client.post(&url).headers(headers).json(body).send()?;
    let response_text = response.text()?;
    let deserialized: T = serde_json::from_str(&response_text)?;
    Ok(deserialized)
}

pub fn get_python_connection(
    url: String,
    tenant_id: String,
    db_name: String,
    api_key: String,
) -> String {
    format!(
        "
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

pub fn get_js_connection(
    url: String,
    tenant_id: String,
    db_name: String,
    api_key: String,
) -> String {
    format!(
        "
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

pub fn prompt_db_name(prompt: &str) -> String {
    println!("{}", prompt.blue().bold());
    Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap()
}

pub fn connect(args: ConnectArgs, current_profile: Profile) {
    let cli_env_config = load_cli_env_config(args.db_args.dev);
    
    let language = args.language.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Python"),
            format!("{} {}", ">".yellow(), "JavaScript/Typescript"),
        ];

        println!("{}", "\nWhich language would you like to use?".blue().bold());
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&options)
            .default(0)
            .interact()
            .unwrap();

        if selection == 0 {
            Language::Python
        } else {
            Language::JavaScript
        }
    });
    println!("{}", language.to_string().green());

    let connection_string = match language {
        Language::Python => get_python_connection(
            cli_env_config.frontend_url.to_string(),
            current_profile.team_id,
            args.name,
            current_profile.api_key,
        ),
        Language::JavaScript => get_js_connection(
            cli_env_config.frontend_url.to_string(),
            current_profile.team_id,
            args.name,
            current_profile.api_key,
        ),
    };

    println!("{}", connection_string);
    let mut clipboard = Clipboard::new().expect("Failed to create clipboard");
    clipboard
        .set_text(connection_string)
        .expect("Failed to copy text");
    println!("\n{}\n", "Copied to clipboard!".blue().bold());
}

pub fn create(args: CreateArgs, current_profile: Profile) {
    let cli_env_config = load_cli_env_config(args.db_args.dev);
    
    let name = args
        .name
        .unwrap_or_else(|| prompt_db_name("What is the name of your new DB?"));

    let dbs = match list_databases(cli_env_config.frontend_url, &current_profile) {
        Ok(dbs) => dbs,
        Err(_) => {
            let message = format!("\nFailed to fetch DBs for profile {}\n", current_profile.name);
            eprintln!("{}", message.red());
            return;
        }
    };

    if dbs.iter().any(|db| db.name == name) {
        let message = format!("\nDB with name {} already exists!\n", name);
        eprintln!("{}", message.red());
        eprintln!("If you want to delete it, use: {} {}", "chroma delete ".yellow(), name.yellow());
        return;
    }
    
    println!(
        "\n{} {}...",
        "Creating database".bold().blue(),
        name.bold().blue()
    );
    
    if create_database(cli_env_config.frontend_url, &current_profile, name.clone()).is_err() {
        let message = format!("\nFailed to create database {}\n", name);
        eprintln!("{}", message.red());
        return;
    };
    
    println!("Database {} created successfully!", name);
    println!(
        "To get a connection string, run:\n   chroma db connect --name {}\n",
        name
    )
}

pub fn delete(args: DeleteArgs, current_profile: Profile) {
    let cli_env_config = load_cli_env_config(args.db_args.dev);
    
    let name = args
        .name
        .unwrap_or_else(|| prompt_db_name("What is the name of the DB you want to delete?"));

    let dbs = match list_databases(cli_env_config.frontend_url, &current_profile) {
        Ok(dbs) => dbs,
        Err(_) => {
            let message = format!("\nFailed to fetch DBs for profile {}\n", current_profile.name);
            eprintln!("{}", message.red());
            return;
        }
    };
    
    if !dbs.iter().any(|db| db.name == name) {
        let message = format!("\nDB {} not found\n", name);
        eprintln!("{}", message.red());
        return;
    }

    println!("{}", "\nAre you sure you want to delete this DB? This action cannot be reverted and you will lose all the data in this DB. If you want to continue please type the name of DB to confirm.".red().bold());
    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();

    if confirm == name {
        match delete_database(cli_env_config.frontend_url, &current_profile, name.clone()) {
            Ok(_) => {},
            Err(_) => {
                let message = format!("\nFailed to delete DB {}\n", name);
                eprintln!("{}", message.red());
            }
        }
        println!("\nDeleted DB {} successfully!\n", name);
    } else {
        println!(
            "\n{} {} {} {}\n",
            "DB deletion cancelled. Confirmation input".yellow(),
            confirm.yellow(),
            "did not match DB name to delete: ".yellow(),
            name.yellow()
        );
    }
}

pub fn list(args: ListArgs, current_profile: Profile) {
    let cli_env_config = load_cli_env_config(args.db_args.dev);
    
    let dbs = match list_databases(cli_env_config.frontend_url, &current_profile) {
        Ok(dbs) => dbs,
        Err(_) => {
            let message = format!("\nFailed to fetch DBs for profile {}\n", current_profile.name);
            eprintln!("Failed to fetch DBs for profile {}", message.red());
            return;
        }
    };
    
    if dbs.is_empty() {
        println!("\nProfile {} has 0 DBs. To create a new Chroma DB use: {}\n", current_profile.name, "chroma db create <db name>".yellow());
        return;
    }
    
    println!(
        "\n{} {} {}",
        "Listing".blue().bold(),
        dbs.len().to_string().blue().bold(),
        "databases".blue().bold()
    );
    for db in dbs {
        println!("{} {}", ">".yellow(), db.name);
    }
    println!();
}

pub fn db_command(command: DbCommand) {
    let current_profile = match get_current_profile() {
        Ok(profile) => profile,
        Err(_) => {
            eprintln!("\n{}", "No current profile found.".red().bold());
            eprintln!("To set a new profile use: {}\n", "chroma login".yellow());
            return;
        }
    };
    
    match command {
        DbCommand::Connect(args) => connect(args, current_profile),
        DbCommand::Create(args) => create(args, current_profile),
        DbCommand::Delete(args) => delete(args, current_profile),
        DbCommand::List(args) => list(args, current_profile),
    }
}