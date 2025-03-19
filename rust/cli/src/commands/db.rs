use crate::utils::{get_current_profile, load_cli_env_config, Profile};
use arboard::Clipboard;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::fmt;
use chroma_types::Database;
use crate::client::ChromaClient;

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
    name: Option<String>,
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

pub fn get_chroma_client(profile: &Profile, dev: bool) -> Option<ChromaClient> {
    let cli_env_config = load_cli_env_config(dev);
    match ChromaClient::from_profile(cli_env_config.frontend_url, profile) { 
        Ok(client) => Some(client),
        Err(e) => {
            let message = format!("Failed to connect to Chroma Cloud {}", e);
            println!("{}", message.red());
            None
        }
    }
}

fn fetch_dbs(chroma_client: &ChromaClient, profile_name: &str) -> Option<Vec<Database>> {
    match chroma_client.list_databases() {
        Ok(dbs) => Some(dbs),
        Err(_) => {
            let message = format!(
                "Failed to fetch DBs for profile {}",
                profile_name
            );
            eprintln!("{}", message.red());
            None
        }
    }
}

pub fn connect(args: ConnectArgs, profile_name: String, current_profile: Profile) {
    let chroma_client = match get_chroma_client(&current_profile, args.db_args.dev) {
        Some(client) => client,
        None => return,
    };

    let dbs = match fetch_dbs(&chroma_client, &profile_name) {
        Some(dbs) => dbs,
        None => return,
    };

    let prompt = "Which DB would you like to connect to?";
    let name = if dbs.len() < 5 {
        println!(
            "{}",
            prompt.blue().bold()
        );
        let db_names: Vec<String> = dbs.iter().map(|db| db.name.clone()).collect();
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&db_names)
            .default(0)
            .interact()
            .unwrap();
        db_names[selection].clone()
    } else {
        args.name.unwrap_or_else(|| prompt_db_name(prompt))
    };

    if !dbs.iter().any(|db| db.name == name) {
        let message = format!("\nDB {} not found", name);
        eprintln!("{}", message.red());
        return;
    }

    let language = args.language.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Python"),
            format!("{} {}", ">".yellow(), "JavaScript/Typescript"),
        ];

        println!(
            "\n{}",
            "Which language would you like to use?".blue().bold()
        );
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
            chroma_client.api_url,
            current_profile.team_id,
            name,
            current_profile.api_key,
        ),
        Language::JavaScript => get_js_connection(
            chroma_client.api_url,
            current_profile.team_id,
            name,
            current_profile.api_key,
        ),
    };

    println!("{}", connection_string);
    let mut clipboard = Clipboard::new().expect("Failed to create clipboard");
    clipboard
        .set_text(connection_string)
        .expect("Failed to copy text");
    println!("\n{}", "Copied to clipboard!".blue().bold());
}

pub fn create(args: CreateArgs, profile_name: String, current_profile: Profile) {
    let chroma_client = match get_chroma_client(&current_profile, args.db_args.dev) {
        Some(client) => client,
        None => return,
    };
    
    println!();
    let name = args
        .name
        .unwrap_or_else(|| prompt_db_name("What is the name of your new DB?"));

    let dbs = match fetch_dbs(&chroma_client, &profile_name) {
        Some(dbs) => dbs,
        None => return,
    };

    if dbs.iter().any(|db| db.name == name) {
        let message = format!("DB with name {} already exists!", name);
        eprintln!("{}", message.red());
        eprintln!(
            "If you want to delete it, use: {} {}\n",
            "chroma db delete".yellow(),
            name.yellow()
        );
        return;
    }

    println!(
        "\n{} {}...",
        "Creating database".bold().blue(),
        name.bold().blue()
    );

    if chroma_client.create_database(name.clone()).is_err() {
        let message = format!("Failed to create database {}", name);
        eprintln!("{}", message.red());
        return;
    };

    let success_message = format!("\nDatabase {} created successfully!", name);
    let instructions = format!(
        "To get a connection string, run:\n   {} {}",
        "chroma db connect".yellow(),
        name.yellow()
    );
    println!("{}", success_message.green());
    println!("{}", instructions);
}

pub fn delete(args: DeleteArgs, profile_name: String, current_profile: Profile) {
    let chroma_client = match get_chroma_client(&current_profile, args.db_args.dev) {
        Some(client) => client,
        None => return,
    };

    let name = args
        .name
        .unwrap_or_else(|| prompt_db_name("What is the name of the DB you want to delete?"));

    let dbs = match fetch_dbs(&chroma_client, &profile_name) {
        Some(dbs) => dbs,
        None => return,
    };

    if !dbs.iter().any(|db| db.name == name) {
        let message = format!("\nDB {} not found", name);
        eprintln!("{}", message.red());
        return;
    }

    println!("{}", "\nAre you sure you want to delete this DB?\nThis action cannot be reverted and you will lose all the data in this DB.\n\nIf you want to continue please type the name of DB to confirm.".red().bold());
    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();

    if confirm == name {
        match chroma_client.delete_database(name.clone()) {
            Ok(_) => {}
            Err(_) => {
                let message = format!("\nFailed to delete DB {}", name);
                eprintln!("{}", message.red());
            }
        }
        println!("\nDeleted DB {} successfully!", name);
    } else {
        println!(
            "\n{} '{}' {} '{}'",
            "DB deletion cancelled. Confirmation input".yellow(),
            confirm.yellow(),
            "did not match DB name to delete: ".yellow(),
            name.yellow()
        );
    }
}

pub fn list(args: ListArgs, profile_name: String, current_profile: Profile) {
    let chroma_client = match get_chroma_client(&current_profile, args.db_args.dev) {
        Some(client) => client,
        None => return,
    };

    let dbs = match chroma_client.list_databases() {
        Ok(dbs) => dbs,
        Err(_) => {
            let message = format!(
                "Failed to fetch DBs for profile {}",
                profile_name
            );
            eprintln!("Failed to fetch DBs for profile {}", message.red());
            return;
        }
    };

    if dbs.is_empty() {
        println!(
            "Profile {} has 0 DBs. To create a new Chroma DB use: {}",
            profile_name,
            "chroma db create <db name>".yellow()
        );
        return;
    }

    println!(
        "{} {} {}",
        "Listing".blue().bold(),
        dbs.len().to_string().blue().bold(),
        "databases".blue().bold()
    );
    for db in dbs {
        println!("{} {}", ">".yellow(), db.name);
    }
}

pub fn db_command(command: DbCommand) {
    let (profile_name, current_profile) = match get_current_profile() {
        Ok((profile_name, current_profile)) => (profile_name, current_profile),
        Err(_) => {
            eprintln!("{}", "No current profile found.".red().bold());
            eprintln!("To set a new profile use: {}", "chroma login".yellow());
            return;
        }
    };

    match command {
        DbCommand::Connect(args) => connect(args, profile_name, current_profile),
        DbCommand::Create(args) => create(args, profile_name, current_profile),
        DbCommand::Delete(args) => delete(args, profile_name, current_profile),
        DbCommand::List(args) => list(args, profile_name, current_profile),
    }
}