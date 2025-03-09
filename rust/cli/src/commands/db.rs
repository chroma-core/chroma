use std::fmt;
use arboard::Clipboard;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;
use crate::client::{create_database, delete_database, list_databases, CHROMA_API_URL};
use crate::commands::install::DbType;
use crate::utils::{get_current_profile, Profile};

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
pub struct CreateArgs {
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ConnectArgs {
    name: String,
    language: Option<Language>,
}

#[derive(Subcommand, Debug)]
pub enum DbCommand {
    Connect(ConnectArgs),
    Create(CreateArgs),
    Delete(DeleteArgs),
    List
}

pub fn get_python_connection(url: String, tenant_id: String, db_name: String, api_key: String) -> String {
    format!("
    import chromadb
    client = chromadb.HttpClient(
        ssl=True,
        host={},
        tenant='{}',
        database='{}',
        headers={{
            'x-chroma-token': '{}'
        }}
    )", url, tenant_id, db_name, api_key)
}

pub fn get_js_connection(url: String, tenant_id: String, db_name: String, api_key: String) -> String {
    format!("
    import {{ ChromaClient }} from 'chromadb';
    const client = new ChromaClient({{
        path: '{}',
        auth: {{ provider: 'token', credentials: '{}', tokenHeaderType: 'X_CHROMA_TOKEN' }},
        tenant: '{}',
        database: '{}'
    }});
", url, api_key, tenant_id, db_name)
}

pub fn prompt_db_name(prompt: &str) -> String {
    println!("{}", prompt.blue().bold());
    Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap()
}

pub fn connect(args: ConnectArgs, current_profile: Profile) {
    let language = args.language.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Python"),
            format!("{} {}", ">".yellow(), "JavaScript/Typescript"),
        ];

        println!("{}", "\nWhat language would you like to use?".blue().bold());
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
        Language::Python => {
            get_python_connection(CHROMA_API_URL.to_string(), current_profile.tenant_id, args.name, current_profile.api_key)
        }
        Language::JavaScript => {
            get_js_connection(CHROMA_API_URL.to_string(), current_profile.tenant_id, args.name, current_profile.api_key)
        }
    };
    
    println!("{}", connection_string);
    let mut clipboard = Clipboard::new().expect("Failed to create clipboard");
    clipboard.set_text(connection_string).expect("Failed to copy text");
    println!("\n{}\n", "Copied to clipboard!".blue().bold());
}

pub fn create(args: CreateArgs, current_profile: Profile) {
    let name = args.name.unwrap_or_else(|| {
        prompt_db_name("What is the name of your new DB?")
    });
    println!("\n{} {}...", "Creating database".bold().blue(), name.bold().blue());
    create_database(&current_profile, name.clone()).expect("Failed to create database");
    println!("Database {} created successfully!", name);
    println!("To get a connection string, run:\n\tchroma db connect --name {}\n", name)
}

pub fn delete(args: DeleteArgs, current_profile: Profile) {
    let name = args.name.unwrap_or_else(|| {
        prompt_db_name("What is the name of the DB you want to delete?")
    });
    
    println!("{}", "\nAre you sure you want to delete this DB? This action cannot be reverted and you will lose all the data in this DB. If you want to continue please type the name of DB to confirm.".red().bold());
    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();
    
    if confirm == name {
        delete_database(&current_profile, name.clone()).expect("Failed to delete database");
        println!("\nDeleted DB {} successfully!\n", name);
    } else {
        println!("\n{} {} {} {}\n", "DB deletion cancelled. Confirmation input".yellow(), confirm.yellow(), "did not match DB name to delete: ".yellow(), name.yellow());
    }
}

pub fn list(current_profile: Profile) {
    let dbs = list_databases(&current_profile).expect("Failed to list databases");
    println!("\n{} {} {}", "Listing".blue().bold(), dbs.len().to_string().blue().bold(), "databases".blue().bold());
    for db in dbs {
        println!("{} {}", ">".yellow(), db.name);
    }
    println!();
}

pub fn db_command(command: DbCommand) {
    let current_profile = get_current_profile();
    match command { 
        DbCommand::Connect(args) => {
            connect(args, current_profile)
        }
        DbCommand::Create(args) => {
            create(args, current_profile)
        }
        DbCommand::Delete(args) => {
            delete(args, current_profile)
        }
        DbCommand::List => {
            list(current_profile)
        }
    }
}
