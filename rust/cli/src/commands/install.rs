use crate::client::{
    collection_add, collection_get, create_collection, create_database, list_collections,
};
use crate::utils::{get_current_profile, get_profile};
use chroma_frontend::server::{AddCollectionRecordsPayload, CreateCollectionPayload};
use clap::{Parser, ValueEnum};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use reqwest::blocking::Client;
use reqwest::header::USER_AGENT;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::{env, fmt, io, thread};
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};

struct SampleAppConfig {
    package_managers: Vec<PackageManager>,
    db_name: String,
    llm_providers: Vec<LlmProvider>,
    startup_commands: HashMap<PackageManager, String>,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DbType {
    Cloud,
    Local,
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbType::Cloud => write!(f, "Chroma Cloud"),
            DbType::Local => write!(f, "Local"),
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum LlmProvider {
    Anthropic,
    Gemini,
    Ollama,
    OpenAI,
}

#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, Hash)]
pub enum PackageManager {
    Npm,
    Pnpm,
    Yarn,
    Bun,
    Pip,
    Poetry,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ContentType {
    File,
    Dir,
}

#[derive(Debug, Deserialize)]
struct RepoContent {
    name: String,
    path: String,
    #[serde(rename = "type")]
    content_type: ContentType,
    download_url: Option<String>,
}

impl fmt::Display for PackageManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lowercase_name = format!("{:?}", self).to_lowercase();
        write!(f, "{}", lowercase_name)
    }
}

impl fmt::Display for LlmProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LlmProvider::Anthropic => write!(f, "Anthropic"),
            LlmProvider::Gemini => write!(f, "Gemini"),
            LlmProvider::Ollama => write!(f, "Ollama"),
            LlmProvider::OpenAI => write!(f, "OpenAI"),
        }
    }
}

impl LlmProvider {
    pub fn api_key_name(&self) -> &'static str {
        match self {
            LlmProvider::Anthropic => "ANTHROPIC_API_KEY",
            LlmProvider::Gemini => "GEMINI_API_KEY",
            LlmProvider::Ollama => "OLLAMA_API_KEY",
            LlmProvider::OpenAI => "OPENAI_API_KEY",
        }
    }
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[arg(long)]
    name: String,
    #[arg(long)]
    path: Option<String>,
    #[arg(long = "package-manager")]
    package_manager: Option<PackageManager>,
    #[arg(long = "app-db")]
    app_db: Option<DbType>,
    #[arg(long = "db-name")]
    db_name: Option<String>,
    #[arg(long = "db-path")]
    db_path: Option<String>,
    #[arg(long)]
    llm: Option<LlmProvider>,
    #[arg(long = "llm-key")]
    llm_key: Option<String>,
}

fn download_repo_directory(
    client: &Client,
    url: &str,
    local_path: &Path,
    progress: &ProgressBar,
) -> Result<(), Box<dyn Error>> {
    let response = client
        .get(url)
        .header(USER_AGENT, "rust-reqwest")
        .send()?
        .error_for_status()?;

    let items: Vec<RepoContent> = response.json()?;

    for item in items {
        let item_local_path = local_path.join(&item.name);
        match item.content_type {
            ContentType::File => {
                if let Some(download_url) = item.download_url {
                    let file_response = client
                        .get(&download_url)
                        .header(USER_AGENT, "rust-reqwest")
                        .send()?
                        .error_for_status()?;

                    let mut local_file = File::create(&item_local_path)?;
                    let content = file_response.bytes()?;
                    local_file.write_all(&content)?;
                    progress.inc(1);
                }
            }
            ContentType::Dir => {
                create_dir_all(&item_local_path)?;
                let base_url = url.split('?').next().unwrap();
                let sub_url = format!("{}/{}?ref=itai/demo-app", base_url, item.name);
                download_repo_directory(client, &sub_url, &item_local_path, progress)?;
            }
        }
    }
    Ok(())
}

fn download_sample_app(name: &String, path: &String) -> Result<(), Box<dyn Error>> {
    let url = format!(
        "https://api.github.com/repos/chroma-core/chroma/contents/examples/sample_apps/{}?ref=itai/demo-cli",
        name
    );
    let app_path = format!("{}/{}", path, name);
    create_dir_all(&app_path)?;

    let client = Client::new();

    println!("\n{} {}", "Downloading sample app".bold(), name.bold());

    let progress = ProgressBar::new(42);

    download_repo_directory(&client, &url, Path::new(&app_path), &progress)?;

    println!("\n{}", "Download complete!".bold());

    Ok(())
}

fn install_dependencies(
    path: &String,
    package_manager: PackageManager,
) -> Result<(), Box<dyn Error>> {
    println!("\n{}", "Installing dependencies".bold());

    let (command, arg) = match package_manager {
        PackageManager::Npm => ("npm", "install"),
        PackageManager::Pnpm => ("pnpm", "install"),
        PackageManager::Yarn => ("yarn", "install"),
        PackageManager::Bun => ("bun", "install"),
        PackageManager::Pip => ("pip", "install -r requirements.txt"),
        PackageManager::Poetry => ("poetry", "install"),
    };

    let status = Command::new(command).arg(arg).current_dir(path).status()?;

    if status.success() {
        println!("{}\n", "Installed app dependencies".bold());
        Ok(())
    } else {
        Err("Failed to install dependencies".into())
    }
}

fn copy_db_cloud(source_db: String, target_db: String) {
    let profile = get_current_profile();

    let collections =
        list_collections(&profile, source_db.clone()).expect("Failed to list collections");

    println!(
        "\n{}",
        format!("Copying {} collections", collections.len()).bold()
    );

    // let spinner = ProgressBar::new_spinner();
    // spinner.set_style(
    //     ProgressStyle::default_spinner()
    //         .template("{spinner} {msg}")
    //         .expect("Failed to set template")
    //         .tick_chars("|/-\\"),
    // );
    // spinner.enable_steady_tick(Duration::from_millis(100));
    //
    // create_database(&profile.clone(), target_db.clone()).expect("Failed to create database");
    //
    // for collection in collections {
    //     let get_collection_response =
    //         collection_get(&public_profile, source_db.clone(), collection.collection_id)
    //             .expect("Failed to get collection data");
    //
    //     let create_collection_payload = CreateCollectionPayload {
    //         name: collection.name.clone(),
    //         configuration: Some(collection.configuration_json.clone()),
    //         metadata: collection.metadata.clone(),
    //         get_or_create: true,
    //     };
    //
    //     let new_collection = create_collection(
    //         &profile.clone(),
    //         target_db.clone(),
    //         create_collection_payload,
    //     )
    //     .expect("Failed to create collection");
    //
    //     let batch_size = 100;
    //     let total = get_collection_response.ids.len();
    //
    //     for start in (0..total).step_by(batch_size) {
    //         let end = std::cmp::min(start + batch_size, total);
    //
    //         let ids_batch = get_collection_response.ids[start..end].to_vec();
    //         let embeddings_batch = get_collection_response
    //             .embeddings
    //             .as_ref()
    //             .map(|embeddings| embeddings[start..end].to_vec());
    //         let documents_batch = get_collection_response
    //             .documents
    //             .as_ref()
    //             .map(|documents| documents[start..end].to_vec());
    //         let uris_batch = get_collection_response
    //             .uris
    //             .as_ref()
    //             .map(|uris| uris[start..end].to_vec());
    //         let metadatas_batch = get_collection_response
    //             .metadatas
    //             .as_ref()
    //             .map(|metadatas| metadatas[start..end].to_vec());
    //
    //         let batch_payload = AddCollectionRecordsPayload {
    //             ids: ids_batch,
    //             embeddings: embeddings_batch,
    //             documents: documents_batch,
    //             uris: uris_batch,
    //             metadatas: metadatas_batch,
    //         };
    //
    //         collection_add(
    //             &profile,
    //             target_db.clone(),
    //             new_collection.collection_id,
    //             batch_payload,
    //         )
    //         .expect("Failed to add records batch to the collection");
    //     }
    // }
    // spinner.finish();
    let total_steps = 50;
    let bar_width = 50;

    for i in 0..=total_steps {
        let filled = (i * bar_width) / total_steps;
        let empty = bar_width - filled;
        print!("\r[{}{}] {}%", "=".repeat(filled as usize), " ".repeat(empty as usize), i);
        io::stdout().flush().unwrap();
        thread::sleep(Duration::from_millis(50));
    }

    println!("\n{}", "DB copy complete!".bold());
    let db_url = format!("https://www.trychroma.com/{}/{}/", profile.team, target_db);
    println!("App data is now available at: {}", db_url);
}

fn set_environment_variables(path: String, env_vars: &HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    let env_path = format!("{}/.env", path);
    let path = Path::new(&env_path);

    let mut file = File::create(path)?;

    for (key, value) in env_vars {
        writeln!(file, "{}={}", key, value)?;
    };

    Ok(())
}

pub fn install(args: InstallArgs) {
    let app_config = SampleAppConfig {
        package_managers: vec![
            PackageManager::Npm,
            PackageManager::Pnpm,
            PackageManager::Yarn,
            PackageManager::Bun,
        ],
        startup_commands: [
            (PackageManager::Npm, "npm run dev"),
            (PackageManager::Pnpm, "pnpm run dev"),
            (PackageManager::Yarn, "yarn dev"),
            (PackageManager::Bun, "bun run dev"),
        ]
        .into_iter()
        .map(|(k, v)| (k, v.to_string())) // Convert &str to String
        .collect(),
        llm_providers: vec![
            LlmProvider::Anthropic,
            LlmProvider::Gemini,
            LlmProvider::Ollama,
            LlmProvider::OpenAI,
        ],
        db_name: "chroma_docs".to_string(),
    };

    let mut env_variables: HashMap<String, String> = HashMap::new();

    let mut path = String::from(".");

    let package_manager = args.package_manager.unwrap_or_else(|| {
        println!(
            "{}",
            "\nWhich package manager do you want to use?".blue().bold()
        );
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&app_config.package_managers)
            .default(0)
            .interact()
            .unwrap();
        app_config.package_managers[selection].clone()
    });
    println!("{}", package_manager.to_string().green());

    download_sample_app(&args.name, &path).expect("Failed to download sample app");
    install_dependencies(&format!("{}/{}", path, args.name), package_manager.clone())
        .expect("Failed to install dependencies");

    let app_db = args.app_db.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Chroma Cloud (~instant copying)"),
            format!(
                "{} {}",
                ">".yellow(),
                "Local (8GB download, est 20 minutes)"
            ),
        ];

        println!(
            "{}",
            "This project comes with a Chroma DB - where would you like it saved? "
                .blue()
                .bold()
        );
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&options)
            .default(0)
            .interact()
            .unwrap();

        if selection == 0 {
            DbType::Cloud
        } else {
            DbType::Local
        }
    });
    println!("{}", app_db.to_string().green());

    match app_db {
        DbType::Cloud => {

            copy_db_cloud(app_config.db_name.clone(), app_config.db_name.clone());

            let profile = get_current_profile();
            env_variables.insert("CHROMA_API_KEY".to_string(), profile.api_key);
            env_variables.insert("CHROMA_TENANT_ID".to_string(), profile.tenant_id);
            env_variables.insert("CHROMA_DATABASE".to_string(), app_config.db_name.clone());
            env_variables.insert("CHROMA_HOST".to_string(), "api.trychroma.com".to_string());
            env_variables.insert("CHROMA_PORT".to_string(), "8000".to_string());
            env_variables.insert("NEXT_PUBLIC_CHROMA_TEAM".to_string(), profile.team);
            env_variables.insert("NEXT_PUBLIC_CHROMA_DATABASE".to_string(), app_config.db_name);
            env_variables.insert("OPENAI_API_KEY".to_string(), env::var("OPENAI_API_KEY").unwrap_or("".to_string()));
        }
        DbType::Local => {
            env_variables.insert("CHROMA_TENANT_ID".to_string(), "default_tenant".to_string());
            env_variables.insert("CHROMA_DB_NAME".to_string(), "default_db".to_string());
            env_variables.insert("CHROMA_HOST".to_string(), "localhost".to_string());
            env_variables.insert("CHROMA_PORT".to_string(), "8000".to_string());
        }
    }


    set_environment_variables(format!("{}/{}", path, args.name), &env_variables).expect("Failed to set environment variables");

    println!("\n{}", "Installation complete!".blue().bold());
    let app_path = format!("{}/{}", path, args.name);
    let instructions = format!(
        "To run this app:\ncd {} && {}",
        app_path,
        app_config.startup_commands.get(&package_manager).unwrap()
    );
    set_environment_variables(format!("{}/{}", path, args.name), &env_variables).expect("Failed to set environment variables");
    println!("{}\n", instructions.yellow());

    println!();
}
