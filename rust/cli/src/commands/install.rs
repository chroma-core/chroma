use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use clap::Parser;
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use reqwest::header::USER_AGENT;
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zip_extract::extract;
use crate::utils::{read_config, CliError, UtilsError, SELECTION_LIMIT};

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Failed to install sample app {0}")]
    InstallFailed(String),
    #[error("Failed to download files from Github")]
    GithubDownloadFailed,
    #[error("No such app {0}")]
    NoSuchApp(String),
    #[error("Sample app {0} requires Chroma CLI with version {1}. Please update your CLI using `chroma update`")]
    VersionMismatch(String, String),
    #[error("Failed to get sample apps listings")]
    ListingsDownloadFailed,
    #[error("Failed to list sample apps")]
    ListingFailed,
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[clap(
        index = 1,
        help = "The name of the sample app to install",
    )]
    name: Option<String>,
    #[clap(long, conflicts_with_all = ["name"])]
    list: bool,
    #[clap(long, hide = true)]
    dev: Option<String>,
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

#[derive(Debug, Deserialize)]
struct AppListing {
    name: String,
    description: String,
    version: String,
    cli_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SampleAppConfig {
    required_env_variables: Vec<String>,
    optional_env_variables: Vec<String>,
    startup_commands: HashMap<String, String>,
}

fn show_apps_message() -> String {
    "Available sample apps:".to_string()
}

fn prompt_app_name_message() -> String {
    "Which sample app would you like to install?".to_string()
}

async fn download_repo_files(
    client: &Client,
    url: &str,
    local_path: &Path,
    progress: &ProgressBar,
) -> Result<(), Box<dyn Error>> {
    let response = client
        .get(url)
        .header(USER_AGENT, "rust-reqwest")
        .send().await?
        .error_for_status()?;

    let items: Vec<RepoContent> = response.json().await?;

    for item in items {
        let item_local_path = local_path.join(&item.name);
        match item.content_type {
            ContentType::File => {
                if let Some(download_url) = item.download_url {
                    let file_response = client
                        .get(&download_url)
                        .header(USER_AGENT, "rust-reqwest")
                        .send().await?
                        .error_for_status()?;

                    let mut local_file = File::create(&item_local_path)?;
                    let content = file_response.bytes().await?;
                    local_file.write_all(&content)?;
                    progress.inc(1);
                }
            }
            ContentType::Dir => {
                create_dir_all(&item_local_path)?;
                let base_url = url.split('?').next().unwrap();
                let sub_url = format!("{}/{}?ref=itai/init-sample-apps", base_url, item.name);
                Box::pin(download_repo_files(client, &sub_url, &item_local_path, progress)).await?;
            }
        }
    }
    Ok(())
}

async fn download_file<T>(
    url: &str,
) -> Result<T, Box<dyn Error>>
where
    T: DeserializeOwned,
{
    let client = Client::new();
    let response = client
        .get(url)
        .header(USER_AGENT, "rust-reqwest")
        .send()
        .await?
        .error_for_status()?;

    let deserialized: T = response.json().await?;
    Ok(deserialized)
}

async fn download_github_file<T: DeserializeOwned>(name: &str) -> Result<T, Box<dyn Error>> {
    let owner = "chroma-core";
    let repo = "chroma";
    let branch_name = "main".to_string();
    let file_path = name;

    let url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        owner, repo, branch_name, file_path
    );
    let file = download_file::<T>(url.as_str()).await?;
    Ok(file)
}

async fn download_sample_app(name: &String, path: &String) -> Result<(), Box<dyn Error>> {
    let url = format!(
        "https://api.github.com/repos/chroma-core/chroma/contents/sample_apps/{}",
        name,
    );
    let app_path = format!("{}/{}", path, name);
    create_dir_all(&app_path)?;

    let client = Client::new();

    println!("{} {}", "Downloading sample app".bold(), name.bold());

    let progress = ProgressBar::new(47);

    download_repo_files(&client, &url, Path::new(&app_path), &progress).await?;

    println!("\n{}", "Download complete!".bold());

    Ok(())
}

async fn download_s3_file(url: &str, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();

    // Create the progress bar
    let progress_bar = ProgressBar::new(0);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Get the file size
    let resp = client.head(url).send().await?;
    let total_size = resp.headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|ct_len| ct_len.to_str().ok())
        .and_then(|ct_len| ct_len.parse::<u64>().ok())
        .unwrap_or(0);

    progress_bar.set_length(total_size);

    // Create directory if it doesn't exist
    if let Some(parent) = Path::new(path).parent() {
        create_dir_all(parent)?;
    }

    // Create the file and download
    let resp = client.get(url).send().await?;
    let mut dest = File::create(path)?;
    let mut stream = resp.bytes_stream();

    let mut downloaded: u64 = 0;

    use futures_util::StreamExt;
    while let Some(item) = stream.next().await {
        let chunk = item?;
        dest.write_all(&chunk)?;
        downloaded += chunk.len() as u64;
        progress_bar.set_position(downloaded);
    }

    progress_bar.finish_with_message("Download complete");

    Ok(())
}

fn extract_zip_file(zip_file_path: &str, output_dir_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let zip_path = Path::new(zip_file_path);
    let output_dir = Path::new(output_dir_path);

    // Open the zip file so it can be read.
    let file = File::open(zip_path)?;

    // Pass the file (which implements Read) to extract.
    extract(file, output_dir, true)?;
    Ok(())
}

fn write_env_file(path: &str, key: &str) -> std::io::Result<()> {
    let env_content = format!(r#"CHROMA_HOST=http://localhost:8000
CHROMA_TENANT=default_tenant
CHROMA_DB_NAME=default_database
OPENAI_API_KEY={}"#, key);

    let path = Path::new(path);
    let mut file = File::create(path)?;
    file.write_all(env_content.as_bytes())?;
    Ok(())
}

fn select_app(apps: &[AppListing]) -> Result<String, CliError> {
    let app_names: Vec<String> = get_display_app_names(apps)?;
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&app_names)
        .default(0)
        .interact()
        .map_err(|_| UtilsError::UserInputFailed)?;
    let name = app_names[selection].clone();
    println!("{}\n", name);
    Ok(name)
}

fn get_display_app_names(apps: &[AppListing]) -> Result<Vec<String>, CliError> {
    let config = read_config()?;
    let installed = config.sample_apps.installed;
    let cli_version = Version::parse(env!("CARGO_PKG_VERSION")).map_err(|_| InstallError::ListingFailed)?;
    apps.into_iter().map(|app| {
        let mut listing = app.name.clone();

        let app_version =  Version::parse(&app.version).map_err(|_| InstallError::ListingFailed)?;
        let requires_update = app_version > cli_version;

        if installed.contains_key(&app.name) {
            let installed_version  = Version::parse(installed.get(&app.name).unwrap()).unwrap();
            if installed_version < app_version {
                listing = match requires_update {
                    true => format!("{} (new version available! Requires CLI update)", listing),
                    false => format!("{} (new version available!)", listing)
                }
            };
        } else if requires_update {
            listing = format!("{} (requires CLI update)", listing);
        }
        Ok(listing)
    }).collect()
}

fn prompt_app_name(apps: &[AppListing], prompt: &str) -> Result<String, CliError> {
    println!("{}", prompt.blue().bold());
    let name = match apps.len() {
        0..=SELECTION_LIMIT => select_app(apps),
        _ => {
            let input = Input::with_theme(&ColorfulTheme::default())
                .interact_text()
                .map_err(|_| UtilsError::UserInputFailed)?;
            Ok(input)
        },
    }?;
   Ok(name)
}

async fn get_app_name(name: Option<String>) -> Result<String, CliError> {
    let apps = download_github_file::<Vec<AppListing>>("sample_apps/config.json").await.map_err(|_| InstallError::ListingsDownloadFailed)?;

    let app_name = match name {
        Some(app_name) => Ok(app_name),
        None => prompt_app_name(&apps, &prompt_app_name_message())
    }?;
    
    let app = apps.iter().find(|app| app.name == app_name).ok_or(InstallError::NoSuchApp(app_name.clone()))?;
    let app_cli_version = Version::parse(&app.cli_version).map_err(|_| InstallError::ListingFailed)?;
    let cli_version = Version::parse(env!("CARGO_PKG_VERSION")).map_err(|_| InstallError::ListingFailed)?;
    if app_cli_version < cli_version {
        return Err(InstallError::VersionMismatch(app_name.clone(), app_cli_version.to_string()).into());
    }
    
    Ok(app_name)
}

fn show_apps(apps: &[AppListing]) -> Result<(), CliError> {
    let app_listings = get_display_app_names(apps)?;
    println!("{}", show_apps_message().blue().bold());
    app_listings.iter().for_each(|listing| {
        println!("{} {}", ">".yellow(), listing);
    });
    Ok(())
}

async fn install_sample_app(args: InstallArgs) -> Result<(), CliError> {
    let apps = download_github_file::<Vec<AppListing>>("sample_apps/config.json").await.map_err(|_| InstallError::ListingsDownloadFailed)?;
    if args.list {
        show_apps(&apps)?;
        return Ok(());
    }
    
    let app_name = get_app_name(args.name).await?;
    
    // Download files
    download_sample_app(&app_name, &".".to_string()).await.map_err(|e| InstallError::GithubDownloadFailed)?;
    
    // Get app config
    

    // println!("{}", "This app requires an OpenAI key in the .env file. Input it here if you want the installer to set it for you, or hit Return to set it later.".bold().blue());
    // let key: String = Password::with_theme(&ColorfulTheme::default())
    //     .allow_empty_password(true)
    //     .report(true)
    //     .interact()
    //     .map_err(|_| UtilsError::UserInputFailed)?;
    // 
    // // Set env variables
    // write_env_file(&format!("./{}/.env", name), &key).map_err(|_e| InstallFailed(name.clone()))?;
    // 
    // println!("\n");
    // 
    // // Ask for installer
    // println!(
    //     "{}",
    //     "Which package manager do you want to use?".blue().bold()
    // );
    // let selection = Select::with_theme(&ColorfulTheme::default())
    //     .items(&app_config.package_managers)
    //     .default(0)
    //     .interact()
    //     .unwrap();
    // let package_manager = app_config.package_managers[selection].clone();
    // 
    // // Run installer
    // install_dependencies(&format!("./{}", name), package_manager.clone()).map_err(|_| InstallFailed(name.clone()))?;

    // Output run instructions

    // Add app to CLI config with version
    Ok(())
}

pub fn install(args: InstallArgs) -> Result<(), CliError> {

    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        install_sample_app(args.name, args.dev).await
    })?;
    Ok(())
}