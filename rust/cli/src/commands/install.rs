use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::{fmt, fs};
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use clap::{Parser, ValueEnum};
use colored::Colorize;
use dialoguer::{Input, Password, Select};
use dialoguer::theme::ColorfulTheme;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use reqwest::header::USER_AGENT;
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zip_extract::extract;
use crate::commands::db::DbError;
use crate::commands::install::InstallError::{GithubDownloadFailed, InstallFailed, NoSuchApp, VersionMismatch};
use crate::commands::install::LlmProvider::OpenAI;
use crate::utils::{CliError, UtilsError};

// Skip installing
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
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[clap(
        index = 1,
        help = "The name of the sample app to install",
    )]
    name: String,
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
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LlmProvider {
    Anthropic,
    Gemini,
    Ollama,
    OpenAI,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum PackageManager {
    Npm,
    Pnpm,
    Yarn,
    Bun,
    Pip,
    Poetry,
}

impl fmt::Display for PackageManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lowercase_name = format!("{:?}", self).to_lowercase();
        write!(f, "{}", lowercase_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SampleAppConfig {
    package_managers: Vec<PackageManager>,
    name: String,
    cli_version: String,
    app_version: String,
    llm_providers: Vec<LlmProvider>,
    startup_commands: HashMap<PackageManager, String>,
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

async fn download_github_file<T: DeserializeOwned>(name: &str, branch: Option<String>,) -> Result<T, Box<dyn Error>> {
    let owner = "chroma-core";
    let repo = "chroma";
    let branch_name = branch.unwrap_or("main".to_string());
    let file_path = name;

    let url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        owner, repo, branch_name, file_path
    );
    let file = download_file::<T>(url.as_str()).await?;
    Ok(file)
}

fn check_version_compatibility(app_name: &str, app_config: &SampleAppConfig) -> Result<(), Box<dyn Error>> {
    let cli_version = env!("CARGO_PKG_VERSION");
    let app_version = &app_config.app_version;

    let cli_semver = Version::parse(cli_version)?;
    let app_semver = Version::parse(app_version)?;

    if app_semver > cli_semver {
        return Err(VersionMismatch(app_name.to_string(), app_version.to_string()).into());
    }

    Ok(())
}

async fn download_sample_app(name: &String, path: &String, branch: Option<String>) -> Result<(), Box<dyn Error>> {
    let branch_ref = match branch {
        Some(branch) => format!("?ref={}", branch),
        None => String::new(),
    };
    let url = format!(
        "https://api.github.com/repos/chroma-core/chroma/contents/sample_apps/{}{}",
        name,
        branch_ref
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
        fs::create_dir_all(parent)?;
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

async fn install_sample_app(name: String, branch: Option<String>) -> Result<(), CliError> {
    // Get all apps manifest to verify app name
    let apps = download_github_file::<Vec<AppListing>>("sample_apps/config.json", branch.clone()).await.map_err(|_| GithubDownloadFailed)?;
    if !apps.iter().any(|app| app.name == name) {
        return Err(NoSuchApp(name.clone()))?
    }

    // Get app manifest and build config
    let config_url = format!("sample_apps/{}/app_config.json", name);
    let app_config = download_github_file::<SampleAppConfig>(&config_url, branch.clone()).await.map_err(|e| {
        println!("Downloading sample app config failed: {}", e);
        GithubDownloadFailed
    })?;

    // Verify CLI version compat
    check_version_compatibility(&name, &app_config).map_err(|_| VersionMismatch(name.clone(), app_config.app_version.clone()))?;

    // Download files
    // download_sample_app(&name, &".".to_string(), branch).await.map_err(|e| {
    //     println!("Downloading sample app failed: {}", e);
    //     GithubDownloadFailed
    // })?;
    // 
    // // Download Chroma from S3
    // println!("\n{}", "Downloading your Chroma DB".bold());
    // let url = "https://s3.us-east-1.amazonaws.com/public.trychroma.com/sample_apps/chatbot/chroma_data.zip";
    // let download_path = format!("./{}/chroma_data.zip", name);
    // download_s3_file(url, &download_path).await.map_err(|_e| InstallFailed(name.clone()))?;
    // 
    // let zip_path = format!("./{}/chroma_data.zip", name);
    // let output = format!("./{}", name);
    // extract_zip_file(zip_path.as_str(), output.as_str()).map_err(|_e| InstallFailed(name.clone()))?;
    // println!("{}\n", "Download complete!".bold());

    println!("{}", "This app requires an OpenAI key in the .env file. Input it here if you want the installer to set it for you, or hit Return to set it later.".bold().blue());
    let key: String = Password::with_theme(&ColorfulTheme::default())
        .allow_empty_password(true)
        .report(true)
        .interact()
        .map_err(|_| UtilsError::UserInputFailed)?;

    // Set env variables
    write_env_file(&format!("./{}/.env", name), &key).map_err(|_e| InstallFailed(name.clone()))?;
    
    println!("\n");

    // Ask for installer
    println!(
        "{}",
        "Which package manager do you want to use?".blue().bold()
    );
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&app_config.package_managers)
        .default(0)
        .interact()
        .unwrap();
    let package_manager = app_config.package_managers[selection].clone();

    // Run installer
    install_dependencies(&format!("./{}", name), package_manager.clone()).map_err(|_| InstallFailed(name.clone()))?;

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