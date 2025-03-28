use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use clap::{Parser, ValueEnum};
use indicatif::ProgressBar;
use reqwest::Client;
use reqwest::header::USER_AGENT;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::commands::db::DbError;
use crate::commands::install::InstallError::{GithubDownloadFailed, NoSuchApp};
use crate::commands::install::LlmProvider::OpenAI;
use crate::utils::CliError;

// Skip installing
#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Failed to install sample app {0}")]
    InstallFailed(String),
    #[error("Failed to download files from Github")]
    GithubDownloadFailed,
    #[error("No such app {0}")]
    NoSuchApp(String),
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
                let sub_url = format!("{}/{}?ref=itai/demo-cli", base_url, item.name);
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

async fn install_sample_app(name: String, branch: Option<String>) -> Result<(), CliError> {
    // Get all apps manifest to verify app name
    let apps = download_github_file::<Vec<AppListing>>("sample_apps/config.json", branch.clone()).await.map_err(|_| GithubDownloadFailed)?;
    if !apps.iter().any(|app| app.name == name) {
        return Err(NoSuchApp(name.clone()))?
    }
    
    // Get app manifest and build config
    let config_url = format!("sample_apps/{}/app_config.json", name);
    let download_app_config = download_github_file::<SampleAppConfig>(&config_url, branch).await.map_err(|e| {
        println!("Downloading sample app config failed: {}", e);
        GithubDownloadFailed
    })?;
    println!("{:?}", download_app_config);
    let app_config = SampleAppConfig {
        package_managers: vec![
            PackageManager::Npm,
            PackageManager::Pnpm,
            PackageManager::Bun,
            PackageManager::Yarn
        ],
        name: "chatbot_debugger".to_string(),
        cli_version: "1.0.0".to_string(),
        app_version: "1.0.0".to_string(),
        llm_providers: vec![OpenAI],
        startup_commands: [
            (PackageManager::Npm, "npm run dev"),
            (PackageManager::Pnpm, "pnpm run dev"),
            (PackageManager::Yarn, "yarn dev"),
            (PackageManager::Bun, "bun run dev"),
        ]
            .into_iter()
            .map(|(k, v)| (k, v.to_string())) // Convert &str to String
            .collect(),
    };

    // Verify CLI version compat

    // Download files

    // Download Chroma from S3

    // Set env variables

    // Ask for installer

    // Run installer

    // Output run instructions

    // Add app to CLI config with version 
    println!("hello");
    Ok(())
}

pub fn install(args: InstallArgs) -> Result<(), CliError> {
    
    let runtime = tokio::runtime::Runtime::new().map_err(|_| DbError::RuntimeError)?;
    runtime.block_on(async {
        install_sample_app(args.name, args.dev).await
    })?;
    Ok(())
}