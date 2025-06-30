use crate::ui_utils::read_secret;
use crate::utils::UtilsError::UserInputFailed;
use crate::utils::{read_config, write_config, CliConfig, CliError, SELECTION_LIMIT};
use clap::Parser;
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::USER_AGENT;
use reqwest::Client;
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{create_dir_all, read_to_string, File};
use std::io::Write;
use std::path::Path;
use thiserror::Error;
use zip_extract::extract;

#[derive(Debug, Error)]
pub enum InstallError {
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
    #[error("Failed to read sample app config")]
    AppConfigReadFailed,
    #[error("Failed to get runtime for installation")]
    RuntimeError,
    #[error("Failed to write .env file")]
    EnvFileWriteFailed,
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[clap(index = 1, help = "The name of the sample app to install")]
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
    #[allow(dead_code)]
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
struct EnvVariable {
    name: String,
    secret: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleAppConfig {
    required_env_variables: Vec<EnvVariable>,
    optional_env_variables: Vec<EnvVariable>,
    startup_commands: HashMap<String, String>,
}

pub struct SampleAppEnvVariables(HashMap<String, String>);

impl SampleAppEnvVariables {
    pub fn local() -> Self {
        let map = HashMap::from([
            (
                "CHROMA_HOST".to_string(),
                "http://localhost:8000".to_string(),
            ),
            ("CHROMA_TENANT".to_string(), "default_tenant".to_string()),
            ("CHROMA_DB_NAME".to_string(), "default_database".to_string()),
        ]);
        SampleAppEnvVariables(map)
    }
}

fn show_apps_message() -> String {
    "Available sample apps:".to_string()
}

fn prompt_app_name_message() -> String {
    "Which sample app would you like to install?".to_string()
}

fn prompt_env_variables_message(env_variables: &[EnvVariable]) -> String {
    match env_variables.len() {
        0..=1 => format!(
            "\nThis app requires the {} environment variable. You can set it up with the installer, or edit your .env file later.",
            env_variables[0].name
        ),
        _ => format!(
            "\nThis app requires the following environment variables: {}. You can set them up with the installer, or edit your .env file later.",
            env_variables
                .iter()
                .map(|v| v.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        ),
    }
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
        .send()
        .await?
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
                        .send()
                        .await?
                        .error_for_status()?;

                    let mut local_file = File::create(&item_local_path)?;
                    let content = file_response.bytes().await?;
                    local_file.write_all(&content)?;
                    progress.set_message(format!("Downloading {}", item.name));
                }
            }
            ContentType::Dir => {
                create_dir_all(&item_local_path)?;
                let base_url = url.split('?').next().unwrap();
                let sub_url = format!("{}/{}", base_url, item.name);
                Box::pin(download_repo_files(
                    client,
                    &sub_url,
                    &item_local_path,
                    progress,
                ))
                .await?;
            }
        }
    }
    Ok(())
}

async fn download_file<T>(url: &str) -> Result<T, Box<dyn Error>>
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

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    progress.set_message("Downloading files...");
    progress.enable_steady_tick(std::time::Duration::from_millis(100));

    download_repo_files(&client, &url, Path::new(&app_path), &progress).await?;

    progress.finish();

    println!("\n{}", "Download complete!".bold());

    Ok(())
}

#[allow(dead_code)]
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
    let total_size = resp
        .headers()
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

#[allow(dead_code)]
fn extract_zip_file(
    zip_file_path: &str,
    output_dir_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let zip_path = Path::new(zip_file_path);
    let output_dir = Path::new(output_dir_path);

    // Open the zip file so it can be read.
    let file = File::open(zip_path)?;

    // Pass the file (which implements Read) to extract.
    extract(file, output_dir, true)?;
    Ok(())
}

fn select_app(apps: &[AppListing], cli_config: &CliConfig) -> Result<String, CliError> {
    let display_names: Vec<String> = get_display_app_names(apps, cli_config)?;
    let app_names = apps.iter().map(|a| &a.name).collect::<Vec<_>>();
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&display_names)
        .default(0)
        .interact()
        .map_err(|_| UserInputFailed)?;
    let name = app_names[selection].clone();
    println!("{}\n", name);
    Ok(name)
}

fn get_display_app_names(
    apps: &[AppListing],
    cli_config: &CliConfig,
) -> Result<Vec<String>, CliError> {
    let installed = &cli_config.sample_apps.installed;
    let cli_version =
        Version::parse(env!("CARGO_PKG_VERSION")).map_err(|_| InstallError::ListingFailed)?;
    let sample_apps_url = "https://github.com/chroma-core/chroma/tree/main/sample_apps";

    apps.iter()
        .map(|app| {
            let url = format!("{}/{}", sample_apps_url, app.name);
            let mut listing = format!("\x1b]8;;{}\x1b\\{}\x1b]8;;\x1b\\", url, app.name);

            let app_version =
                Version::parse(&app.version).map_err(|_| InstallError::ListingFailed)?;
            let requires_update = app_version > cli_version;

            if installed.contains_key(&app.name) {
                let installed_version = Version::parse(installed.get(&app.name).unwrap()).unwrap();
                if installed_version < app_version {
                    listing = match requires_update {
                        true => format!("{} (new version available! Requires CLI update)", listing),
                        false => format!("{} (new version available!)", listing),
                    }
                };
            } else if requires_update {
                listing = format!("{} (requires CLI update)", listing);
            }

            listing = format!("{}: {}", listing, app.description);

            Ok(listing)
        })
        .collect()
}

fn prompt_app_name(
    apps: &[AppListing],
    prompt: &str,
    cli_config: &CliConfig,
) -> Result<String, CliError> {
    println!("{}", prompt.blue().bold());
    let name = match apps.len() {
        0..=SELECTION_LIMIT => select_app(apps, cli_config),
        _ => {
            let input = Input::with_theme(&ColorfulTheme::default())
                .interact_text()
                .map_err(|_| UserInputFailed)?;
            Ok(input)
        }
    }?;
    Ok(name)
}

async fn get_app(
    apps: &[AppListing],
    name: Option<String>,
    cli_config: &CliConfig,
) -> Result<(String, String), CliError> {
    let app_name = match name {
        Some(app_name) => Ok(app_name),
        None => prompt_app_name(apps, &prompt_app_name_message(), cli_config),
    }?;

    let app = apps
        .iter()
        .find(|app| app.name == app_name)
        .ok_or(InstallError::NoSuchApp(app_name.clone()))?;
    let app_cli_version =
        Version::parse(&app.cli_version).map_err(|_| InstallError::ListingFailed)?;
    let cli_version =
        Version::parse(env!("CARGO_PKG_VERSION")).map_err(|_| InstallError::ListingFailed)?;
    if app_cli_version > cli_version {
        return Err(
            InstallError::VersionMismatch(app_name.clone(), app_cli_version.to_string()).into(),
        );
    }

    Ok((app_name, app.version.clone()))
}

fn show_apps(apps: &[AppListing], cli_config: &CliConfig) -> Result<(), CliError> {
    let app_listings = get_display_app_names(apps, cli_config)?;
    println!("{}", show_apps_message().blue().bold());
    app_listings.iter().for_each(|listing| {
        println!("{} {}", ">".yellow(), listing,);
    });
    Ok(())
}

fn read_app_config(app_name: &str) -> Result<SampleAppConfig, Box<dyn Error>> {
    let mut path = env::current_dir()?;
    path.push(app_name);
    path.push("config.json");

    let contents = read_to_string(path)?;
    let config: SampleAppConfig = serde_json::from_str(&contents)?;

    Ok(config)
}

fn write_env_file(
    sample_app_env_variables: SampleAppEnvVariables,
    file_path: String,
) -> std::io::Result<()> {
    let mut env_var_strings = sample_app_env_variables
        .0
        .iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<String>>();
    env_var_strings.sort();
    env_var_strings.push("".to_string());

    let path = Path::new(&file_path);
    let mut file = File::create(path)?;
    file.write_all(env_var_strings.join("\n").as_bytes())?;
    Ok(())
}

fn get_app_env_variables(app_config: &SampleAppConfig) -> Result<SampleAppEnvVariables, CliError> {
    let mut env_variables = SampleAppEnvVariables::local();

    app_config
        .required_env_variables
        .iter()
        .for_each(|env_var| {
            env_variables.0.insert(env_var.name.clone(), "".to_string());
        });

    app_config
        .optional_env_variables
        .iter()
        .for_each(|env_var| {
            env_variables.0.insert(env_var.name.clone(), "".to_string());
        });

    println!(
        "{}",
        prompt_env_variables_message(&app_config.required_env_variables)
            .blue()
            .bold()
    );
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&["Set with the installer", "Manually set later in .env"])
        .default(0)
        .interact()
        .map_err(|_| UserInputFailed)?;

    if selection == 0 {
        for env_var in app_config.required_env_variables.clone() {
            let prompt = format!("Enter your {} (Return to skip)", env_var.name);
            let value = read_secret(&prompt).map_err(|_| UserInputFailed)?;
            env_variables.0.insert(env_var.name.clone(), value);
        }
    }

    Ok(env_variables)
}

fn display_run_instructions(app_config: SampleAppConfig) {
    let instructions = app_config
        .startup_commands
        .iter()
        .map(|(key, value)| format!("{}\n{}", key.underline(), value))
        .collect::<Vec<String>>()
        .join("\n\n");
    println!(
        "\n\n{}\n{}",
        "Installation completed!".bold().blue(),
        instructions
    );
}

async fn install_sample_app(args: InstallArgs) -> Result<(), CliError> {
    let mut cli_config = read_config()?;
    let apps = download_github_file::<Vec<AppListing>>("sample_apps/listings.json")
        .await
        .map_err(|_| InstallError::ListingsDownloadFailed)?;
    if args.list {
        show_apps(&apps, &cli_config)?;
        return Ok(());
    }

    let (app_name, app_version) = get_app(&apps, args.name, &cli_config).await?;

    // Download files
    download_sample_app(&app_name, &".".to_string())
        .await
        .map_err(|_| InstallError::GithubDownloadFailed)?;

    // Get app config
    let app_config =
        read_app_config(app_name.as_str()).map_err(|_| InstallError::AppConfigReadFailed)?;

    let env_variables = get_app_env_variables(&app_config)?;
    write_env_file(env_variables, format!("./{}/.env", app_name))
        .map_err(|_| InstallError::EnvFileWriteFailed)?;

    // Add app to CLI config with version
    cli_config
        .sample_apps
        .installed
        .insert(app_name, app_version);
    write_config(&cli_config)?;

    // Output run instructions
    display_run_instructions(app_config);

    Ok(())
}

pub fn install(args: InstallArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async { install_sample_app(args).await })?;
    Ok(())
}
