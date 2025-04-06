use crate::client::ChromaClientError;
use crate::commands::db::DbError;
use crate::commands::install::InstallError;
use crate::commands::login::LoginError;
use crate::commands::profile::ProfileError;
use crate::commands::run::RunError;
use crate::commands::update::UpdateError;
use crate::commands::vacuum::VacuumError;
use crate::dashboard_client::DashboardClientError;
use arboard::Clipboard;
use colored::Colorize;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent},
    terminal::{disable_raw_mode, enable_raw_mode},
    ExecutableCommand,
};
use regex::Regex;
use reqwest::header::HeaderMap;
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::{fs, io};
use thiserror::Error;

pub const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

pub const CHROMA_DIR: &str = ".chroma";
pub const CREDENTIALS_FILE: &str = "credentials";
const CONFIG_FILE: &str = "config.json";
pub const SELECTION_LIMIT: usize = 5;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Utils(#[from] UtilsError),
    #[error("{0}")]
    Profile(#[from] ProfileError),
    #[error("{0}")]
    Run(#[from] RunError),
    #[error("Failed to vacuum Chroma")]
    Vacuum(#[from] VacuumError),
    #[error("{0}")]
    Client(#[from] ChromaClientError),
    #[error("{0}")]
    Db(#[from] DbError),
    #[error("{0}")]
    Update(#[from] UpdateError),
    #[error("{0}")]
    Login(#[from] LoginError),
    #[error("{0}")]
    DashboardClient(#[from] DashboardClientError),
    #[error("{0}")]
    Install(#[from] InstallError),
}

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("User home directory not found")]
    HomeDirNotFound,
    #[error("Failed to create .chroma directory")]
    ChromaDirCreateFailed,
    #[error("Failed to create credentials file")]
    CredsFileCreateFailed,
    #[error("Failed to create config file")]
    ConfigFileCreateFailed,
    #[error("Failed to read credentials file")]
    CredsFileReadFailed,
    #[error("Failed to parse credentials file")]
    CredsFileParseFailed,
    #[error("Failed to write credentials file")]
    CredsFileWriteFailed,
    #[error("Failed to read config file")]
    ConfigFileReadFailed,
    #[error("Failed to parse config file")]
    ConfigFileParseFailed,
    #[error("Failed to write config file")]
    ConfigFileWriteFailed,
    #[error("Failed to get user input")]
    UserInputFailed,
    #[error("Failed to open browser. {0}")]
    BrowserOpenFailed(String),
    #[error("Failed to copy to clipboard")]
    CopyToClipboardFailed,
    #[error("Input validation failed")]
    NameValidationFailed,
    #[error("name cannot be empty and must only contain alphanumerics, underscores, or hyphens")]
    InvalidName,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub api_key: String,
    pub tenant_id: String,
}

impl Profile {
    pub fn new(api_key: String, tenant_id: String) -> Self {
        Self { api_key, tenant_id }
    }
}

fn default_show_updates() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SampleAppsConfig {
    #[serde(default = "default_show_updates")]
    pub show_updates: bool,
    #[serde(default)]
    pub installed: HashMap<String, String>,
}

impl Default for SampleAppsConfig {
    fn default() -> Self {
        Self {
            show_updates: default_show_updates(),
            installed: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
    pub sample_apps: SampleAppsConfig,
}

#[derive(Debug, Deserialize)]
pub struct AddressBook {
    pub frontend_url: String,
    pub dashboard_api_url: String,
    pub dashboard_frontend_url: String,
}

impl AddressBook {
    pub fn new(
        frontend_url: String,
        dashboard_api_url: String,
        dashboard_frontend_url: String,
    ) -> Self {
        AddressBook {
            frontend_url,
            dashboard_api_url,
            dashboard_frontend_url,
        }
    }
    pub fn local() -> Self {
        Self::new(
            "http://localhost:8000".to_string(),
            "http://localhost:8002".to_string(),
            "http://localhost:3001".to_string(),
        )
    }

    pub fn cloud() -> Self {
        Self::new(
            "https://api.trychroma.com:8000".to_string(),
            "https://backend.trychroma.com".to_string(),
            "https://trychroma.com".to_string(),
        )
    }
}

#[derive(Debug)]
pub enum Environment {
    Local,
    Cloud,
}

impl Environment {
    pub fn address_book(&self) -> AddressBook {
        match self {
            Environment::Local => AddressBook::local(),
            Environment::Cloud => AddressBook::cloud(),
        }
    }
}

pub type Profiles = HashMap<String, Profile>;

fn get_chroma_dir() -> Result<PathBuf, CliError> {
    let home_dir = dirs::home_dir().ok_or(UtilsError::HomeDirNotFound)?;
    let chroma_dir = home_dir.join(CHROMA_DIR);
    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir).map_err(|_| UtilsError::ChromaDirCreateFailed)?;
    };
    Ok(chroma_dir)
}

fn get_credentials_file_path() -> Result<PathBuf, CliError> {
    let chroma_dir = get_chroma_dir()?;
    let credentials_path = chroma_dir.join(CREDENTIALS_FILE);
    if !credentials_path.exists() {
        fs::write(&credentials_path, "").map_err(|_| UtilsError::CredsFileCreateFailed)?;
    }
    Ok(credentials_path)
}

fn create_config_file(config_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let default_config = CliConfig {
        current_profile: String::new(),
        sample_apps: SampleAppsConfig::default(),
    };
    let json_str = serde_json::to_string_pretty(&default_config)?;
    fs::write(config_path, json_str)?;
    Ok(())
}

fn get_config_file_path() -> Result<PathBuf, CliError> {
    let chroma_dir = get_chroma_dir()?;
    let config_path = chroma_dir.join(CONFIG_FILE);
    if !config_path.exists() {
        create_config_file(&config_path).map_err(|_| UtilsError::ConfigFileCreateFailed)?;
    }
    Ok(config_path)
}

pub fn get_address_book(dev: bool) -> AddressBook {
    match dev {
        true => Environment::Local.address_book(),
        false => Environment::Cloud.address_book(),
    }
}

pub fn read_profiles() -> Result<Profiles, CliError> {
    let credentials_path = get_credentials_file_path()?;
    let contents =
        fs::read_to_string(credentials_path).map_err(|_| UtilsError::CredsFileReadFailed)?;
    let profiles: Profiles =
        toml::from_str(&contents).map_err(|_| UtilsError::CredsFileParseFailed)?;
    Ok(profiles)
}

pub fn write_profiles(profiles: &Profiles) -> Result<(), CliError> {
    let credentials_path = get_credentials_file_path()?;
    let toml_str = toml::to_string(profiles).map_err(|_| UtilsError::CredsFileParseFailed)?;
    fs::write(credentials_path, toml_str).map_err(|_| UtilsError::CredsFileWriteFailed)?;
    Ok(())
}

pub fn read_config() -> Result<CliConfig, CliError> {
    let config_path = get_config_file_path()?;
    let contents =
        fs::read_to_string(&config_path).map_err(|_| UtilsError::ConfigFileReadFailed)?;
    let config: CliConfig =
        serde_json::from_str(&contents).map_err(|_| UtilsError::ConfigFileParseFailed)?;
    Ok(config)
}

pub fn write_config(config: &CliConfig) -> Result<(), CliError> {
    let config_path = get_config_file_path()?;
    let json_str =
        serde_json::to_string_pretty(config).map_err(|_| UtilsError::ConfigFileParseFailed)?;
    fs::write(config_path, json_str).map_err(|_| UtilsError::ConfigFileWriteFailed)?;
    Ok(())
}

pub fn get_profile(name: String) -> Result<Profile, CliError> {
    let profiles = read_profiles()?;
    if !profiles.contains_key(&name) {
        Err(ProfileError::ProfileNotFound(name).into())
    } else {
        Ok(profiles[&name].clone())
    }
}

pub fn get_current_profile() -> Result<(String, Profile), CliError> {
    let config = read_config()?;
    let profile_name = config.current_profile.clone();
    let profile = get_profile(config.current_profile).map_err(|e| match e {
        CliError::Profile(ProfileError::ProfileNotFound(_)) => ProfileError::NoActiveProfile.into(),
        _ => e,
    })?;
    Ok((profile_name, profile))
}

pub fn copy_to_clipboard(copy_string: &str) -> Result<(), CliError> {
    let mut clipboard = Clipboard::new().map_err(|_| UtilsError::CopyToClipboardFailed)?;
    clipboard
        .set_text(copy_string)
        .map_err(|_| UtilsError::CopyToClipboardFailed)?;
    println!("\n{}", "Copied to clipboard!".blue().bold());
    Ok(())
}

pub fn validate_uri(input: String) -> Result<String, UtilsError> {
    if input.is_empty() {
        return Err(UtilsError::InvalidName);
    }

    let re = Regex::new(r"^[a-zA-Z0-9_-]+$")
        .map_err(|e| e.to_string())
        .map_err(|_| UtilsError::NameValidationFailed)?;
    if !re.is_match(&input) {
        return Err(UtilsError::InvalidName);
    }

    Ok(input)
}

pub async fn send_request<T, R>(
    url: &String,
    method: Method,
    route: &str,
    headers: Option<HeaderMap>,
    body: Option<&T>,
) -> Result<R, Box<dyn Error>>
where
    T: Serialize,
    R: DeserializeOwned + Default,
{
    let url = format!("{}{}", url, route);

    let client = Client::new();
    let mut request_builder = client.request(method, url);

    if let Some(headers) = headers {
        request_builder = request_builder.headers(headers);
    }

    if let Some(b) = body {
        request_builder = request_builder.json(b);
    }

    let response = request_builder.send().await?.error_for_status()?;
    let parsed_response = response.json::<R>().await?;
    Ok(parsed_response)
}

pub fn read_secret(prompt: &str) -> io::Result<String> {
    let mut stdout = stdout();
    let mut password = String::new();

    stdout.write_all(prompt.as_bytes())?;
    stdout.write_all(b": ")?;
    stdout.flush()?;

    enable_raw_mode()?;

    loop {
        if let Event::Key(KeyEvent { code, .. }) = event::read()? {
            match code {
                KeyCode::Enter => break,
                KeyCode::Char(c) => {
                    password.push(c);
                    stdout.write_all(b"*")?;
                }
                KeyCode::Backspace => {
                    if !password.is_empty() {
                        password.pop();
                        stdout.execute(cursor::MoveLeft(1))?;
                        stdout.write_all(b" ")?;
                        stdout.execute(cursor::MoveLeft(1))?;
                    }
                }
                _ => {}
            }
            stdout.flush()?;
        }
    }

    disable_raw_mode()?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;

    Ok(password)
}
