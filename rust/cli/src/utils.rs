use crate::client::ClientError;
use crate::commands::db::DbError;
use crate::commands::profile::ProfileError;
use crate::commands::run::RunError;
use crate::commands::vacuum::VacuumError;
use arboard::Clipboard;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
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

const CHROMA_DIR: &str = ".chroma";
const CREDENTIALS_FILE: &str = "credentials";
const CONFIG_FILE: &str = "config.json";

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Utils(#[from] UtilsError),
    #[error("{0}")]
    Profile(#[from] ProfileError),
    #[error("{0}")]
    Run(#[from] RunError),
    #[error("{0}")]
    Vacuum(#[from] VacuumError),
    #[error("{0}")]
    Client(#[from] ClientError),
    #[error("{0}")]
    Db(#[from] DbError),
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub api_key: String,
    pub tenant_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
}

#[derive(Debug, Deserialize)]
pub struct AddressBook {
    pub frontend_url: String,
}

impl AddressBook {
    pub fn new(frontend_url: String) -> Self {
        AddressBook { frontend_url }
    }
    pub fn local() -> Self {
        Self::new("http://localhost:8000".to_string())
    }

    pub fn cloud() -> Self {
        Self::new("https://api.trychroma.com".to_string())
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
