use crate::client::admin_client::{AdminClient, AdminClientError};
use crate::client::chroma_client::ChromaClientError;
use crate::client::collection::CollectionAPIError;
use crate::client::dashboard_client::DashboardClientError;
use crate::commands::browse::BrowseError;
use crate::commands::copy::CopyError;
use crate::commands::db::DbError;
use crate::commands::install::InstallError;
use crate::commands::login::LoginError;
use crate::commands::profile::ProfileError;
use crate::commands::run::RunError;
use crate::commands::update::UpdateError;
use crate::commands::vacuum::VacuumError;
use crate::commands::webpage::WebPageError;
use crate::ui_utils::Theme;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::spawn;
use tokio::task::JoinHandle;

pub const CHROMA_DIR: &str = ".chroma";
pub const CREDENTIALS_FILE: &str = "credentials";
const CONFIG_FILE: &str = "config.json";
pub const SELECTION_LIMIT: usize = 5;
pub const CHROMA_API_KEY_ENV_VAR: &str = "CHROMA_API_KEY";
pub const CHROMA_TENANT_ENV_VAR: &str = "CHROMA_TENANT";
pub const CHROMA_DATABASE_ENV_VAR: &str = "CHROMA_DATABASE";

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
    #[error("{0}")]
    AdminClient(#[from] AdminClientError),
    #[error("{0}")]
    Browse(#[from] BrowseError),
    #[error("{0}")]
    Copy(#[from] CopyError),
    #[error("{0}")]
    Collection(#[from] CollectionAPIError),
    #[error("{0}")]
    WebPage(#[from] WebPageError),
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
    #[error("Failed to copy to clipboard")]
    CopyToClipboardFailed,
    #[error("Input validation failed")]
    NameValidationFailed,
    #[error("name cannot be empty and must only contain alphanumerics, underscores, or hyphens")]
    InvalidName,
    #[error("Failed to find an available port")]
    PortSearch,
    #[error("Failed to connect to a local Chroma server")]
    LocalConnect,
    #[error("Not a Chroma path")]
    NotChromaPath,
    #[error("Quota Error: {0}")]
    Quota(String),
}

#[derive(Parser, Debug, Clone)]
pub struct LocalChromaArgs {
    #[clap(long, conflicts_with_all = ["host"], help = "The data path for your local Chroma server")]
    pub path: Option<String>,
    #[clap(long, conflicts_with_all = ["path"], help = "The hostname for your local Chroma server")]
    pub host: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorResponse {
    pub(crate) message: String,
}

impl Default for ErrorResponse {
    fn default() -> Self {
        Self {
            message: "".to_owned(),
        }
    }
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
    #[serde(default)]
    pub theme: Theme,
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

impl Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Environment::Local => write!(f, "Local"),
            Environment::Cloud => write!(f, "Cloud"),
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
        theme: Theme::default(),
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

pub fn find_available_port(min: u16, max: u16) -> Result<u16, CliError> {
    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let port = rng.gen_range(min..=max);
        let addr = format!("127.0.0.1:{}", port);

        if TcpListener::bind(&addr).is_ok() {
            return Ok(port);
        }
    }

    Err(UtilsError::PortSearch.into())
}

pub async fn parse_host(host: String) -> Result<AdminClient, CliError> {
    let admin_client = AdminClient::local(host);
    admin_client.healthcheck().await?;
    Ok(admin_client)
}

pub async fn standup_local_chroma(
    config: FrontendServerConfig,
) -> Result<(AdminClient, JoinHandle<()>), CliError> {
    let host = format!("http://localhost:{}", config.port);
    let handle = spawn(async move {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config, true).await;
    });
    let admin_client = AdminClient::local(host);
    admin_client
        .healthcheck()
        .await
        .map_err(|_| UtilsError::LocalConnect)?;
    Ok((admin_client, handle))
}

pub async fn parse_path(path: String) -> Result<(AdminClient, JoinHandle<()>), CliError> {
    if !is_chroma_path(&path) {
        return Err(UtilsError::NotChromaPath.into());
    }
    let mut config = FrontendServerConfig::single_node_default();
    config.persist_path = path;
    config.port = find_available_port(8000, 9000)?;
    standup_local_chroma(config).await
}

pub async fn parse_local() -> Result<AdminClient, CliError> {
    let default_host = AddressBook::local().frontend_url;
    parse_host(default_host).await
}

pub fn is_chroma_path<P: AsRef<Path>>(dir: P) -> bool {
    let config = FrontendServerConfig::single_node_default();
    let db_path = dir.as_ref().join(config.sqlite_filename);
    db_path.is_file()
}

pub fn parse_value(s: &str) -> Value {
    if let Ok(n) = s.parse::<i64>() {
        Value::Number(n.into())
    } else if let Ok(f) = s.parse::<f64>() {
        Value::Number(serde_json::Number::from_f64(f).unwrap())
    } else if let Ok(b) = s.parse::<bool>() {
        Value::Bool(b)
    } else {
        Value::String(s.to_string())
    }
}
