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
use chroma::client::{
    ChromaAuthMethod, ChromaHttpClientError, ChromaHttpClientOptions, ChromaRetryOptions,
};
use chroma::ChromaHttpClient;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::spawn;
use tokio::task::JoinHandle;

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
    ChromaClient(#[from] ChromaHttpClientError),
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
    Browse(#[from] BrowseError),
    #[error("{0}")]
    Copy(#[from] CopyError),
    #[error("{0}")]
    WebPage(#[from] WebPageError),
}

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("User home directory not found")]
    HomeDirNotFound,
    #[error("Failed to create .chroma directory")]
    ChromaDirCreateFailed,
    #[error("~/.chroma exists but is not a directory")]
    ChromaDirNotADirectory,
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
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Invalid API key")]
    InvalidApiKey,
}

#[derive(Parser, Debug, Clone)]
pub struct LocalChromaArgs {
    #[clap(long, conflicts_with_all = ["host"], help = "The data path for your local Chroma server")]
    pub path: Option<String>,
    #[clap(long, conflicts_with_all = ["path"], help = "The hostname for your local Chroma server")]
    pub host: Option<String>,
}

pub async fn connect_local(
    args: LocalChromaArgs,
) -> Result<(ChromaHttpClient, Option<JoinHandle<()>>), CliError> {
    if let Some(host) = args.host {
        let client = local_client(&host)?;
        client.heartbeat().await?;
        Ok((client, None))
    } else if let Some(path) = args.path {
        let mut config = FrontendServerConfig::single_node_default();
        let db_path = Path::new(&path).join(&config.sqlite_filename);
        if !db_path.is_file() {
            return Err(UtilsError::NotChromaPath.into());
        }

        let port = find_available_port()?;
        config.persist_path = path;
        config.port = port;

        let host = format!("http://localhost:{}", config.port);
        let handle = spawn(async move {
            frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config, true)
                .await;
        });
        let client = local_client(&host)?;
        client
            .heartbeat()
            .await
            .map_err(|_| UtilsError::LocalConnect)?;
        Ok((client, Some(handle)))
    } else {
        let client = local_client_default()?;
        client
            .heartbeat()
            .await
            .map_err(|_| UtilsError::LocalConnect)?;
        Ok((client, None))
    }
}

fn find_available_port() -> Result<u16, CliError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|_| UtilsError::PortSearch)?;
    let port = listener
        .local_addr()
        .map_err(|_| UtilsError::PortSearch)?
        .port();
    Ok(port)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
    pub sample_apps: SampleAppsConfig,
    #[serde(default)]
    pub theme: Theme,
}

pub type Profiles = HashMap<String, Profile>;

pub fn cloud_client(profile: &Profile) -> Result<ChromaHttpClient, CliError> {
    let mut options = ChromaHttpClientOptions::cloud_admin(&profile.api_key)
        .map_err(|_| UtilsError::InvalidApiKey)?;
    options.tenant_id = Some(profile.tenant_id.clone());
    Ok(ChromaHttpClient::new(options))
}

pub fn local_client(host: &str) -> Result<ChromaHttpClient, CliError> {
    let options = ChromaHttpClientOptions {
        endpoint: host
            .parse()
            .map_err(|_| UtilsError::InvalidUrl(host.to_string()))?,
        endpoints: Vec::new(),
        auth_method: ChromaAuthMethod::None,
        retry_options: ChromaRetryOptions::default(),
        tenant_id: Some("default_tenant".to_string()),
        database_name: Some("default_database".to_string()),
    };
    Ok(ChromaHttpClient::new(options))
}

pub fn local_client_default() -> Result<ChromaHttpClient, CliError> {
    let options = ChromaHttpClientOptions {
        endpoint: ChromaHttpClientOptions::default().endpoint,
        endpoints: Vec::new(),
        auth_method: ChromaAuthMethod::None,
        retry_options: ChromaRetryOptions::default(),
        tenant_id: Some("default_tenant".to_string()),
        database_name: Some("default_database".to_string()),
    };
    Ok(ChromaHttpClient::new(options))
}
