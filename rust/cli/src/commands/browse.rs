use crate::client::admin_client::AdminClient;
use crate::client::chroma_client::ChromaClient;
use crate::commands::db::get_db_name;
use crate::commands::install::InstallError;
use crate::tui::collection_browser::lib::CollectionBrowser;
use crate::utils::{find_available_port, get_current_profile, AddressBook, CliError};
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use crossterm::style::Stylize;
use std::sync::Arc;
use thiserror::Error;
use tokio::spawn;
use tokio::task::JoinHandle;

#[derive(Parser, Debug, Clone)]
pub struct BrowseArgs {
    #[clap(index = 1, help = "The name of the collection to browse")]
    collection_name: String,
    #[clap(long = "db")]
    db_name: Option<String>,
    #[clap(long, conflicts_with_all = ["host", "config_path"])]
    path: Option<String>,
    #[clap(long, conflicts_with_all = ["path", "config_path"])]
    host: Option<String>,
    #[clap(long = "config", conflicts_with_all = ["host", "path"])]
    config_path: Option<String>,
    #[clap(long)]
    local: bool,
}

#[derive(Debug, Error)]
pub enum BrowseError {
    #[error("Failed to start a local Chroma server")]
    ServerStart,
    #[error("No DBs found for current profile")]
    NoDBs,
    #[error("Collection {0} not found")]
    CollectionNotFound(String),
    #[error("Failed to run collection browser app")]
    BrowserApp,
}

fn input_db_prompt(collection_name: &str) -> String {
    format!("Which DB has collection {}", collection_name)
        .bold()
        .blue()
        .to_string()
}

async fn parse_host(host: String) -> Result<AdminClient, CliError> {
    let admin_client = AdminClient::local(host);
    admin_client.healthcheck().await?;
    Ok(admin_client)
}

async fn standup_local_chroma(
    config: FrontendServerConfig,
) -> Result<(AdminClient, JoinHandle<()>), CliError> {
    let host = format!("http://localhost:{}", config.port);
    let handle = spawn(async move {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
    });
    let admin_client = AdminClient::local(host);
    admin_client.healthcheck().await?;
    Ok((admin_client, handle))
}

async fn parse_path(path: String) -> Result<(AdminClient, JoinHandle<()>), CliError> {
    let mut config = FrontendServerConfig::single_node_default();
    config.persist_path = path;
    config.port = find_available_port(8000, 9000)?;
    standup_local_chroma(config).await
}

async fn parse_config(config_path: String) -> Result<(AdminClient, JoinHandle<()>), CliError> {
    let config = FrontendServerConfig::load_from_path(&config_path);
    standup_local_chroma(config).await
}

async fn parse_local() -> Result<(AdminClient, Option<JoinHandle<()>>), CliError> {
    let default_host = String::from("http://localhost:8000");
    match parse_host(default_host).await {
        Ok(admin_client) => Ok((admin_client, None)),
        Err(_) => {
            let default_config = String::from("chroma.config.yml");
            let (admin_client, handle) = parse_config(default_config).await?;
            Ok((admin_client, Some(handle)))
        }
    }
}

async fn parse_local_args(
    args: BrowseArgs,
) -> Result<(ChromaClient, Option<JoinHandle<()>>), CliError> {
    let (admin_client, handle) = if args.host.is_some() {
        (parse_host(args.host.unwrap()).await?, None)
    } else if args.path.is_some() {
        let (client, handle) = parse_path(args.path.unwrap()).await?;
        (client, Some(handle))
    } else if args.config_path.is_some() {
        let (client, handle) = parse_config(args.config_path.unwrap()).await?;
        (client, Some(handle))
    } else if args.local {
        parse_local().await?
    } else {
        return Err(BrowseError::ServerStart.into());
    };

    if let Some(db_name) = args.db_name.clone() {
        let _verified = admin_client.get_database(db_name).await?;
    }

    let chroma_client = ChromaClient::with_admin_client(
        admin_client,
        args.db_name.unwrap_or(String::from("default_database")),
    );
    Ok((chroma_client, handle))
}

pub async fn get_cloud_client(
    db_name: Option<String>,
    collection_name: &str,
) -> Result<ChromaClient, CliError> {
    let profile = get_current_profile()?;
    let admin_client = AdminClient::from_profile(AddressBook::cloud().frontend_url, &profile.1);

    if let Some(db_name) = db_name {
        let _verified = admin_client.get_database(db_name.clone()).await?;
        return Ok(ChromaClient::with_admin_client(admin_client, db_name));
    }

    let databases = admin_client.list_databases().await?;
    match databases.len() {
        0 => Err(BrowseError::NoDBs.into()),
        1 => Ok(ChromaClient::with_admin_client(
            admin_client,
            databases[0].name.clone(),
        )),
        _ => {
            let input_name = get_db_name(&databases, &input_db_prompt(collection_name))?;
            let _verified = admin_client.get_database(input_name.clone()).await?;
            Ok(ChromaClient::with_admin_client(admin_client, input_name))
        }
    }
}

fn local_setup(args: BrowseArgs) -> bool {
    args.local || args.host.is_some() || args.path.is_some() || args.config_path.is_some()
}

pub fn browse(args: BrowseArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {
        let (client, _handle) = match local_setup(args.clone()) {
            true => parse_local_args(args.clone()).await,
            false => Ok((
                get_cloud_client(args.db_name, &args.collection_name).await?,
                None,
            )),
        }?;

        let collection = client
            .get_collection(args.collection_name.clone())
            .await
            .map_err(|_| BrowseError::CollectionNotFound(args.collection_name))?;

        let mut collection_browser = CollectionBrowser::new(collection);
        collection_browser
            .run()
            .await
            .map_err(|_| BrowseError::BrowserApp)?;

        Ok::<(), CliError>(())
    })?;
    Ok(())
}
