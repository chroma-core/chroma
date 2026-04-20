use crate::commands::db::get_db_name;
use crate::commands::install::InstallError;
use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::tui::collection_browser::CollectionBrowser;
use crate::ui_utils::Theme;
use crate::utils::{cloud_client, connect_local, CliError, LocalChromaArgs};
use chroma::ChromaHttpClient;
use clap::Parser;
use crossterm::style::Stylize;
use thiserror::Error;

#[derive(Parser, Debug, Clone)]
pub struct BrowseArgs {
    #[clap(index = 1, help = "The name of the collection to browse")]
    collection_name: String,
    #[clap(long = "db", help = "The Chroma Cloud DB name with your collection")]
    db_name: Option<String>,
    #[clap(long, help = "Find this collection on a local Chroma server")]
    local: bool,
    #[clap(long, help = "Dark or Light theme for the collection browser")]
    theme: Option<Theme>,
    #[clap(flatten)]
    local_chroma_args: LocalChromaArgs,
}

#[derive(Debug, Error)]
pub enum BrowseError {
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

pub async fn get_cloud_client(
    db_name: Option<String>,
    collection_name: &str,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<ChromaHttpClient, CliError> {
    let profile = store.get_current_profile()?;
    let client = cloud_client(&profile.1)?;

    if let Some(db_name) = db_name {
        // Verify the DB exists
        let dbs = client.list_databases().await?;
        if !dbs.iter().any(|db| db.name == db_name) {
            return Err(CliError::Db(crate::commands::db::DbError::DbNotFound(
                db_name,
            )));
        }
        client.set_database_name(db_name);
        return Ok(client);
    }

    let databases = client.list_databases().await?;
    match databases.len() {
        0 => Err(BrowseError::NoDBs.into()),
        1 => {
            client.set_database_name(&databases[0].name);
            Ok(client)
        }
        _ => {
            let input_name = get_db_name(&databases, &input_db_prompt(collection_name), term)?;
            // Verify the DB exists
            if !databases.iter().any(|db| db.name == input_name) {
                return Err(CliError::Db(crate::commands::db::DbError::DbNotFound(
                    input_name,
                )));
            }
            client.set_database_name(input_name);
            Ok(client)
        }
    }
}

fn is_local(args: &BrowseArgs) -> bool {
    args.local || args.local_chroma_args.host.is_some() || args.local_chroma_args.path.is_some()
}

pub fn browse(args: BrowseArgs) -> Result<(), CliError> {
    let store = FileConfigStore::default();
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {
        let (client, _handle) = if is_local(&args) {
            let (client, handle) = connect_local(args.local_chroma_args.clone()).await?;
            if let Some(db_name) = &args.db_name {
                let dbs = client.list_databases().await?;
                if !dbs.iter().any(|db| &db.name == db_name) {
                    return Err(CliError::Db(crate::commands::db::DbError::DbNotFound(
                        db_name.clone(),
                    )));
                }
                client.set_database_name(db_name);
            }
            (client, handle)
        } else {
            let client = get_cloud_client(
                args.db_name.clone(),
                &args.collection_name,
                &store,
                &mut term,
            )
            .await?;
            (client, None)
        };

        let collection = client
            .get_collection(args.collection_name.clone())
            .await
            .map_err(|_| BrowseError::CollectionNotFound(args.collection_name))?;

        let mut config = store.read_config()?;

        if let Some(theme) = args.theme {
            if config.theme != theme {
                config.theme = theme;
                store.write_config(&config)?;
            }
        }

        let mut collection_browser = CollectionBrowser::new(collection, config.theme);
        collection_browser
            .run()
            .await
            .map_err(|_| BrowseError::BrowserApp)?;

        Ok::<(), CliError>(())
    })?;
    Ok(())
}
