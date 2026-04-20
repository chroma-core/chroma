use crate::commands::browse::BrowseError;
use crate::commands::db::get_db_name;
use crate::commands::install::InstallError;
use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::utils::{
    cloud_client, connect_local, CliError, ErrorResponse, LocalChromaArgs, Profile, UtilsError,
};
use chroma::client::Database;
use chroma::ChromaHttpClient;
use chroma_types::operator::Key;
use chroma_types::plan::SearchPayload;
use clap::Parser;
use crossterm::style::Stylize;
use futures::{stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::{self, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;

#[derive(Debug)]
enum Environment {
    Local,
    Cloud,
}

impl Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Environment::Local => write!(f, "Local"),
            Environment::Cloud => write!(f, "Cloud"),
        }
    }
}

#[derive(Debug, Error)]
pub enum CopyError {
    #[error("Can only copy collections from local to Chroma Cloud or from Chroma Cloud to local")]
    InvalidSourceDestination,
    #[error("No collections to copy found")]
    NoCollections,
    #[error("Collection {0} already exists in target")]
    CollectionAlreadyExists(String),
}

#[derive(Parser, Debug)]
pub struct CopyArgs {
    #[clap(
        long = "all",
        conflicts_with = "collections",
        help = "Copy all collections"
    )]
    all: bool,
    #[clap(long = "collections", help = "The names of collections to copy")]
    collections: Vec<String>,
    #[clap(
        long = "from-local",
        conflicts_with_all = ["from_cloud", "to_local"],
        help = "Copy from a local Chroma server"
    )]
    from_local: bool,
    #[clap(long = "from-cloud", conflicts_with_all = ["to_cloud", "from_local"], help = "Copy from Chroma Cloud")]
    from_cloud: bool,
    #[clap(
        long = "to-local",
        conflicts_with = "to_cloud",
        help = "Copy to a local Chroma server"
    )]
    to_local: bool,
    #[clap(long = "to-cloud", help = "Copy to Chroma Cloud")]
    to_cloud: bool,
    #[clap(long = "db", help = "Chroma Cloud DB with the collections to copy")]
    db: Option<String>,
    #[clap(long = "host", conflicts_with_all = ["path"], help = "Local Chroma server host")]
    host: Option<String>,
    #[clap(long = "path", help = "Data path for your local Chroma server")]
    path: Option<String>,
    #[clap(
        long = "batch",
        default_value_t = 100,
        value_parser = clap::value_parser!(u32).range(1..=300),
        help = "Batch size for records when copying (min 1, max 300)"
    )]
    batch: u32,
    #[clap(
        long = "concurrent",
        default_value_t = 5,
        value_parser = clap::value_parser!(u32).range(1..=8),
        help = "Number of concurrent processes when copying (min 1, max 8)"
    )]
    concurrent: u32,
}

fn select_chroma_server_prompt() -> &'static str {
    "What Chroma server has the collection(s) you want to copy?"
}

fn select_db_prompt(from: bool) -> String {
    let direction = if from { "from" } else { "to" };
    format!("Which DB do you want to copy collections {}", direction)
}

fn start_copy_prompt(collections_num: usize) -> String {
    format!("Copying {} collection(s)", collections_num)
}

fn verify_db_exists(dbs: &[Database], name: &str) -> Result<(), CliError> {
    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(crate::commands::db::DbError::DbNotFound(
            name.to_string(),
        )));
    }
    Ok(())
}

async fn get_cloud_client(
    profile: Profile,
    db_name: Option<String>,
    from: bool,
    term: &mut dyn Terminal,
) -> Result<ChromaHttpClient, CliError> {
    let client = cloud_client(&profile)?;

    if let Some(db_name) = db_name {
        let dbs = client.list_databases().await?;
        verify_db_exists(&dbs, &db_name)?;
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
            let input_name = get_db_name(&databases, &select_db_prompt(from), term)?;
            verify_db_exists(&databases, &input_name)?;
            client.set_database_name(input_name);
            Ok(client)
        }
    }
}

async fn get_chroma_clients(
    args: &CopyArgs,
    source: Environment,
    target: Environment,
    profile: Profile,
    term: &mut dyn Terminal,
) -> Result<(ChromaHttpClient, ChromaHttpClient, Option<JoinHandle<()>>), CliError> {
    let local_args = LocalChromaArgs {
        host: args.host.clone(),
        path: args.path.clone(),
    };
    let (local_client, handle) = connect_local(local_args).await?;
    let cloud_client = get_cloud_client(profile, args.db.clone(), args.from_cloud, term).await?;

    match (source, target) {
        (Environment::Cloud, Environment::Local) => Ok((cloud_client, local_client, handle)),
        (Environment::Local, Environment::Cloud) => Ok((local_client, cloud_client, handle)),
        _ => Err(CopyError::InvalidSourceDestination.into()),
    }
}

fn get_target_and_destination(
    args: &CopyArgs,
    term: &mut dyn Terminal,
) -> Result<(Environment, Environment), CliError> {
    let (source, target) = match (
        args.from_cloud,
        args.from_local,
        args.to_cloud,
        args.to_local,
    ) {
        (true, _, _, _) => (Environment::Cloud, Environment::Local),
        (_, true, _, _) => (Environment::Local, Environment::Cloud),
        (_, _, true, _) => (Environment::Local, Environment::Cloud),
        (_, _, _, true) => (Environment::Cloud, Environment::Local),
        _ => {
            let prompt = select_chroma_server_prompt().bold().blue();
            term.println(&format!("{}", prompt));
            let options = vec![
                Environment::Cloud.to_string(),
                Environment::Local.to_string(),
            ];
            let selection = term.prompt_select(&options)?;
            term.println(&format!("{}\n", &options[selection]));
            match selection {
                0 => (Environment::Cloud, Environment::Local),
                _ => (Environment::Local, Environment::Cloud),
            }
        }
    };

    Ok((source, target))
}

async fn copy_collections(
    source: ChromaHttpClient,
    target: ChromaHttpClient,
    collections: Vec<String>,
    all: bool,
    step: u32,
    concurrent: u32,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let collections = if all {
        source.list_collections(10000, None).await?
    } else {
        let mut source_collections = vec![];
        for collection in collections {
            let source_collection = source.get_collection(&collection).await?;
            source_collections.push(source_collection);
        }
        source_collections
    };

    if collections.is_empty() {
        return Err(CopyError::NoCollections.into());
    }

    term.println(&format!(
        "{}",
        start_copy_prompt(collections.len()).bold().blue()
    ));

    term.println("Verifying collections...");
    // Verify that collections don't exist on target
    for collection in collections.clone() {
        if target.get_collection(collection.name()).await.is_ok() {
            return Err(CopyError::CollectionAlreadyExists(collection.name().to_string()).into());
        }
    }

    for collection in collections {
        let size = collection.count().await?;

        let offsets: Vec<u32> = (0..size).step_by(step as usize).collect();
        let records_added = Arc::new(AtomicUsize::new(0));

        let target_collection = target
            .create_collection(
                collection.name(),
                collection.schema().clone(),
                collection.metadata().clone(),
            )
            .await?;

        term.println(&format!("Copying collection: {}", collection.name()));

        let collection_progress = ProgressBar::new(size as u64);
        collection_progress.set_style(
            ProgressStyle::default_bar()
                .template("{bar:40.cyan/blue} {pos}/{len}")
                .unwrap()
                .progress_chars("◼◼-"),
        );

        stream::iter(offsets.into_iter().map(|offset| {
            let collection = collection.clone();
            let target_collection = target_collection.clone();
            let records_added = records_added.clone();
            let collection_progress = collection_progress.clone();

            async move {
                let search = SearchPayload::default().limit(Some(step), offset).select([
                    Key::Document,
                    Key::Embedding,
                    Key::Metadata,
                ]);

                let response = collection.search(vec![search]).await?;

                let ids = response.ids.into_iter().next().unwrap_or_default();
                if ids.is_empty() {
                    return Ok::<(), CliError>(());
                }

                let num_records = ids.len();
                let documents = response.documents.into_iter().next().flatten();
                let embeddings: Vec<Vec<f32>> = response
                    .embeddings
                    .into_iter()
                    .next()
                    .flatten()
                    .unwrap_or_default()
                    .into_iter()
                    .flatten()
                    .collect();
                let metadatas = response.metadatas.into_iter().next().flatten();

                target_collection
                    .add(ids, embeddings, documents, None, metadatas)
                    .await
                    .map_err(|e| {
                        if e.to_string().to_lowercase().contains("quota") {
                            let msg = serde_json::from_str::<ErrorResponse>(&e.to_string())
                                .unwrap_or_default()
                                .message;
                            return CliError::Utils(UtilsError::Quota(msg));
                        }
                        CliError::ChromaClient(e)
                    })?;

                let current_added =
                    records_added.fetch_add(num_records, Ordering::Relaxed) + num_records;
                collection_progress.set_position(current_added as u64);

                Ok(())
            }
        }))
        .buffer_unordered(concurrent as usize)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<(), CliError>>()?;

        collection_progress.finish();
    }

    term.println("Copy Completed!");

    Ok(())
}

pub fn copy(args: CopyArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {
        if !args.all && args.collections.is_empty() {
            return Err(CopyError::NoCollections.into());
        }

        let store = FileConfigStore::default();
        let (_, profile) = store.get_current_profile()?;
        let (source, target) = get_target_and_destination(&args, &mut term)?;
        let (source_client, target_client, _handle) =
            get_chroma_clients(&args, source, target, profile, &mut term).await?;
        copy_collections(
            source_client,
            target_client,
            args.collections,
            args.all,
            args.batch,
            args.concurrent,
            &mut term,
        )
        .await?;
        Ok::<(), CliError>(())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;

    fn default_args() -> CopyArgs {
        CopyArgs {
            all: false,
            collections: vec![],
            from_local: false,
            from_cloud: false,
            to_local: false,
            to_cloud: false,
            db: None,
            host: None,
            path: None,
            batch: 100,
            concurrent: 5,
        }
    }

    #[test]
    fn test_get_target_and_destination_from_cloud() {
        let mut args = default_args();
        args.from_cloud = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));
    }

    #[test]
    fn test_get_target_and_destination_from_local() {
        let mut args = default_args();
        args.from_local = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }

    #[test]
    fn test_get_target_and_destination_to_cloud() {
        let mut args = default_args();
        args.to_cloud = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }

    #[test]
    fn test_get_target_and_destination_to_local() {
        let mut args = default_args();
        args.to_local = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));
    }

    #[test]
    fn test_get_target_and_destination_interactive() {
        let args = default_args();
        // Select index 0 = Cloud (source), so target = Local
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));

        // Select index 1 = Local (source), so target = Cloud
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }
}
