use crate::client::admin_client::AdminClient;
use crate::client::chroma_client::{ChromaClient, ChromaClientError};
use crate::client::collection::CollectionAPIError;
use crate::commands::browse::BrowseError;
use crate::commands::db::get_db_name;
use crate::commands::install::InstallError;
use crate::utils::{
    get_current_profile, parse_host, parse_local, parse_path, AddressBook, CliError, Environment,
    ErrorResponse, Profile, UtilsError,
};
use chroma_types::{CollectionConfiguration, IncludeList};
use clap::Parser;
use crossterm::style::Stylize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Select;
use indicatif::{ProgressBar, ProgressStyle};
use thiserror::Error;
use tokio::task::JoinHandle;

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

async fn get_cloud_client(
    profile: Profile,
    db_name: Option<String>,
    from: bool,
) -> Result<ChromaClient, CliError> {
    let host = AddressBook::cloud().frontend_url;
    let admin_client = AdminClient::from_profile(host, &profile);

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
            let input_name = get_db_name(&databases, &select_db_prompt(from))?;
            let _verified = admin_client.get_database(input_name.clone()).await?;
            Ok(ChromaClient::with_admin_client(admin_client, input_name))
        }
    }
}

async fn get_local_client(
    host: &Option<String>,
    path: &Option<String>,
) -> Result<(ChromaClient, Option<JoinHandle<()>>), CliError> {
    let (admin_client, handle) = if host.is_some() {
        (parse_host(host.clone().unwrap_or_default()).await?, None)
    } else if path.is_some() {
        let (client, handle) = parse_path(path.clone().unwrap_or_default()).await?;
        (client, Some(handle))
    } else {
        let client = parse_local().await?;
        (client, None)
    };

    let chroma_client =
        ChromaClient::with_admin_client(admin_client, String::from("default_database"));
    Ok((chroma_client, handle))
}

async fn get_chroma_clients(
    args: &CopyArgs,
    source: Environment,
    target: Environment,
    profile: Profile,
) -> Result<(ChromaClient, ChromaClient, Option<JoinHandle<()>>), CliError> {
    let (local_client, handle) = get_local_client(&args.host, &args.path).await?;
    let cloud_client = get_cloud_client(profile, args.db.clone(), args.from_cloud).await?;

    match (source, target) {
        (Environment::Cloud, Environment::Local) => Ok((cloud_client, local_client, handle)),
        (Environment::Local, Environment::Cloud) => Ok((local_client, cloud_client, handle)),
        _ => Err(CopyError::InvalidSourceDestination.into()),
    }
}

fn get_target_and_destination(args: &CopyArgs) -> Result<(Environment, Environment), CliError> {
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
            println!("{}", prompt);
            let options = vec![Environment::Cloud, Environment::Local];
            let selection = Select::with_theme(&ColorfulTheme::default())
                .items(&options)
                .default(0)
                .interact()
                .map_err(|_| UtilsError::UserInputFailed)?;
            let selected_option = &options[selection];
            println!("{}\n", selected_option);
            match selected_option {
                Environment::Cloud => (Environment::Cloud, Environment::Local),
                Environment::Local => (Environment::Local, Environment::Cloud),
            }
        }
    };

    Ok((source, target))
}

async fn copy_collections(
    source: ChromaClient,
    target: ChromaClient,
    collections: Vec<String>,
    all: bool,
) -> Result<(), CliError> {
    let collections = if all {
        source
            .list_collections()
            .await
            .map_err(|_| ChromaClientError::ListCollections)?
    } else {
        let mut source_collections = vec![];
        for collection in collections {
            let source_collection = source
                .get_collection(collection.clone())
                .await
                .map_err(|_| ChromaClientError::CollectionGet(collection))?;
            source_collections.push(source_collection);
        }
        source_collections
    };

    if collections.is_empty() {
        return Err(CopyError::NoCollections.into());
    }

    println!("{}", start_copy_prompt(collections.len()).bold().blue());

    // Verify that collections don't exist on target
    for collection in collections.clone() {
        if target.get_collection(collection.name.clone()).await.is_ok() {
            return Err(CopyError::CollectionAlreadyExists(collection.name.clone()).into());
        }
    }

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    progress.set_message(String::from("Verifying collections..."));
    progress.enable_steady_tick(std::time::Duration::from_millis(100));

    for collection in collections {
        let size = collection
            .count()
            .await
            .map_err(|_| CollectionAPIError::Count(collection.name.clone()))?;

        let target_collection = target
            .create_collection(
                collection.name.clone(),
                collection.metadata.clone(),
                Some(CollectionConfiguration::from(collection.config.clone())),
            )
            .await
            .map_err(|_| ChromaClientError::CreateCollection(collection.name.clone()))?;

        let message = format!("Copying collection: {} ({} records)", collection.name, size);
        progress.set_message(message);

        for i in (0..(size + 1)).step_by(100) {
            let records = collection
                .get(
                    None,
                    None,
                    None,
                    Some(IncludeList::all()),
                    Some(100),
                    Some(i),
                )
                .await
                .map_err(|_| ChromaClientError::CollectionGet(collection.name.clone()))?;

            target_collection
                .add(
                    records.ids,
                    records.embeddings,
                    records.documents,
                    records.uris,
                    records.metadatas,
                )
                .await
                .map_err(|e| {
                    if e.to_string().contains("Quota") {
                        let msg = serde_json::from_str::<ErrorResponse>(&e.to_string())
                            .unwrap_or_default()
                            .message;
                        return CliError::Utils(UtilsError::Quota(msg));
                    }
                    CliError::Collection(CollectionAPIError::Add(collection.name.clone()))
                })?;
        }
    }

    println!("Copy Completed!");

    Ok(())
}

pub fn copy(args: CopyArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {
        if !args.all && args.collections.is_empty() {
            return Err(CopyError::NoCollections.into());
        }

        let (_, profile) = get_current_profile()?;
        let (source, target) = get_target_and_destination(&args)?;
        let (source_client, target_client, _handle) =
            get_chroma_clients(&args, source, target, profile).await?;
        copy_collections(source_client, target_client, args.collections, args.all).await?;
        Ok::<(), CliError>(())
    })?;
    Ok(())
}
