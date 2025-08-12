use chroma_blockstore::RootManager;
use chroma_config::Configurable;
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_system::Dispatcher;
use chroma_system::Orchestrator;
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::CollectionUuid;
use chroma_types::Segment;
use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use futures::StreamExt;
use garbage_collector_library::{
    config::GarbageCollectorConfig,
    garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator, types::CleanupMode,
};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use prost::Message;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, clap::ValueEnum, Clone)]
enum CliCleanupMode {
    DryRun,
    Rename,
    Delete,
    DryRunV2,
    DeleteV2,
}

impl From<CliCleanupMode> for CleanupMode {
    fn from(mode: CliCleanupMode) -> Self {
        match mode {
            CliCleanupMode::DryRun => CleanupMode::DryRun,
            CliCleanupMode::Rename => CleanupMode::Rename,
            CliCleanupMode::Delete => CleanupMode::Delete,
            CliCleanupMode::DryRunV2 => CleanupMode::DryRunV2,
            CliCleanupMode::DeleteV2 => CleanupMode::DeleteV2,
        }
    }
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct CollectionIdsSource {
    #[clap(long)]
    collection_id: Option<String>,
    #[clap(long)]
    collection_ids: Option<Vec<String>>,
    #[clap(long)]
    read_collection_ids_stdin: Option<bool>,
}

impl CollectionIdsSource {
    fn get_collection_ids(&self) -> HashSet<CollectionUuid> {
        if self.read_collection_ids_stdin.unwrap_or(false) {
            let mut collection_ids = String::new();
            std::io::stdin()
                .read_line(&mut collection_ids)
                .expect("Failed to read from stdin");
            collection_ids
                .trim()
                .split(',')
                .map(|id| {
                    CollectionUuid::from_str(id.trim()).expect("Invalid collection ID format")
                })
                .collect()
        } else if let Some(id) = &self.collection_id {
            HashSet::from([CollectionUuid::from_str(id).expect("Invalid collection ID format")])
        } else if let Some(ids) = &self.collection_ids {
            ids.iter()
                .map(|id| CollectionUuid::from_str(id).expect("Invalid collection ID format"))
                .collect()
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, Parser)]
enum GarbageCollectorCommand {
    /// Manually run garbage collection on a specific collection
    GarbageCollect {
        #[arg(long)]
        collection_id: String,
        #[arg(long, default_value = "true")]
        enable_log_gc: bool,
        #[arg(long, default_value = "false")]
        enable_dangerous_option_to_ignore_min_versions_for_wal3: bool,
        #[arg(long)]
        cleanup_mode: CliCleanupMode,
        #[arg(long)]
        version_absolute_cutoff_time: DateTime<Utc>,
        #[arg(long)]
        collection_soft_delete_absolute_cutoff_time: DateTime<Utc>,
    },

    DownloadCollection {
        #[clap(flatten)]
        collection_id_source: CollectionIdsSource,
        #[arg(long)]
        output_directory: PathBuf,
        #[arg(long, default_value = "false")]
        include_blocks: bool,
        #[arg(short, long, default_value = "10")]
        download_concurrency: usize,
    },
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config_path: String,

    #[command(subcommand)]
    command: GarbageCollectorCommand,
}

async fn download_file(
    storage_client: &Storage,
    storage_key: &str,
    output_directory: &PathBuf,
) -> Result<Arc<Vec<u8>>, Box<dyn std::error::Error>> {
    let output_path = output_directory.join(storage_key);

    if output_path.exists() {
        let data = std::fs::read(output_path)?;
        return Ok(Arc::new(data));
    }

    let data = storage_client.get(storage_key, Default::default()).await?;
    std::fs::create_dir_all(output_path.parent().unwrap())?;
    std::fs::write(output_path, data.as_ref())?;
    Ok(data)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    match args.command {
        GarbageCollectorCommand::GarbageCollect {
            collection_id,
            enable_log_gc,
            enable_dangerous_option_to_ignore_min_versions_for_wal3,
            cleanup_mode,
            version_absolute_cutoff_time,
            collection_soft_delete_absolute_cutoff_time,
        } => {
            let collection_id = collection_id.parse().expect("Invalid collection ID format");

            let system = chroma_system::System::new();
            let registry = chroma_config::registry::Registry::new();
            let config = GarbageCollectorConfig::load_from_path(&args.config_path);

            let log_client = Log::try_from_config(&(config.log, system.clone()), &registry)
                .await
                .unwrap();

            let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
                .await
                .expect("Failed to create dispatcher from config");

            let dispatcher_handle = system.start_component(dispatcher);

            let storage_client = Storage::try_from_config(&config.storage_config, &registry)
                .await
                .expect("Failed to create storage client");

            let mut sysdb_client = SysDb::try_from_config(
                &chroma_sysdb::SysDbConfig::Grpc(config.sysdb_config),
                &registry,
            )
            .await
            .expect("Failed to create sysdb client");

            let root_manager_cache =
                chroma_cache::from_config_persistent(&config.root_cache_config)
                    .await
                    .unwrap();
            let root_manager = RootManager::new(storage_client.clone(), root_manager_cache);

            let mut collections = sysdb_client
                .get_collections(GetCollectionsOptions {
                    collection_id: Some(collection_id),
                    ..Default::default()
                })
                .await
                .unwrap();
            if collections.is_empty() {
                tracing::error!("No collection found with ID: {}", collection_id);
                return;
            }
            if collections.len() > 1 {
                tracing::error!(
                    "Multiple collections returned when querying for ID: {}",
                    collection_id
                );
                return;
            }

            let collection = collections.pop().unwrap();

            let orchestrator = GarbageCollectorOrchestrator::new(
                collection_id,
                collection.version_file_path.unwrap(),
                collection.lineage_file_path,
                version_absolute_cutoff_time,
                collection_soft_delete_absolute_cutoff_time,
                sysdb_client,
                dispatcher_handle,
                system.clone(),
                storage_client,
                log_client,
                root_manager,
                cleanup_mode.into(),
                config.min_versions_to_keep,
                enable_log_gc,
                enable_dangerous_option_to_ignore_min_versions_for_wal3,
            );

            orchestrator.run(system.clone()).await.unwrap();
        }

        GarbageCollectorCommand::DownloadCollection {
            collection_id_source,
            output_directory,
            include_blocks,
            download_concurrency,
        } => {
            if include_blocks {
                unimplemented!("Downloading collections with blocks is not yet implemented");
            }

            let registry = chroma_config::registry::Registry::new();
            let config = GarbageCollectorConfig::load_from_path(&args.config_path);

            let storage_client = Storage::try_from_config(&config.storage_config, &registry)
                .await
                .expect("Failed to create storage client");

            let mut sysdb_client = SysDb::try_from_config(
                &chroma_sysdb::SysDbConfig::Grpc(config.sysdb_config),
                &registry,
            )
            .await
            .expect("Failed to create sysdb client");

            let collection_ids = collection_id_source.get_collection_ids();

            let collections = sysdb_client
                .get_collections(GetCollectionsOptions {
                    collection_ids: Some(collection_ids.iter().cloned().collect()),
                    ..Default::default()
                })
                .await
                .expect("Failed to get collections");

            if collections.len() != collection_ids.len() {
                tracing::error!(
                    "Expected {} collections, but found {}",
                    collection_ids.len(),
                    collections.len()
                );
                return;
            }

            let object_store_output_directory = output_directory.join("object_store");
            let collections_jsonl_path = output_directory.join("collections.jsonl");

            std::fs::create_dir_all(&collections_jsonl_path.parent().unwrap())
                .expect("Failed to create output directory");

            let mut collections_jsonl_file = std::fs::File::create(&collections_jsonl_path)
                .expect("Failed to create JSONL file");

            for collection in &collections {
                let collection_json = serde_json::to_string(collection)
                    .expect("Failed to serialize collection to JSON");
                writeln!(collections_jsonl_file, "{}", collection_json)
                    .expect("Failed to write to JSONL file");
            }

            let bar = ProgressBar::new(collections.len() as u64);
            bar.set_style(
                ProgressStyle::default_spinner()
                    .template("{msg} {spinner} {bar:40.cyan/blue} {pos}/{len} {eta}")
                    .unwrap(),
            );
            bar.enable_steady_tick(std::time::Duration::from_millis(100));
            bar.set_message("Downloading version and lineage files...");

            let sparse_indices_to_fetch = futures::stream::iter(collections)
                .map(|collection| {
                    let storage_client = storage_client.clone();
                    let object_store_output_directory = &object_store_output_directory;
                    let bar = bar.clone();
                    async move {
                        let version_file = download_file(
                            &storage_client,
                            collection.version_file_path.as_ref().unwrap(),
                            &object_store_output_directory,
                        )
                        .await
                        .expect("Failed to download version file");

                        let decoded_version_file =
                            CollectionVersionFile::decode(version_file.as_slice()).unwrap();

                        let mut sparse_indices_to_fetch = HashSet::new();

                        for version in decoded_version_file.version_history.unwrap().versions {
                            for segment in version.segment_info.unwrap().segment_compaction_info {
                                for (_, value) in segment
                                    .file_paths
                                    .into_iter()
                                    .filter(|(k, _)| !k.contains("hnsw"))
                                {
                                    for path in value.paths {
                                        let (prefix, id) =
                                            Segment::extract_prefix_and_id(&path).unwrap();
                                        sparse_indices_to_fetch
                                            .insert(RootManager::get_storage_key(prefix, &id));
                                    }
                                }
                            }
                        }

                        if let Some(lineage_file_path) = &collection.lineage_file_path {
                            download_file(
                                &storage_client,
                                lineage_file_path,
                                &object_store_output_directory,
                            )
                            .await
                            .expect("Failed to download lineage file");
                        }

                        bar.inc(1);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                        futures::stream::iter(sparse_indices_to_fetch)
                    }
                })
                .buffer_unordered(download_concurrency)
                .flatten()
                .collect::<HashSet<String>>()
                .await;

            bar.reset();
            bar.set_length(sparse_indices_to_fetch.len() as u64);
            bar.set_message("Fetching sparse indices...");

            futures::stream::iter(sparse_indices_to_fetch)
                .map(|path| {
                    let storage_client = storage_client.clone();
                    let object_store_output_directory = &object_store_output_directory;
                    let bar = bar.clone();
                    async move {
                        download_file(&storage_client, &path, &object_store_output_directory)
                            .await
                            .expect("Failed to download sparse index file");

                        bar.inc(1);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                })
                .buffer_unordered(download_concurrency)
                .collect::<()>()
                .await;

            bar.finish();
        }
    }
}
