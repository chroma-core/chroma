use chroma_blockstore::RootManager;
use chroma_config::Configurable;
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_system::Dispatcher;
use chroma_system::Orchestrator;
use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use garbage_collector_library::{
    config::GarbageCollectorConfig,
    garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator, types::CleanupMode,
};

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
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config_path: String,

    #[command(subcommand)]
    command: GarbageCollectorCommand,
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
    }
}
