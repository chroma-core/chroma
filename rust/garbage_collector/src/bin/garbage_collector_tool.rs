use chroma_blockstore::RootManager;
use chroma_config::Configurable;
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_storage::StorageError;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_system::Dispatcher;
use chroma_system::Orchestrator;
use chroma_types::chroma_proto::CollectionVersionFile;
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
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::collections::HashSet;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Debug, clap::ValueEnum, Clone)]
enum CliCleanupMode {
    DryRunV2,
    DeleteV2,
}

impl From<CliCleanupMode> for CleanupMode {
    fn from(mode: CliCleanupMode) -> Self {
        match mode {
            CliCleanupMode::DryRunV2 => CleanupMode::DryRunV2,
            CliCleanupMode::DeleteV2 => CleanupMode::DeleteV2,
        }
    }
}

#[derive(Debug, Parser)]
enum GarbageCollectorCommand {
    /// Manually run garbage collection on a specific collection
    GarbageCollect {
        #[arg(short, long)]
        config_path: String,
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

    /// Export collections from the sysdb to a file.
    /// Requires the `DATABASE_URL` environment variable to be set.
    #[command(name = "export-sysdb-collections")]
    ExportSysDbCollections {
        #[arg(long)]
        collection_id: String,
        /// If true, include all collections where `root_collection_id` matches the given `collection_id`.
        #[arg(long, default_value = "false")]
        include_all_children: bool,
        #[arg(long)]
        output_file: PathBuf,
    },

    /// Import collections from a file created by `export-sysdb-collections` into the sysdb.
    /// Requires the `DATABASE_URL` environment variable to be set.
    #[command(name = "import-sysdb-collections")]
    ImportSysDbCollections {
        #[arg(long)]
        input_file: PathBuf,
    },

    /// Download all files from object storage associated with a given set of collections.
    DownloadCollections {
        #[arg(short, long)]
        config_path: String,
        /// Path to the file containing exported collections in JSON format.
        /// This should have been created by `export-sysdb-collections`.
        #[arg(long)]
        exported_collections_path: PathBuf,
        #[arg(long)]
        output_directory: PathBuf,
        #[arg(long, default_value = "false")]
        include_blocks: bool,
        #[arg(short, long, default_value = "10")]
        download_concurrency: usize,
    },

    // Manually collect the given collection-id.
    #[command(name = "manual-collection")]
    ManualCollection {
        #[arg(long)]
        collection_id: String,
    },
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: GarbageCollectorCommand,
}

async fn download_file(
    storage_client: &Storage,
    storage_key: &str,
    output_directory: &Path,
) -> Result<Option<Arc<Vec<u8>>>, Box<dyn std::error::Error>> {
    let output_path = output_directory.join(storage_key);

    if output_path.exists() {
        let data = std::fs::read(output_path)?;
        return Ok(Some(Arc::new(data)));
    }

    match storage_client.get(storage_key, Default::default()).await {
        Ok(data) => {
            std::fs::create_dir_all(output_path.parent().unwrap())?;
            std::fs::write(output_path, data.as_ref())?;
            Ok(Some(data))
        }
        Err(StorageError::NotFound { .. }) => Ok(None),
        Err(e) => Err(Box::new(e)),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SerializedCollection {
    id: String,
    name: String,
    dimension: Option<i32>,
    database_id: String,
    is_deleted: bool,
    log_position: i64,
    version: i32,
    configuration_json_str: String,
    total_records_post_compaction: i64,
    size_bytes_post_compaction: i64,
    last_compaction_time_secs: i64,
    version_file_name: String,
    lineage_file_name: Option<String>,
    root_collection_id: Option<String>,
    tenant: String,
    num_versions: i32,
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
            config_path,
        } => {
            let collection_id = collection_id.parse().expect("Invalid collection ID format");

            let system = chroma_system::System::new();
            let registry = chroma_config::registry::Registry::new();
            let config = GarbageCollectorConfig::load_from_path(&config_path);

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
                10,
            );

            let result = orchestrator.run(system.clone()).await;

            system.stop().await;
            system.join().await;

            match result {
                Ok(_) => tracing::info!("Garbage collection completed successfully."),
                Err(e) => tracing::error!("Garbage collection failed: {}", e),
            }
        }

        GarbageCollectorCommand::DownloadCollections {
            exported_collections_path,
            output_directory,
            include_blocks,
            download_concurrency,
            config_path,
        } => {
            if include_blocks {
                unimplemented!("Downloading collections with blocks is not yet implemented");
            }

            let registry = chroma_config::registry::Registry::new();
            let config = GarbageCollectorConfig::load_from_path(&config_path);

            let storage_client = Storage::try_from_config(&config.storage_config, &registry)
                .await
                .expect("Failed to create storage client");

            let object_store_output_directory = output_directory.join("object_store");

            let collections_file = std::fs::read_to_string(&exported_collections_path)
                .expect("Failed to read exported collections file");
            let collections: Vec<SerializedCollection> = collections_file
                .lines()
                .map(|line| serde_json::from_str(line).expect("Failed to parse collection JSON"))
                .collect();

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
                            &collection.version_file_name,
                            object_store_output_directory,
                        )
                        .await
                        .expect("Failed to download version file")
                        .expect("Version file not found");

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

                        if let Some(lineage_file_path) = &collection.lineage_file_name {
                            download_file(
                                &storage_client,
                                lineage_file_path,
                                object_store_output_directory,
                            )
                            .await
                            .expect("Failed to download lineage file");
                        }

                        bar.inc(1);

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

            let missing_sparse_indices_count = Arc::new(AtomicUsize::new(0));
            let total_sparse_indices_count = sparse_indices_to_fetch.len();
            futures::stream::iter(sparse_indices_to_fetch)
                .map(|path| {
                    let storage_client = storage_client.clone();
                    let object_store_output_directory = &object_store_output_directory;
                    let bar = bar.clone();
                    let missing_sparse_indices_count = Arc::clone(&missing_sparse_indices_count);
                    async move {
                        let result =
                            download_file(&storage_client, &path, object_store_output_directory)
                                .await
                                .expect("Failed to download sparse index file");

                        if result.is_none() {
                            missing_sparse_indices_count
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }

                        bar.inc(1);
                    }
                })
                .buffer_unordered(download_concurrency)
                .collect::<()>()
                .await;

            bar.finish();

            let missing_count =
                missing_sparse_indices_count.load(std::sync::atomic::Ordering::SeqCst);
            if missing_count > 0 {
                tracing::warn!(
                    "{} sparse indices out of {} were not found.",
                    missing_count,
                    total_sparse_indices_count,
                );
            }
        }

        GarbageCollectorCommand::ExportSysDbCollections {
            collection_id,
            include_all_children,
            output_file,
        } => {
            let database_url = std::env::var("DATABASE_URL")
                .expect("DATABASE_URL environment variable is not set.");

            let pool = PgPoolOptions::new()
                .connect(&database_url)
                .await
                .expect("Failed to connect to database");

            let mut rows = sqlx::query("SELECT * FROM collections WHERE id = $1")
                .bind(&collection_id)
                .fetch_all(&pool)
                .await
                .expect("Failed to fetch collection from database");

            if include_all_children {
                let children =
                    sqlx::query("SELECT * FROM collections WHERE root_collection_id = $1")
                        .bind(&collection_id)
                        .fetch_all(&pool)
                        .await
                        .expect("Failed to fetch child collections from database");
                rows.extend(children);
            }

            let mut file =
                std::fs::File::create(&output_file).expect("Failed to create output file");

            let len = rows.len();
            for row in rows {
                let collection = SerializedCollection {
                    id: row.get::<String, _>("id"),
                    name: row.get::<String, _>("name"),
                    configuration_json_str: row.get::<String, _>("configuration_json_str"),
                    dimension: row.get::<Option<i32>, _>("dimension"),
                    tenant: row.get::<String, _>("tenant"),
                    log_position: row.get::<i64, _>("log_position"),
                    version: row.get::<i32, _>("version"),
                    total_records_post_compaction: row
                        .get::<i64, _>("total_records_post_compaction"),
                    size_bytes_post_compaction: row.get::<i64, _>("size_bytes_post_compaction"),
                    last_compaction_time_secs: row.get::<i64, _>("last_compaction_time_secs"),
                    version_file_name: row.get::<String, _>("version_file_name"),
                    root_collection_id: row.get::<Option<String>, _>("root_collection_id"),
                    lineage_file_name: row.get::<Option<String>, _>("lineage_file_name"),
                    is_deleted: row.get::<bool, _>("is_deleted"),
                    database_id: row.get::<String, _>("database_id"),
                    num_versions: row.get::<i32, _>("num_versions"),
                };

                let collection_json = serde_json::to_string(&collection)
                    .expect("Failed to serialize collection to JSON");
                writeln!(file, "{}", collection_json)
                    .expect("Failed to write collection to output file");
            }

            tracing::info!("Exported {} collections to {}", len, output_file.display());
        }

        GarbageCollectorCommand::ManualCollection { collection_id } => {
            // Connect to the garbage collector service
            let gc_endpoint = std::env::var("GARBAGE_COLLECTOR_ENDPOINT")
                .unwrap_or_else(|_| "http://[::1]:50055".to_string());

            tracing::info!("Connecting to garbage collector at {}", gc_endpoint);

            let mut client = chroma_types::chroma_proto::garbage_collector_client::GarbageCollectorClient::connect(gc_endpoint)
                .await
                .expect("Failed to connect to garbage collector service");

            let request = tonic::Request::new(
                chroma_types::chroma_proto::KickoffGarbageCollectionRequest {
                    collection_id: collection_id.clone(),
                },
            );

            match client.kickoff_garbage_collection(request).await {
                Ok(_) => {
                    tracing::info!(
                        "Successfully triggered manual garbage collection for collection {}",
                        collection_id
                    );
                }
                Err(e) => {
                    tracing::error!("Failed to trigger manual garbage collection: {}", e);
                }
            }
        }

        GarbageCollectorCommand::ImportSysDbCollections { input_file } => {
            let database_url = std::env::var("DATABASE_URL")
                .expect("DATABASE_URL environment variable is not set.");

            let pool = PgPoolOptions::new()
                .connect(&database_url)
                .await
                .expect("Failed to connect to database");

            let file =
                std::fs::File::open(&input_file).expect("Failed to open input file for reading");
            let reader = std::io::BufReader::new(file);

            let mut created_database_ids = HashSet::new();
            let mut created_tenant_ids = HashSet::new();

            let mut num_imported = 0;
            for line in reader.lines() {
                let line = line.expect("Failed to read line from input file");
                let collection: SerializedCollection =
                    serde_json::from_str(&line).expect("Failed to parse collection JSON");

                if !created_tenant_ids.contains(&collection.tenant) {
                    sqlx::query("INSERT INTO tenants (id, last_compaction_time) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                        .bind(&collection.tenant)
                        .bind(0)
                        .execute(&pool)
                        .await
                        .expect("Failed to insert tenant in database");
                    created_tenant_ids.insert(collection.tenant.clone());
                }

                if !created_database_ids.contains(&collection.database_id) {
                    sqlx::query(
                        "INSERT INTO databases (id, name, tenant_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    )
                    .bind(&collection.database_id)
                    .bind(format!("imported_{}", &collection.database_id))
                    .bind(&collection.tenant)
                    .execute(&pool)
                    .await
                    .expect("Failed to insert database in database");
                    created_database_ids.insert(collection.database_id.clone());
                }

                sqlx::query(
                    "INSERT INTO collections (
                        id,
                        name,
                        dimension,
                        database_id,
                        is_deleted,
                        log_position,
                        version,
                        configuration_json_str,
                        total_records_post_compaction,
                        size_bytes_post_compaction,
                        last_compaction_time_secs,
                        version_file_name,
                        lineage_file_name,
                        root_collection_id,
                        tenant,
                        num_versions
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ON CONFLICT DO NOTHING",
                )
                .bind(&collection.id)
                .bind(&collection.name)
                .bind(collection.dimension)
                .bind(&collection.database_id)
                .bind(collection.is_deleted)
                .bind(collection.log_position)
                .bind(collection.version)
                .bind(&collection.configuration_json_str)
                .bind(collection.total_records_post_compaction)
                .bind(collection.size_bytes_post_compaction)
                .bind(collection.last_compaction_time_secs)
                .bind(&collection.version_file_name)
                .bind(collection.lineage_file_name)
                .bind(collection.root_collection_id)
                .bind(&collection.tenant)
                .bind(collection.num_versions)
                .execute(&pool)
                .await
                .expect("Failed to insert collection in database");

                num_imported += 1;
            }

            tracing::info!(
                "Imported {} collections from {}",
                num_imported,
                input_file.display()
            );
        }
    }
}
