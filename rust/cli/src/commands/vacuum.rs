use std::{fs, io};
use std::error::Error;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use clap::Parser;
use colored::Colorize;
use dialoguer::Confirm;
use sqlx::Row;
use chroma_config::Configurable;
use chroma_config::registry::Registry;
use chroma_frontend::frontend::Frontend;
use chroma_frontend::{frontend_service_entrypoint_with_config, FrontendConfig};
use chroma_log::Log;
use chroma_log::sqlite_log::SqliteLog;
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_system::System;
use chroma_sqlite::db::{SqliteDb};
use chroma_sysdb::SysDb;
use chroma_types::{CollectionUuid, GetCollectionRequest, ListCollectionsRequest};
use crate::utils::{get_frontend_config, LocalFrontendCommandArgs, DEFAULT_PERSISTENT_PATH, SQLITE_FILENAME};

#[derive(Parser, Debug)]
pub struct VacuumArgs {
    #[clap(flatten)]
    pub frontend_args: LocalFrontendCommandArgs,
    #[arg(long)]
    pub force: bool,
}

fn get_dir_size(path: &Path) -> io::Result<u64> {
    let mut total_size = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = fs::metadata(&entry_path)?;
        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            total_size += get_dir_size(&entry_path)?;
        }
    }
    Ok(total_size)
}

pub async fn trigger_vector_segments_max_seq_id_migration(
    sqlite: &SqliteDb,
    sysdb: &mut SysDb,
    segment_manager: &LocalSegmentManager
) -> Result<(), Box<dyn Error>> {
    let rows = sqlx::query(
        r#"
                SELECT collection FROM "segments"
                WHERE "id" NOT IN (SELECT "segment_id" FROM "max_seq_id") AND "type" = 'urn:chroma:segment/vector/hnsw-local-persisted'
            "#,
    ).fetch_all(sqlite.get_conn()).await?;

    let collection_ids: Result<Vec<CollectionUuid>, _> = rows
        .into_iter()
        .map(|row| CollectionUuid::from_str(row.get::<&str, _>(0)))
        .collect();

    let collection_ids = collection_ids?;
    
    for collection_id in collection_ids {
        let collection = sysdb.get_collection_with_segments(collection_id).await?;

        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection.collection.dimension {
            Some(dim) => dim,
            None => continue,
        };

        segment_manager.get_hnsw_writer(&collection.vector_segment, dim as usize).await?;
    }
    
    Ok(())
}

pub async fn vacuum_chroma(config: FrontendConfig) -> Result<(), Box<dyn Error>> {
    let system = System::new();
    let registry = Registry::new();
    let mut frontend = Frontend::try_from_config(&(config, system), &registry).await?;
    
    let sqlite = registry.get::<SqliteDb>()?;
    let segment_manager = registry.get::<LocalSegmentManager>()?;
    let mut sysdb = registry.get::<SysDb>()?;
    let mut log = registry.get::<Log>()?;
    
    trigger_vector_segments_max_seq_id_migration(&sqlite, &mut sysdb, &segment_manager).await?;
    
    let tenant = String::from("default");
    let database = String::from("default");
    
    let list_collections_request = ListCollectionsRequest::try_new(tenant, database, None, 0)?;
    let collections = frontend.list_collections(list_collections_request).await?;

    for collection in collections {
        let seq_ids = sqlx::query(
            r#"
                SELECT COALESCE(max_seq_id.seq_id, -1) AS seq_id
                FROM segments
                    LEFT JOIN max_seq_id ON segments.id = max_seq_id.segment_id
                WHERE segments.collection = ?
            "#,
        ).fetch_all(sqlite.get_conn()).await?;

        let min_seq_id: Option<u64> = seq_ids
            .iter()
            .map(|row| row.get(0))
            .min()
            .unwrap_or(None);
        
        if min_seq_id.is_none() {
            continue;
        }
        
        log.purge_logs(collection.collection_id, min_seq_id.unwrap()).await?;
    }

    sqlx::query(&format!("PRAGMA busy_timeout = {}", 5000))
        .execute(sqlite.get_conn())
        .await?;
    
    sqlx::query("VACUUM")
        .execute(sqlite.get_conn())
        .await?;
    
    sqlx::query(
        "INSERT INTO maintenance_log (operation, timestamp)
         VALUES ('vacuum', CURRENT_TIMESTAMP)"
    )
        .execute(sqlite.get_conn())
        .await?;
    
    Ok(())
}

pub fn vacuum(args: VacuumArgs) {
    // Vacuum the database. This may result in a small increase in performance.
    // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
    // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    let config = match get_frontend_config(
        args.frontend_args.config_path,
        args.frontend_args.persistent_path,
        None
    ) {
        Ok(config) => config,
        Err(e) => {
            println!("{}", e.red());
            return;
        }
    };
    
    let persistent_path = config.persist_path.unwrap_or(DEFAULT_PERSISTENT_PATH.into());
    
    if (!Path::new(&persistent_path).exists()) {
        println!("{}", format!("Path does not exist: {}", &persistent_path).red());
        return;
    }

    if (!Path::new(format!("{}/{}", &persistent_path, SQLITE_FILENAME).as_str()).exists()) {
        println!("{}", format!("Not a Chroma path: {}", &persistent_path).red());
        return;
    }

    // let proceed = Confirm::new()
    //     .with_prompt("Are you sure you want to vacuum the database? This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?")
    //     .default(false)
    //     .interact()
    //     .unwrap_or_else(|e| {
    //         eprintln!("Failed to get confirmation: {}", e);
    //         false
    //     });
    // 
    // if (!proceed) {
    //     println!("{}", "Vacuum cancelled".red());
    //     return;
    // }

    // let initial_size = get_dir_size(Path::new(&persistent_path));
    
    let frontend_config = config.frontend.clone();
    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        // TODO: change this
        vacuum_chroma(frontend_config).await.expect("TODO: panic message");
    });
}