use crate::utils::{
    get_frontend_config, LocalFrontendCommandArgs, DEFAULT_PERSISTENT_PATH, SQLITE_FILENAME,
};
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_frontend::frontend::Frontend;
use chroma_frontend::FrontendConfig;
use chroma_log::sqlite_log::{
    legacy_embeddings_queue_config_default_kind, LegacyEmbeddingsQueueConfig, SqliteLog,
};
use chroma_log::Log;
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_types::{CollectionUuid, ListCollectionsRequest};
use clap::Parser;
use colored::Colorize;
use dialoguer::Confirm;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::Row;
use std::error::Error;
use std::path::Path;
use std::str::FromStr;
use std::{fs, io};

#[derive(Parser, Debug)]
pub struct VacuumArgs {
    #[clap(flatten)]
    frontend_args: LocalFrontendCommandArgs,
    #[arg(long)]
    force: bool,
}

fn sizeof_fmt(num: u64, suffix: Option<&str>) -> String {
    let suffix = suffix.unwrap_or("B");
    let mut n = num as f64;
    let units = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"];
    for unit in &units {
        if n.abs() < 1024.0 {
            return format!("{:3.1}{}{}", n, unit, suffix);
        }
        n /= 1024.0;
    }
    format!("{:.1}Yi{}", n, suffix)
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

async fn get_collection_ids_to_migrate(
    sqlite: &SqliteDb,
) -> Result<Vec<CollectionUuid>, Box<dyn Error>> {
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

    Ok(collection_ids)
}

async fn trigger_vector_segments_max_seq_id_migration(
    sqlite: &SqliteDb,
    sysdb: &mut SysDb,
    segment_manager: &LocalSegmentManager,
) -> Result<(), Box<dyn Error>> {
    let collection_ids = get_collection_ids_to_migrate(sqlite).await?;

    for collection_id in collection_ids {
        let collection = sysdb.get_collection_with_segments(collection_id).await?;

        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection.collection.dimension {
            Some(dim) => dim,
            None => continue,
        };

        segment_manager
            .get_hnsw_writer(&collection.vector_segment, dim as usize)
            .await?;
    }

    Ok(())
}

async fn configure_sql_embedding_queue(log: &SqliteLog) -> Result<(), Box<dyn Error>> {
    let config = LegacyEmbeddingsQueueConfig {
        automatically_purge: true,
        kind: legacy_embeddings_queue_config_default_kind(),
    };

    log.update_legacy_embeddings_queue_config(config).await?;
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

    println!("Purging the log...\n");

    trigger_vector_segments_max_seq_id_migration(&sqlite, &mut sysdb, &segment_manager).await?;

    let tenant = String::from("default_tenant");
    let database = String::from("default_database");

    let list_collections_request = ListCollectionsRequest::try_new(tenant, database, None, 0)?;
    let collections = frontend.list_collections(list_collections_request).await?;

    let pb = ProgressBar::new(collections.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{bar:40.cyan/blue} {pos}/{len} ({percent}%)")?
            .progress_chars("=>-"),
    );

    if let Log::Sqlite(ref log) = log {
        configure_sql_embedding_queue(log).await?;
    } else {
        return Err("Expected a Sqlite log for vacuum".into());
    }

    for collection in collections {
        let seq_ids = sqlx::query(
            r#"
                SELECT COALESCE(max_seq_id.seq_id, -1) AS seq_id
                FROM segments
                    LEFT JOIN max_seq_id ON segments.id = max_seq_id.segment_id
                WHERE segments.collection = ?
            "#,
        )
        .bind(collection.collection_id.to_string())
        .fetch_all(sqlite.get_conn())
        .await?;

        let min_seq_id: Option<i64> = seq_ids.iter().map(|row| row.get(0)).min().unwrap_or(None);

        if min_seq_id.is_none() {
            continue;
        }

        if min_seq_id.is_some() && min_seq_id.unwrap() < 0 {
            continue;
        }

        log.purge_logs(collection.collection_id, min_seq_id.unwrap() as u64)
            .await?;

        pb.inc(1);
    }

    println!("Vacuuming (this may take a while)...\n");

    sqlx::query(&format!("PRAGMA busy_timeout = {}", 5000))
        .execute(sqlite.get_conn())
        .await?;

    sqlx::query("VACUUM").execute(sqlite.get_conn()).await?;

    sqlx::query(
        "INSERT INTO maintenance_log (operation, timestamp)
         VALUES ('vacuum', CURRENT_TIMESTAMP)",
    )
    .execute(sqlite.get_conn())
    .await?;

    Ok(())
}

pub fn vacuum(args: VacuumArgs) {
    // Vacuum the database. This may result in a small increase in performance.
    // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
    // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    println!("{}", "\nChroma Vacuum\n".underline().bold());

    let config = match get_frontend_config(
        args.frontend_args.config_path,
        args.frontend_args.persistent_path,
        None,
    ) {
        Ok(config) => config,
        Err(e) => {
            println!("{}", e.red());
            return;
        }
    };

    let persistent_path = config
        .persist_path
        .unwrap_or(DEFAULT_PERSISTENT_PATH.into());

    if !Path::new(&persistent_path).exists() {
        println!(
            "{}",
            format!("Path does not exist: {}", &persistent_path).red()
        );
        return;
    }

    if !Path::new(format!("{}/{}", &persistent_path, SQLITE_FILENAME).as_str()).exists() {
        println!(
            "{}",
            format!("Not a Chroma path: {}", &persistent_path).red()
        );
        return;
    }

    let proceed = match args.force {
        true => true,
        false => {
            println!(
                "{}",
                "Are you sure you want to vacuum the database?"
                    .bold()
                    .blue()
            );
            Confirm::new()
                .with_prompt("This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?")
                .interact()
                .unwrap_or(false)
        }
    };

    println!();

    if !proceed {
        println!("{}", "Vacuum cancelled\n".red());
        return;
    }

    let initial_size = match get_dir_size(Path::new(&persistent_path)) {
        Ok(size) => size,
        Err(_e) => {
            println!("{}", "Failed to get Chroma directory size\n".red());
            return;
        }
    };

    let frontend_config = config.frontend.clone();
    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    let vacuum_result = runtime.block_on(vacuum_chroma(frontend_config));

    if let Err(_e) = vacuum_result {
        eprintln!("Failed to vacuum Chroma");
        return;
    }

    let post_vacuum_size = match get_dir_size(Path::new(&persistent_path)) {
        Ok(size) => size,
        Err(_e) => {
            println!("Failed to get Chroma directory size after vacuum");
            return;
        }
    };

    let size_diff = initial_size - post_vacuum_size;

    println!("🧼 {}", "Vacuum complete!".green().bold());
    println!(
        "Database size reduced by {} (⬇️{:.1}%)",
        sizeof_fmt(size_diff, None).to_string().green(),
        (((size_diff as f64) / (initial_size as f64)) * 100.0)
            .to_string()
            .green()
    );
    println!();
}
