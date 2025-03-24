use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend::Frontend;
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
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::Row;
use std::error::Error;
use std::path::Path;
use std::str::FromStr;
use std::{fs, io};
use std::io::{Stdout, Write};
use thiserror::Error;
use crate::{cli_writeln, CliError, Handler};
use crate::utils::{CliInput, InputProvider, UtilsError};

#[derive(Parser, Debug)]
pub struct VacuumArgs {
    #[clap(long, help = "The path of your Chroma DB")]
    path: Option<String>,
    #[clap(long, default_value_t = false, help = "Skip vacuum confirmation")]
    force: bool,
}

#[derive(Debug, Error)]
pub enum VacuumError {
    #[error("Path {0} does not exist")]
    PathDoesNotExist(String),
    #[error("Failed to get size of of your Chroma directory")]
    DirSizeFailed,
    #[error("Not a Chroma path: {0}")]
    NotAChromaPath(String),
    #[error("Cannot find Sqlite config for Chroma")]
    SqliteConfigNotFound,
    #[error("Failed to vacuum Chroma")]
    VacuumFailed,
    #[error("Embeddings queue not configured for Chroma")]
    EmbeddingQueueNotConfigured,
}

fn start_message() -> String {
    format!("{}\n", "Chroma Vacuum".underline().bold())
}

fn vacuum_confirm_message() -> String {
    format!("{}\n{}", "Are you sure you want to vacuum the database?".bold().blue(), "This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue? (Y/n)")
}

fn vacuum_cancel_message() -> String {
    format!("{}", "\nVacuum cancelled\n".yellow())
}

fn log_purge_message() -> String {
    "Purging the log...\n".to_string()
}

fn vacuuming_message() -> String {
    "Vacuuming (this may take a while)...\n".to_string()
}

fn vacuum_complete_message(size_diff: u64, initial_size: u64) -> String {
    format!(
        "🧼 {}\n{} {} (⬇️{:.1}%)", 
        "Vacuum complete!".green().bold(), 
        "Database size reduced by".green(),
        sizeof_fmt(size_diff, None).to_string().green(),
        (((size_diff as f64) / (initial_size as f64)) * 100.0)
            .to_string()
            .green()
    )
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

fn get_dir_size(path: &Path) -> Result<u64, io::Error> {
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
            .get_hnsw_writer(&collection.collection, &collection.vector_segment, dim as usize)
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

pub struct VacuumCommandHandler<W: Write, I: InputProvider> {
    args: VacuumArgs,
    writer: W,
    input_provider: I,
    config: FrontendServerConfig
}

impl<W: Write, I: InputProvider> VacuumCommandHandler<W, I> {
    pub fn new(args: VacuumArgs, writer: W, input_provider: I) -> Self {
        VacuumCommandHandler { args, writer, input_provider, config: FrontendServerConfig::single_node_default() }
    }
    
    fn set_config_for_vacuum(&mut self) -> Result<(), VacuumError> {
        if let Some(persistent_path) = &self.args.path {
            self.config.persist_path = persistent_path.to_string();
        }
        
        let persistent_path = &self.config.persist_path.to_string();
        if !Path::new(persistent_path).exists() {
            return Err(VacuumError::PathDoesNotExist(persistent_path.to_string()));
        };

        let sqlite_url = format!("{}/{}", &persistent_path, &self.config.sqlite_filename);

        if !Path::new(sqlite_url.as_str()).exists() {
            return Err(VacuumError::NotAChromaPath(sqlite_url));
        }

        match self.config.frontend.sqlitedb.as_mut() {
            Some(sqlite_config) => {
                sqlite_config.url = Some(sqlite_url);
            }
            None => return Err(VacuumError::SqliteConfigNotFound),
        };
        
        Ok(())
    }
    
    fn confirm_vacuum(&mut self) -> Result<bool, UtilsError> {
        match self.args.force {
            true => Ok(true),
            false => {
                cli_writeln!(self.writer, "{}", vacuum_confirm_message())?;
                let confirm = &self.input_provider.get_user_input()?;
                Ok(confirm.to_string().to_lowercase().eq("y"))
            }
        }
    }
    
    async fn vacuum_chroma(&mut self) -> Result<(), Box<dyn Error>> {
        let system = System::new();
        let registry = Registry::new();
        let frontend_config = &self.config.frontend;
        let mut frontend = Frontend::try_from_config(&(frontend_config.clone(), system), &registry).await?;

        let sqlite = registry.get::<SqliteDb>()?;
        let segment_manager = registry.get::<LocalSegmentManager>()?;
        let mut sysdb = registry.get::<SysDb>()?;
        let mut log = registry.get::<Log>()?;

        cli_writeln!(self.writer, "{}", log_purge_message())?;

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
            return Err(VacuumError::SqliteConfigNotFound.into());
        }

        for collection in collections {
            let seq_ids = sqlx::query(r#"
                SELECT COALESCE(max_seq_id.seq_id, -1) AS seq_id
                FROM segments
                    LEFT JOIN max_seq_id ON segments.id = max_seq_id.segment_id
                WHERE segments.collection = ?
            "#,)
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

        cli_writeln!(self.writer, "{}", vacuuming_message())?;

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
}

impl VacuumCommandHandler<Stdout, CliInput> {
    pub fn default(args: VacuumArgs) -> Self {
        let stdout = io::stdout();
        let input_provider = CliInput::new();
        VacuumCommandHandler::new(args, stdout, input_provider)
    }
}

#[async_trait::async_trait]
impl<W: Write + Send, I: InputProvider + Send> Handler for VacuumCommandHandler<W, I> {
    async fn run(&mut self) -> Result<(), CliError> {
        // Vacuum the database. This may result in a small increase in performance.
        // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
        // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
        cli_writeln!(self.writer, "{}", start_message())?;
        
        self.set_config_for_vacuum()?;
        
        let proceed = self.confirm_vacuum()?;
        if !proceed {
            cli_writeln!(self.writer, "{}", vacuum_cancel_message())?;
            return Ok(());
        }
        
        let initial_size =
            get_dir_size(Path::new(&self.config.persist_path)).map_err(|_| VacuumError::DirSizeFailed)?;

        self.vacuum_chroma().await.map_err(|_| VacuumError::VacuumFailed)?;

        let post_vacuum_size =
            get_dir_size(Path::new(&self.config.persist_path)).map_err(|_| VacuumError::DirSizeFailed)?;

        println!("{} {}", initial_size, post_vacuum_size);
        let size_diff = initial_size - post_vacuum_size;
        cli_writeln!(self.writer, "{}", vacuum_complete_message(size_diff, initial_size))?;
        
        Ok(())
    }
}
