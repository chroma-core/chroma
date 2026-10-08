//! Explicit offline recovery for immutable HNSW construction settings.
#[cfg(test)]
mod tests;
use chroma_config::{registry::Registry, Configurable};
use chroma_segment::local_hnsw::{
    inspect_persisted_hnsw_index, inspect_persisted_hnsw_index_for_config_repair, HNSW_HEADER_FILE,
    HNSW_INDEX_FILES, METADATA_FILE,
};
use chroma_sqlite::{
    config::{MigrationHash, SqliteDBConfig},
    db::SqliteDb,
};
use chroma_types::{
    CollectionUuid, InternalCollectionConfiguration, Metadata, MetadataValue, Schema, Segment,
    SegmentScope, SegmentType, SegmentUuid,
};
use clap::Args;
use serde_json::Value;
use sqlx::{sqlite::SqliteConnectOptions, Connection, Row, SqliteConnection};
use std::{
    error::Error,
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Args, Debug)]
pub struct HnswConfigRepairArgs {
    /// Persistent directory of a stopped Chroma instance. Never modified.
    #[arg(long)]
    pub path: PathBuf,
    /// New persistent directory, outside the source directory. Must not exist.
    #[arg(long)]
    pub output: PathBuf,
    /// Collection whose invalid ef_construction values should be repaired.
    #[arg(long)]
    pub collection: CollectionUuid,
    /// Replacement for values outside 1..=4096; valid values are preserved.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=4096))]
    pub ef_construction: u32,
}

pub fn run(args: HnswConfigRepairArgs) -> ! {
    let result = tokio::runtime::Runtime::new()
        .map_err(|err| Box::new(err) as Box<dyn Error>)
        .and_then(|runtime| runtime.block_on(repair(&args)));
    match result {
        Ok(()) => {
            println!("Repaired store: {}. Original store preserved. Start Chroma with the repaired path.", args.output.display());
            std::process::exit(0);
        }
        Err(err) => {
            eprintln!("HNSW configuration repair failed: {err}");
            std::process::exit(1);
        }
    }
}

/// Copy a stopped store and repair out-of-range construction settings in that copy.
/// The destination is published only after validation succeeds. Logs, watermarks,
/// graph edges and embeddings are preserved; this does not rebuild the index.
pub async fn repair(args: &HnswConfigRepairArgs) -> Result<()> {
    if !(1..=4096).contains(&args.ef_construction) {
        return Err("ef_construction must be in 1..=4096".into());
    }
    let source = args.path.canonicalize()?;
    let output = std::path::absolute(&args.output)?;
    let parent = output
        .parent()
        .ok_or("output must have a parent directory")?
        .canonicalize()?;
    if parent.starts_with(&source) || output.symlink_metadata().is_ok() {
        return Err("output must not exist and must be outside the source directory".into());
    }
    if !source.join("chroma.sqlite3").is_file() {
        return Err("source has no chroma.sqlite3 database".into());
    }
    let staging = tempfile::tempdir_in(&parent)?;
    copy_directory(&source, staging.path())?;
    // Opening only the copy also recovers any SQLite journal left by a crash.
    let options = SqliteConnectOptions::new()
        .filename(staging.path().join("chroma.sqlite3"))
        .create_if_missing(false);
    let mut db = SqliteConnection::connect_with(&options).await?;
    // Use the store's existing hash algorithm when validating and applying
    // migrations. Only the staged copy is opened or migrated.
    let hash_length: Option<i64> =
        sqlx::query_scalar("SELECT length(hash) FROM migrations LIMIT 1")
            .fetch_optional(&mut db)
            .await?;
    let hash_type = match hash_length {
        Some(32) | None => MigrationHash::MD5,
        Some(64) => MigrationHash::SHA256,
        _ => return Err("unrecognized migration hash in source database".into()),
    };
    db.close().await?;
    let migrated = SqliteDb::try_from_config(
        &SqliteDBConfig {
            url: Some(
                staging
                    .path()
                    .join("chroma.sqlite3")
                    .to_str()
                    .ok_or("database path is not UTF-8")?
                    .to_owned(),
            ),
            hash_type,
            ..Default::default()
        },
        &Registry::new(),
    )
    .await?;
    migrated.close().await;
    let mut db = SqliteConnection::connect_with(&options).await?;
    let result = repair_copy(&mut db, staging.path(), args).await;
    db.close().await?;
    result?;
    if output.symlink_metadata().is_ok() {
        return Err("output appeared during repair; refusing to overwrite it".into());
    }
    fs::rename(staging.path(), &output)?;
    Ok(())
}

fn copy_directory(source: &Path, output: &Path) -> Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let destination = output.join(entry.file_name());
        if kind.is_dir() {
            fs::create_dir(&destination)?;
            copy_directory(&entry.path(), &destination)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), destination)?;
        } else {
            return Err(format!(
                "refusing symlink or special file: {}",
                entry.path().display()
            )
            .into());
        }
    }
    Ok(())
}

fn replace_invalid(value: &mut Value, replacement: u32) -> bool {
    if value.as_u64().is_some_and(|n| (1..=4096).contains(&n)) {
        false
    } else {
        *value = replacement.into();
        true
    }
}

async fn repair_copy(
    db: &mut SqliteConnection,
    root: &Path,
    args: &HnswConfigRepairArgs,
) -> Result<()> {
    let mut tx = db.begin().await?;
    let id = args.collection.to_string();
    let row =
        sqlx::query("SELECT config_json_str, schema_str, dimension FROM collections WHERE id = ?")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await?;
    let mut config: Value = serde_json::from_str(
        row.try_get::<Option<&str>, _>("config_json_str")?
            .unwrap_or("{}"),
    )?;
    let mut changed = false;
    if let Some(value) = config.pointer_mut("/vector_index/hnsw/ef_construction") {
        changed |= replace_invalid(value, args.ef_construction);
    }
    let internal: InternalCollectionConfiguration = serde_json::from_value(config.clone())?;
    let stored_schema = row.try_get::<Option<&str>, _>("schema_str")?;
    let mut schema: Schema = match stored_schema {
        Some(value) => serde_json::from_str(value)?,
        None => Schema::try_from(&internal)?,
    };
    for types in std::iter::once(&mut schema.defaults).chain(schema.keys.values_mut()) {
        if let Some(hnsw) = types
            .float_list
            .as_mut()
            .and_then(|value| value.vector_index.as_mut())
            .and_then(|value| value.config.hnsw.as_mut())
        {
            if hnsw
                .ef_construction
                .is_some_and(|n| !(1..=4096).contains(&n))
            {
                hnsw.ef_construction = Some(args.ef_construction as usize);
                changed = true;
            }
        }
    }
    let rows =
        sqlx::query("SELECT id, type FROM segments WHERE collection = ? AND scope = 'VECTOR'")
            .bind(&id)
            .fetch_all(&mut *tx)
            .await?;
    if rows.len() != 1
        || rows[0].try_get::<&str, _>("type")? != "urn:chroma:segment/vector/hnsw-local-persisted"
    {
        return Err("repair requires exactly one local persisted HNSW segment".into());
    }
    let segment_id: SegmentUuid = rows[0].try_get::<&str, _>("id")?.parse()?;
    for (table, column, owner) in [
        ("segment_metadata", "segment_id", segment_id.to_string()),
        ("collection_metadata", "collection_id", id.clone()),
    ] {
        changed |= sqlx::query(&format!("UPDATE {table} SET int_value = ?, str_value = NULL, float_value = NULL, bool_value = NULL WHERE {column} = ? AND key = 'hnsw:construction_ef' AND (int_value IS NULL OR int_value < 1 OR int_value > 4096)"))
            .bind(i64::from(args.ef_construction)).bind(owner).execute(&mut *tx).await?.rows_affected() > 0;
    }
    let mut metadata = Metadata::new();
    for row in sqlx::query("SELECT key, str_value, int_value, float_value FROM segment_metadata WHERE segment_id = ? AND key LIKE 'hnsw:%'")
        .bind(segment_id.to_string()).fetch_all(&mut *tx).await? {
        let value = if let Some(value) = row.try_get::<Option<i64>, _>("int_value")? {
            MetadataValue::Int(value)
        } else if let Some(value) = row.try_get::<Option<f64>, _>("float_value")? {
            MetadataValue::Float(value)
        } else {
            MetadataValue::Str(row.try_get::<String, _>("str_value")?)
        };
        metadata.insert(row.try_get("key")?, value);
    }
    let segment = Segment {
        id: segment_id,
        r#type: SegmentType::HnswLocalPersisted,
        scope: SegmentScope::VECTOR,
        collection: args.collection,
        metadata: Some(metadata),
        file_path: Default::default(),
    };
    schema
        .get_internal_hnsw_config_with_legacy_fallback(&segment)?
        .ok_or("collection has no HNSW configuration")?;
    let index = root.join(segment_id.to_string());
    if index.join(HNSW_HEADER_FILE).exists() {
        // Only construction settings may be invalid before this repair.
        let inspection = inspect_persisted_hnsw_index_for_config_repair(&index)?;
        if !index.join(METADATA_FILE).is_file()
            || row.try_get::<Option<i64>, _>("dimension")? != Some(inspection.dimensionality as i64)
        {
            return Err(
                "configuration repair requires an intact ID map and matching index dimension"
                    .into(),
            );
        }
        let header = fs::read(index.join(HNSW_HEADER_FILE))?;
        if u32::from_ne_bytes(header[..4].try_into()?) != 1 {
            return Err("construction repair only supports HNSW persistence version 1".into());
        }
        // Version (u32), six size_t values, level + entrypoint (u32), three
        // size_t values and mult (f64) precede ef_construction in version 1.
        let offset = 20 + 9 * std::mem::size_of::<usize>();
        let old =
            usize::from_ne_bytes(header[offset..offset + std::mem::size_of::<usize>()].try_into()?);
        if !(1..=4096).contains(&old) {
            // Match native index creation, which floors construction effort at M.
            let m_offset = offset - 8 - std::mem::size_of::<usize>();
            let m = usize::from_ne_bytes(header[m_offset..offset - 8].try_into()?);
            let replacement = (args.ef_construction as usize).max(m);
            if replacement > 4096 {
                return Err(
                    "persisted M exceeds the construction limit; this index needs rebuilding"
                        .into(),
                );
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(index.join(HNSW_HEADER_FILE))?;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(&replacement.to_ne_bytes())?;
            file.sync_all()?;
            changed = true;
        }
        // Require normal validation before publishing the repaired copy.
        inspect_persisted_hnsw_index(&index)?;
    } else {
        let watermark: Option<i64> =
            sqlx::query_scalar("SELECT seq_id FROM max_seq_id WHERE segment_id = ?")
                .bind(segment_id.to_string())
                .fetch_optional(&mut *tx)
                .await?;
        if watermark.is_some_and(|offset| offset > 0)
            || HNSW_INDEX_FILES
                .iter()
                .chain(std::iter::once(&METADATA_FILE))
                .any(|file| index.join(file).exists())
        {
            return Err("persisted HNSW header is missing; configuration repair cannot recover lost index data".into());
        }
    }
    if !changed {
        return Err("collection has no invalid ef_construction setting to repair".into());
    }
    sqlx::query("UPDATE collections SET config_json_str = ?, schema_str = ? WHERE id = ?")
        .bind(serde_json::to_string(&config)?)
        // Keep legacy fallback independent of this machine's CPU-dependent
        // defaults. A synthesized schema is only needed for validation.
        .bind(
            stored_schema
                .map(|_| serde_json::to_string(&schema))
                .transpose()?,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
