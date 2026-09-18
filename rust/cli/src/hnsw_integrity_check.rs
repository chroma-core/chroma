use chroma_frontend::config::FrontendServerConfig;
use chroma_segment::local_hnsw::{
    inspect_persisted_hnsw_metadata, parse_persisted_hnsw_dim, PersistedHnswLabelMismatch,
    PersistedHnswMetadata, HNSW_HEADER_FILE, HNSW_INDEX_FILES, METADATA_FILE,
};
use chroma_sqlite::helpers::get_embeddings_queue_topic_name;
use chroma_types::{CollectionUuid, SegmentType};
use clap::Parser;
use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use thiserror::Error;

#[derive(Parser, Debug)]
#[command(
    name = "chroma-hnsw-integrity-check",
    about = "Detect local Chroma HNSW startup fast-forward and integrity hazards",
    after_help = "Run against a stopped Chroma instance for a consistent view of SQLite and index files. Exit codes: 0 = no corruption or pending migration; 1 = findings; 2 = check failed."
)]
pub struct HnswIntegrityCheckArgs {
    #[arg(
        long,
        value_name = "DIR",
        default_value = "./chroma",
        help = "Chroma persistent directory"
    )]
    path: PathBuf,
    #[arg(
        long,
        value_name = "FILE",
        help = "Sqlite database path; defaults to <path>/chroma.sqlite3"
    )]
    sqlite: Option<PathBuf>,
    #[arg(long, value_name = "UUID", help = "Limit the check to one collection")]
    collection: Option<CollectionUuid>,
    #[arg(
        long,
        default_value = "default",
        help = "Configured SQLite log tenant ID"
    )]
    log_tenant: String,
    #[arg(
        long,
        default_value = "default",
        help = "Configured SQLite log topic namespace"
    )]
    log_topic_namespace: String,
    #[arg(long, help = "Emit machine-readable JSON")]
    json: bool,
}

#[derive(Debug, Error)]
pub enum HnswIntegrityCheckError {
    #[error("persistent directory does not exist: {0}")]
    MissingPersistDirectory(String),
    #[error("sqlite database does not exist: {0}")]
    MissingSqliteDatabase(String),
    #[error("failed to open sqlite database read-only: {0}")]
    SqliteOpen(#[source] sqlx::Error),
    #[error("failed to query sqlite database: {0}")]
    SqliteQuery(#[source] sqlx::Error),
    #[error("invalid sequence watermark in sqlite: {0}")]
    InvalidSequenceId(String),
    #[error("invalid collection id in sqlite: {0}")]
    InvalidCollectionId(String),
    #[error("failed to serialize JSON report: {0}")]
    Json(#[source] serde_json::Error),
}

#[derive(Debug, Serialize)]
struct Report {
    persist_path: String,
    sqlite_path: String,
    checked_segments: usize,
    pending_fast_forwards: usize,
    corruptions: usize,
    warnings: usize,
    issues: Vec<Issue>,
}

#[derive(Debug, Serialize)]
struct Issue {
    severity: Severity,
    kind: &'static str,
    collection_id: String,
    collection_name: String,
    vector_segment_id: String,
    collection_dimension: Option<i64>,
    vector_max_seq_id: Option<i64>,
    metadata_max_seq_id: Option<i64>,
    purge_watermark: i64,
    detail: String,
    log_state: LogState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Severity {
    FastForward,
    Corrupt,
    Warning,
}

#[derive(Clone, Debug, Serialize)]
struct LogState {
    topic: String,
    row_count: i64,
    min_seq_id: Option<i64>,
    max_seq_id: Option<i64>,
    rows_at_or_below_vector_watermark: i64,
    rows_below_purge_watermark: i64,
}

#[derive(Debug)]
struct SegmentRow {
    collection_id: CollectionUuid,
    collection_name: String,
    collection_dimension: Option<i64>,
    vector_segment_id: String,
    vector_max_seq_id: Option<i64>,
    metadata_max_seq_id: Option<i64>,
}

pub fn hnsw_integrity_check(args: HnswIntegrityCheckArgs) -> ! {
    let code = match run_blocking(args) {
        Ok(outcome) => {
            if outcome.has_findings() {
                1
            } else {
                0
            }
        }
        Err(err) => {
            eprintln!("{err}");
            2
        }
    };
    std::process::exit(code);
}

pub fn hnsw_integrity_check_exit_code(args: HnswIntegrityCheckArgs) -> ExitCode {
    match run_blocking(args) {
        Ok(outcome) => outcome.exit_code(),
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(2)
        }
    }
}

fn run_blocking(args: HnswIntegrityCheckArgs) -> Result<CheckOutcome, HnswIntegrityCheckError> {
    let json = args.json;
    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    let report = runtime.block_on(run(args))?;
    if json {
        print_json_report(&report)?;
    } else {
        print_human_report(&report);
    }
    Ok(CheckOutcome { report })
}

struct CheckOutcome {
    report: Report,
}

impl CheckOutcome {
    fn has_findings(&self) -> bool {
        self.report.corruptions > 0 || self.report.pending_fast_forwards > 0
    }

    fn exit_code(&self) -> ExitCode {
        if self.has_findings() {
            ExitCode::from(1)
        } else {
            ExitCode::SUCCESS
        }
    }
}

async fn run(args: HnswIntegrityCheckArgs) -> Result<Report, HnswIntegrityCheckError> {
    if !args.path.is_dir() {
        return Err(HnswIntegrityCheckError::MissingPersistDirectory(
            args.path.display().to_string(),
        ));
    }
    let sqlite_path = args.sqlite.clone().unwrap_or_else(|| {
        args.path
            .join(FrontendServerConfig::single_node_default().sqlite_filename)
    });
    if !sqlite_path.is_file() {
        return Err(HnswIntegrityCheckError::MissingSqliteDatabase(
            sqlite_path.display().to_string(),
        ));
    }

    let pool = open_read_only_sqlite(&sqlite_path).await?;
    let segments = load_hnsw_segments(
        &pool,
        args.collection.as_ref().map(ToString::to_string).as_deref(),
    )
    .await?;
    let mut issues = Vec::new();

    for segment in &segments {
        let log_state =
            load_log_state(&pool, segment, &args.log_tenant, &args.log_topic_namespace).await?;
        inspect_segment(&args.path, segment, log_state, &mut issues);
    }

    let corruptions = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Corrupt)
        .count();
    let pending_fast_forwards = issues
        .iter()
        .filter(|issue| issue.severity == Severity::FastForward)
        .count();
    let warnings = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Warning)
        .count();

    Ok(Report {
        persist_path: args.path.display().to_string(),
        sqlite_path: sqlite_path.display().to_string(),
        checked_segments: segments.len(),
        pending_fast_forwards,
        corruptions,
        warnings,
        issues,
    })
}

async fn open_read_only_sqlite(sqlite_path: &Path) -> Result<SqlitePool, HnswIntegrityCheckError> {
    let options = SqliteConnectOptions::new()
        .filename(sqlite_path)
        .read_only(true)
        .create_if_missing(false);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(HnswIntegrityCheckError::SqliteOpen)
}

async fn load_hnsw_segments(
    pool: &SqlitePool,
    collection_id: Option<&str>,
) -> Result<Vec<SegmentRow>, HnswIntegrityCheckError> {
    let mut query = String::from(
        r#"
        SELECT
            collections.id AS collection_id,
            collections.name AS collection_name,
            collections.dimension AS collection_dimension,
            vector_segments.id AS vector_segment_id,
            vector_max.seq_id AS vector_max_seq_id,
            metadata_max.seq_id AS metadata_max_seq_id
        FROM collections
        INNER JOIN databases
            ON databases.id = collections.database_id
        INNER JOIN segments AS vector_segments
            ON vector_segments.collection = collections.id
           AND vector_segments.type = ?
        LEFT JOIN max_seq_id AS vector_max
            ON vector_max.segment_id = vector_segments.id
        LEFT JOIN segments AS metadata_segments
            ON metadata_segments.collection = collections.id
           AND metadata_segments.type = ?
        LEFT JOIN max_seq_id AS metadata_max
            ON metadata_max.segment_id = metadata_segments.id
        "#,
    );
    if collection_id.is_some() {
        query.push_str(" WHERE collections.id = ?");
    }
    query.push_str(" ORDER BY collections.id");

    let vector_segment_type: String = SegmentType::HnswLocalPersisted.into();
    let metadata_segment_type: String = SegmentType::Sqlite.into();
    let mut sql = sqlx::query(&query)
        .bind(vector_segment_type)
        .bind(metadata_segment_type);
    if let Some(collection_id) = collection_id {
        sql = sql.bind(collection_id);
    }

    let rows = sql
        .fetch_all(pool)
        .await
        .map_err(HnswIntegrityCheckError::SqliteQuery)?;
    rows.into_iter()
        .map(|row| {
            let collection_id_str: String = row.get("collection_id");
            let collection_id = CollectionUuid::from_str(&collection_id_str).map_err(|_| {
                HnswIntegrityCheckError::InvalidCollectionId(collection_id_str.clone())
            })?;
            Ok(SegmentRow {
                collection_id,
                collection_name: row.get("collection_name"),
                collection_dimension: row.get("collection_dimension"),
                vector_segment_id: row.get("vector_segment_id"),
                vector_max_seq_id: read_seq_id(&row, "vector_max_seq_id")?,
                metadata_max_seq_id: read_seq_id(&row, "metadata_max_seq_id")?,
            })
        })
        .collect()
}

// Older SQLite stores use an eight-byte, big-endian watermark. A SQL CAST
// silently turns those blobs into zero and can hide missing-index corruption.
fn read_seq_id(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<i64>, HnswIntegrityCheckError> {
    if let Ok(value) = row.try_get::<Option<i64>, _>(column) {
        if value.is_some_and(|value| value < 0) {
            return Err(HnswIntegrityCheckError::InvalidSequenceId(
                column.to_string(),
            ));
        }
        return Ok(value);
    }
    let bytes: Vec<u8> = row
        .try_get(column)
        .map_err(HnswIntegrityCheckError::SqliteQuery)?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| HnswIntegrityCheckError::InvalidSequenceId(column.to_string()))?;
    let value = i64::try_from(u64::from_be_bytes(bytes))
        .map_err(|_| HnswIntegrityCheckError::InvalidSequenceId(column.to_string()))?;
    Ok(Some(value))
}

async fn load_log_state(
    pool: &SqlitePool,
    segment: &SegmentRow,
    log_tenant: &str,
    log_topic_namespace: &str,
) -> Result<LogState, HnswIntegrityCheckError> {
    let vector_watermark = segment.vector_max_seq_id.unwrap_or_default();
    let purge_watermark = purge_watermark(segment);
    let topic =
        get_embeddings_queue_topic_name(log_tenant, log_topic_namespace, segment.collection_id);
    let row = sqlx::query(
        r#"
        SELECT
            MIN(seq_id) AS min_seq_id,
            MAX(seq_id) AS max_seq_id,
            COUNT(*) AS row_count,
            COALESCE(SUM(CASE WHEN seq_id <= ? THEN 1 ELSE 0 END), 0)
                AS rows_at_or_below_vector_watermark,
            COALESCE(SUM(CASE WHEN seq_id < ? THEN 1 ELSE 0 END), 0)
                AS rows_below_purge_watermark
        FROM embeddings_queue
        WHERE topic = ?
        "#,
    )
    .bind(vector_watermark)
    .bind(purge_watermark)
    .bind(&topic)
    .fetch_one(pool)
    .await
    .map_err(HnswIntegrityCheckError::SqliteQuery)?;

    Ok(LogState {
        topic,
        row_count: row.get("row_count"),
        min_seq_id: row.get("min_seq_id"),
        max_seq_id: row.get("max_seq_id"),
        rows_at_or_below_vector_watermark: row.get("rows_at_or_below_vector_watermark"),
        rows_below_purge_watermark: row.get("rows_below_purge_watermark"),
    })
}

fn inspect_segment(
    persist_path: &Path,
    segment: &SegmentRow,
    log_state: LogState,
    issues: &mut Vec<Issue>,
) {
    let vector_watermark = segment.vector_max_seq_id.unwrap_or_default();
    let has_durable_watermark = vector_watermark > 0;
    let has_sqlite_vector_watermark = segment.vector_max_seq_id.is_some();
    let index_dir = persist_path.join(&segment.vector_segment_id);
    let has_any_hnsw_file = HNSW_INDEX_FILES
        .iter()
        .copied()
        .chain(std::iter::once(METADATA_FILE))
        .any(|filename| index_dir.join(filename).is_file());

    if segment.collection_dimension.is_none() {
        if has_durable_watermark {
            push_issue(
                issues,
                Severity::Corrupt,
                "missing_collection_dimension",
                segment,
                log_state,
                "sqlite has a vector max_seq_id for this collection, but the collection has no dimension".to_string(),
            );
        }
        return;
    }

    if !index_dir.is_dir() {
        if has_durable_watermark {
            push_issue(
                issues,
                Severity::Corrupt,
                "missing_index_directory",
                segment,
                log_state,
                format!(
                    "sqlite vector max_seq_id is {}, but HNSW directory is missing: {}",
                    vector_watermark,
                    index_dir.display()
                ),
            );
        }
        return;
    }

    if !has_durable_watermark && !has_any_hnsw_file {
        return;
    }

    let expected_dim = segment.collection_dimension.unwrap() as usize;
    let severity = if has_durable_watermark {
        Severity::Corrupt
    } else {
        Severity::Warning
    };

    for filename in HNSW_INDEX_FILES
        .iter()
        .copied()
        .chain(std::iter::once(METADATA_FILE))
    {
        let file_path = index_dir.join(filename);
        if !file_path.is_file() {
            push_issue(
                issues,
                severity,
                "missing_hnsw_file",
                segment,
                log_state.clone(),
                format!("HNSW file is missing: {}", file_path.display()),
            );
        }
    }

    let header_path = index_dir.join(HNSW_HEADER_FILE);
    if header_path.is_file() {
        match parse_hnsw_dim_from_path(&header_path) {
            Ok(actual_dim) if actual_dim != expected_dim => push_issue(
                issues,
                severity,
                "hnsw_header_dimension_mismatch",
                segment,
                log_state.clone(),
                format!(
                    "HNSW header dimensionality {} does not match collection dimensionality {}",
                    actual_dim, expected_dim
                ),
            ),
            Ok(_) => {}
            Err(err) => push_issue(
                issues,
                severity,
                "invalid_hnsw_header",
                segment,
                log_state.clone(),
                format!("failed to parse {}: {err}", header_path.display()),
            ),
        }
    }

    let metadata_path = index_dir.join(METADATA_FILE);
    if metadata_path.is_file() {
        match inspect_persisted_hnsw_metadata(&metadata_path) {
            Ok(metadata) => inspect_hnsw_metadata(
                segment,
                log_state,
                issues,
                severity,
                expected_dim,
                has_sqlite_vector_watermark,
                metadata,
            ),
            Err(err) => push_issue(
                issues,
                severity,
                "invalid_hnsw_metadata",
                segment,
                log_state,
                format!("failed to read {}: {err}", metadata_path.display()),
            ),
        }
    }
}

fn inspect_hnsw_metadata(
    segment: &SegmentRow,
    log_state: LogState,
    issues: &mut Vec<Issue>,
    severity: Severity,
    expected_dim: usize,
    has_sqlite_vector_watermark: bool,
    metadata: PersistedHnswMetadata,
) {
    if !has_sqlite_vector_watermark {
        if let Some(legacy_max_seq_id) = metadata.legacy_max_seq_id {
            if legacy_max_seq_id > 0 && metadata.id_to_label_count > 0 {
                push_issue(
                    issues,
                    Severity::FastForward,
                    "pending_startup_fast_forward",
                    segment,
                    log_state.clone(),
                    format!(
                        "sqlite has no vector max_seq_id row, but HNSW metadata has legacy max_seq_id {legacy_max_seq_id}; opening this segment will migrate that value into sqlite"
                    ),
                );
            }
        }
    }

    if let Some(actual_dim) = metadata.dimensionality {
        if actual_dim != expected_dim {
            push_issue(
                issues,
                severity,
                "hnsw_metadata_dimension_mismatch",
                segment,
                log_state.clone(),
                format!(
                    "HNSW metadata dimensionality {} does not match collection dimensionality {}",
                    actual_dim, expected_dim
                ),
            );
        }
    }

    if metadata.id_to_label_count != metadata.label_to_id_count {
        push_issue(
            issues,
            severity,
            "hnsw_metadata_label_count_mismatch",
            segment,
            log_state.clone(),
            format!(
                "HNSW metadata has {} id_to_label entries but {} label_to_id entries",
                metadata.id_to_label_count, metadata.label_to_id_count
            ),
        );
    }

    if metadata.id_to_label_count > metadata.total_elements_added as usize {
        push_issue(
            issues,
            severity,
            "hnsw_metadata_total_elements_mismatch",
            segment,
            log_state.clone(),
            format!(
                "HNSW metadata maps {} ids but total_elements_added is {}",
                metadata.id_to_label_count, metadata.total_elements_added
            ),
        );
    }

    if let Some(label_mismatch) = metadata.first_label_mismatch {
        match label_mismatch {
            PersistedHnswLabelMismatch::LabelMapsToDifferentId {
                id,
                label,
                reverse_id,
            } => push_issue(
                issues,
                severity,
                "hnsw_metadata_label_mismatch",
                segment,
                log_state,
                format!(
                    "HNSW metadata maps id {id:?} to label {label}, but label maps back to {reverse_id:?}"
                ),
            ),
            PersistedHnswLabelMismatch::MissingReverseLabel { id, label } => push_issue(
                issues,
                severity,
                "hnsw_metadata_missing_reverse_label",
                segment,
                log_state,
                format!(
                    "HNSW metadata maps id {id:?} to label {label}, but the reverse label is missing"
                ),
            ),
        }
    }
}

fn push_issue(
    issues: &mut Vec<Issue>,
    severity: Severity,
    kind: &'static str,
    segment: &SegmentRow,
    log_state: LogState,
    detail: String,
) {
    issues.push(Issue {
        severity,
        kind,
        collection_id: segment.collection_id.to_string(),
        collection_name: segment.collection_name.clone(),
        vector_segment_id: segment.vector_segment_id.clone(),
        collection_dimension: segment.collection_dimension,
        vector_max_seq_id: segment.vector_max_seq_id,
        metadata_max_seq_id: segment.metadata_max_seq_id,
        purge_watermark: purge_watermark(segment),
        detail,
        log_state,
    });
}

fn purge_watermark(segment: &SegmentRow) -> i64 {
    segment
        .metadata_max_seq_id
        .unwrap_or_default()
        .min(segment.vector_max_seq_id.unwrap_or_default())
}

fn parse_hnsw_dim_from_path(path: &Path) -> Result<usize, String> {
    let mut file = std::fs::File::open(path).map_err(|err| err.to_string())?;
    let mut header = [0; std::mem::size_of::<i32>() + 6 * std::mem::size_of::<usize>()];
    file.read_exact(&mut header)
        .map_err(|err| err.to_string())?;
    parse_persisted_hnsw_dim(&header).ok_or_else(|| "invalid persisted HNSW header".to_string())
}

fn print_json_report(report: &Report) -> Result<(), HnswIntegrityCheckError> {
    println!(
        "{}",
        serde_json::to_string_pretty(report).map_err(HnswIntegrityCheckError::Json)?
    );
    Ok(())
}

fn print_human_report(report: &Report) {
    if report.issues.is_empty() {
        println!(
            "OK: checked {} persisted HNSW vector segment(s); no pending startup fast-forward or corruption indicators found.",
            report.checked_segments
        );
        return;
    }

    println!(
        "Checked {} persisted HNSW vector segment(s): {} pending startup fast-forward(s), {} corruption(s), {} warning(s).",
        report.checked_segments, report.pending_fast_forwards, report.corruptions, report.warnings
    );
    println!("sqlite: {}", report.sqlite_path);
    println!("persist: {}", report.persist_path);
    println!();

    for issue in &report.issues {
        println!(
            "[{:?}] {} collection={} name={:?} vector_segment={}",
            issue.severity,
            issue.kind,
            issue.collection_id,
            issue.collection_name,
            issue.vector_segment_id
        );
        println!("  {}", issue.detail);
        println!(
            "  watermarks: metadata={:?} vector={:?} purge={}",
            issue.metadata_max_seq_id, issue.vector_max_seq_id, issue.purge_watermark
        );
        println!(
            "  logs: rows={} range={:?}..{:?} rows<=vector_watermark={} rows<purge_watermark={}",
            issue.log_state.row_count,
            issue.log_state.min_seq_id,
            issue.log_state.max_seq_id,
            issue.log_state.rows_at_or_below_vector_watermark,
            issue.log_state.rows_below_purge_watermark
        );
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checker_reads_configured_log_topic_without_writing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("chroma.sqlite3");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE databases (id TEXT, name TEXT, tenant_id TEXT);
             CREATE TABLE collections (id TEXT, name TEXT, dimension INTEGER, database_id TEXT);
             CREATE TABLE segments (id TEXT, type TEXT, collection TEXT);
             CREATE TABLE max_seq_id (segment_id TEXT, seq_id INTEGER);
             CREATE TABLE embeddings_queue (seq_id INTEGER, topic TEXT);
             INSERT INTO databases VALUES ('db', 'user_database', 'user_tenant');",
        )
        .execute(&pool)
        .await
        .unwrap();
        let collection_id = CollectionUuid::new();
        sqlx::query("INSERT INTO collections VALUES (?, 'test', 3, 'db')")
            .bind(collection_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        let vector = uuid::Uuid::new_v4().to_string();
        for (id, kind, watermark) in [
            (vector.as_str(), SegmentType::HnswLocalPersisted, 5_i64),
            ("metadata", SegmentType::Sqlite, 10_i64),
        ] {
            let kind: String = kind.into();
            sqlx::query("INSERT INTO segments VALUES (?, ?, ?)")
                .bind(id)
                .bind(kind)
                .bind(collection_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id VALUES (?, ?)")
                .bind(id)
                .bind(watermark)
                .execute(&pool)
                .await
                .unwrap();
        }
        let topic = get_embeddings_queue_topic_name("log_tenant", "log_namespace", collection_id);
        sqlx::query("INSERT INTO embeddings_queue VALUES (4, ?), (6, ?)")
            .bind(&topic)
            .bind(&topic)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let before = std::fs::read(&path).unwrap();
        let args = HnswIntegrityCheckArgs::parse_from([
            "check",
            "--path",
            root.path().to_str().unwrap(),
            "--log-tenant",
            "log_tenant",
            "--log-topic-namespace",
            "log_namespace",
        ]);
        let report = run(args).await.unwrap();
        assert_eq!(report.checked_segments, 1);
        assert_eq!(report.corruptions, 1);
        let issue = &report.issues[0];
        assert_eq!(issue.kind, "missing_index_directory");
        assert_eq!(issue.log_state.row_count, 2);
        assert_eq!(issue.log_state.rows_at_or_below_vector_watermark, 1);
        assert_eq!(issue.log_state.rows_below_purge_watermark, 1);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let readonly = open_read_only_sqlite(&path).await.unwrap();
        assert!(sqlx::query("DELETE FROM collections")
            .execute(&readonly)
            .await
            .is_err());
        readonly.close().await;
    }

    #[tokio::test]
    async fn legacy_blob_watermarks_are_not_cast_to_zero() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let row = sqlx::query("SELECT ? AS seq_id")
            .bind(42_u64.to_be_bytes().to_vec())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(read_seq_id(&row, "seq_id").unwrap(), Some(42));
        let row = sqlx::query("SELECT ? AS seq_id")
            .bind(vec![0_u8; 2])
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(read_seq_id(&row, "seq_id").is_err());
    }

    #[test]
    fn flags_pending_startup_fast_forward() {
        let collection_id = CollectionUuid::new();
        let segment = SegmentRow {
            collection_id,
            collection_name: "name".to_string(),
            collection_dimension: Some(3),
            vector_segment_id: "segment".to_string(),
            vector_max_seq_id: None,
            metadata_max_seq_id: Some(10),
        };
        let log_state = LogState {
            topic: "persistent://tenant/database/collection".to_string(),
            row_count: 0,
            min_seq_id: None,
            max_seq_id: None,
            rows_at_or_below_vector_watermark: 0,
            rows_below_purge_watermark: 0,
        };
        let metadata = PersistedHnswMetadata {
            dimensionality: Some(3),
            total_elements_added: 1,
            legacy_max_seq_id: Some(7),
            id_to_label_count: 1,
            label_to_id_count: 1,
            first_label_mismatch: None,
        };
        let mut issues = Vec::new();

        inspect_hnsw_metadata(
            &segment,
            log_state,
            &mut issues,
            Severity::Warning,
            3,
            false,
            metadata,
        );

        assert!(issues.iter().any(|issue| {
            issue.severity == Severity::FastForward && issue.kind == "pending_startup_fast_forward"
        }));
    }
}
