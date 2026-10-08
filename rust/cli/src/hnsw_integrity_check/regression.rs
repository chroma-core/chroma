use super::*;
use chroma_segment::local_hnsw::LocalHnswSegmentWriter;
use chroma_sqlite::db::test_utils::get_new_sqlite_db;
use chroma_types::{
    Chunk, Collection, InternalCollectionConfiguration, InternalHnswConfiguration, LogRecord,
    Operation, OperationRecord, Schema, Segment, SegmentScope, SegmentUuid,
    VectorIndexConfiguration,
};
use proptest::prelude::*;

async fn fixture(persist: bool) -> (tempfile::TempDir, SegmentRow) {
    let root = tempfile::tempdir().unwrap();
    let sqlite = get_new_sqlite_db().await;
    let mut collection = Collection::test_collection(3);
    collection.config = InternalCollectionConfiguration {
        vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
            sync_threshold: 2,
            ..Default::default()
        }),
        embedding_function: None,
    };
    collection.schema = Some(Schema::try_from(&collection.config).unwrap());
    let segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::HnswLocalPersisted,
        scope: SegmentScope::VECTOR,
        collection: collection.collection_id,
        metadata: None,
        file_path: Default::default(),
    };
    let mut writer = LocalHnswSegmentWriter::from_segment(
        &collection,
        &segment,
        3,
        Some(root.path().to_str().unwrap().into()),
        sqlite,
    )
    .await
    .unwrap();
    let records = (1..=if persist { 2 } else { 1 })
        .map(|offset| LogRecord {
            log_offset: offset,
            record: OperationRecord {
                id: offset.to_string(),
                embedding: Some(vec![1.0; 3]),
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        })
        .collect::<Vec<_>>();
    writer
        .apply_log_chunk(Chunk::new(records.into()))
        .await
        .unwrap();
    writer.index.close().await;
    let row = SegmentRow {
        collection_id: collection.collection_id,
        collection_name: collection.name,
        collection_dimension: Some(3),
        vector_segment_id: segment.id.to_string(),
        vector_max_seq_id: persist.then_some(2),
        metadata_max_seq_id: Some(if persist { 2 } else { 1 }),
        configuration_error: None,
    };
    (root, row)
}

fn inspect(path: &Path, segment: &SegmentRow) -> Vec<Issue> {
    let mut issues = vec![];
    inspect_segment(
        path,
        segment,
        LogState {
            topic: "topic".into(),
            row_count: 0,
            min_seq_id: None,
            max_seq_id: None,
            rows_at_or_below_vector_watermark: 0,
            rows_below_purge_watermark: 0,
        },
        &mut issues,
    );
    issues
}

#[tokio::test]
async fn stale_checkpoint_is_reported_as_corruption() {
    let (root, mut row) = fixture(true).await;
    row.vector_max_seq_id = Some(10);
    row.metadata_max_seq_id = Some(10);
    let issues = inspect(root.path(), &row);
    assert!(issues.iter().any(|issue| {
        issue.kind == "hnsw_checkpoint_behind_sqlite" && issue.severity == Severity::Corrupt
    }));
}

#[tokio::test]
async fn checker_validates_configuration_in_current_and_legacy_stores_read_only() {
    for layout in ["legacy", "config", "schema"] {
        for invalid in [false, true] {
            let (root, row) = fixture(true).await;
            let database = root.path().join("chroma.sqlite3");
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(&database)
                        .create_if_missing(true),
                )
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE databases (id TEXT);
                 INSERT INTO databases VALUES ('db');
                 CREATE TABLE collections (id TEXT, name TEXT, dimension INTEGER, database_id TEXT);
                 CREATE TABLE segments (id TEXT, type TEXT, collection TEXT);
                 CREATE TABLE max_seq_id (segment_id TEXT, seq_id INTEGER);
                 CREATE TABLE embeddings_queue (seq_id INTEGER, topic TEXT);
                 CREATE TABLE segment_metadata (segment_id TEXT, key TEXT, str_value TEXT, int_value INTEGER, float_value REAL);",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO collections VALUES (?, 'test', 3, 'db')")
                .bind(row.collection_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO segments VALUES (?, ?, ?)")
                .bind(&row.vector_segment_id)
                .bind(String::from(SegmentType::HnswLocalPersisted))
                .bind(row.collection_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id VALUES (?, 2)")
                .bind(&row.vector_segment_id)
                .execute(&pool)
                .await
                .unwrap();
            let ef = if invalid { 10000 } else { 100 };
            let config = InternalCollectionConfiguration {
                vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                    ef_construction: ef,
                    ..Default::default()
                }),
                embedding_function: None,
            };
            match layout {
                "legacy" => {
                    sqlx::query("INSERT INTO segment_metadata (segment_id, key, int_value) VALUES (?, 'hnsw:construction_ef', ?)")
                        .bind(&row.vector_segment_id)
                        .bind(ef as i64)
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                "config" => {
                    sqlx::query("ALTER TABLE collections ADD COLUMN config_json_str TEXT")
                        .execute(&pool)
                        .await
                        .unwrap();
                    sqlx::query("UPDATE collections SET config_json_str = ?")
                        .bind(serde_json::to_string(&config).unwrap())
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                "schema" => {
                    sqlx::raw_sql("ALTER TABLE collections ADD COLUMN config_json_str TEXT; ALTER TABLE collections ADD COLUMN schema_str TEXT;")
                        .execute(&pool).await.unwrap();
                    sqlx::query("UPDATE collections SET config_json_str = '{}', schema_str = ?")
                        .bind(serde_json::to_string(&Schema::try_from(&config).unwrap()).unwrap())
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            pool.close().await;
            let before = std::fs::read(&database).unwrap();
            let args = HnswIntegrityCheckArgs::parse_from([
                "check",
                "--path",
                root.path().to_str().unwrap(),
            ]);
            let report = run(args).await.unwrap();
            assert_eq!(
                report.corruptions,
                usize::from(invalid),
                "{layout}: {report:?}"
            );
            if invalid {
                assert_eq!(report.issues[0].kind, "invalid_hnsw_configuration");
            }
            assert_eq!(CheckOutcome { report }.has_findings(), invalid);
            assert_eq!(std::fs::read(&database).unwrap(), before);
        }
    }
}

#[tokio::test]
async fn healthy_indexes_pass_before_and_after_first_persist() {
    for persisted in [false, true] {
        let (root, row) = fixture(persisted).await;
        let issues = inspect(root.path(), &row);
        assert!(issues.is_empty(), "{issues:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn truncated_native_files_are_corrupt(file in 0..4usize, length in 0..4u64) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (root, row) = fixture(true).await;
            let path = root.path().join(&row.vector_segment_id).join(HNSW_INDEX_FILES[file]);
            std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(length).unwrap();
            let before = std::fs::read(&path).unwrap();
            let issues = inspect(root.path(), &row);
            assert!(issues.iter().any(|issue| issue.severity == Severity::Corrupt && issue.kind == "invalid_hnsw_index"), "{issues:?}");
            assert_eq!(std::fs::read(path).unwrap(), before);
        });
    }
}

#[tokio::test]
async fn recovery_required_is_a_finding_even_without_replay_logs() {
    let (root, row) = fixture(true).await;
    let index = root.path().join(&row.vector_segment_id);
    let path = index.join("data_level0.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    let flags = u32::from_ne_bytes(bytes[..4].try_into().unwrap()) | 0x10000;
    bytes[..4].copy_from_slice(&flags.to_ne_bytes());
    std::fs::write(path, bytes).unwrap();
    assert!(
        inspect_persisted_hnsw_index(&index)
            .unwrap()
            .recovery_required
    );
    let issues = inspect(root.path(), &row);
    assert!(
        issues
            .iter()
            .any(|issue| issue.severity == Severity::Corrupt
                && issue.kind == "hnsw_checkpoint_requires_recovery"),
        "{issues:?}"
    );
    let outcome = CheckOutcome {
        report: Report {
            persist_path: root.path().display().to_string(),
            sqlite_path: String::new(),
            checked_segments: 1,
            pending_fast_forwards: 0,
            corruptions: issues
                .iter()
                .filter(|issue| issue.severity == Severity::Corrupt)
                .count(),
            warnings: 0,
            issues,
        },
    };
    assert!(outcome.has_findings());
    assert_eq!(outcome.exit_code(), ExitCode::from(1));
}

#[tokio::test]
async fn checkpoint_offsets_report_pending_startup_restoration() {
    let (root, mut row) = fixture(true).await;
    for (watermark, pending, corrupt) in [
        (None, true, false),
        (Some(1), true, false),
        (Some(2), false, false),
        (Some(3), false, true),
    ] {
        row.vector_max_seq_id = watermark;
        let issues = inspect(root.path(), &row);
        assert_eq!(
            issues
                .iter()
                .any(|issue| issue.kind == "pending_startup_fast_forward"),
            pending
        );
        assert_eq!(
            issues
                .iter()
                .any(|issue| issue.severity == Severity::Corrupt),
            corrupt,
        );
    }
}
