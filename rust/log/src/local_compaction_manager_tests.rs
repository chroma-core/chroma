use super::*;
use crate::sqlite_log::{LegacyEmbeddingsQueueConfig, SqliteLog, SqlitePushLogsError};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sysdb::test_sysdb::TestSysDb;
use chroma_system::{ComponentHandle, System};
use chroma_types::{
    Collection, KnnIndex, Operation, OperationRecord, Segment, SegmentScope, SegmentType,
};

struct Fixture {
    db: SqliteDb,
    log: SqliteLog,
    collection: Collection,
    metadata: SegmentUuid,
    vector: SegmentUuid,
    sysdb: TestSysDb,
    handle: ComponentHandle<LocalCompactionManager>,
    system: System,
    compactor_joined: bool,
    path: tempfile::TempDir,
    persistent: bool,
    manager: LocalSegmentManager,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_persistence(true).await
    }

    async fn with_persistence(persistent: bool) -> Self {
        let db = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
        let registry = Registry::new();
        registry.register(db.clone());
        let path = tempfile::tempdir().unwrap();
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": persistent.then(|| path.path().to_str().unwrap())
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let metadata = SegmentUuid::new();
        let vector = SegmentUuid::new();
        let mut sysdb = TestSysDb::new();
        sysdb.add_collection(collection.clone());
        for (id, scope, r#type) in [
            (metadata, SegmentScope::METADATA, SegmentType::Sqlite),
            (
                vector,
                SegmentScope::VECTOR,
                SegmentType::HnswLocalPersisted,
            ),
        ] {
            // The segment manager checks SQLite for deletion even with a mock sysdb.
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(id.to_string())
                .bind(String::from(r#type))
                .bind(String::from(scope.clone()))
                .bind(collection.collection_id.to_string())
                .execute(db.get_conn())
                .await
                .unwrap();
            sysdb.add_segment(Segment {
                id,
                scope,
                r#type,
                collection: collection.collection_id,
                metadata: None,
                file_path: Default::default(),
            });
        }
        let log = SqliteLog::new(db.clone(), "default".into(), "default".into());
        log.update_legacy_embeddings_queue_config(LegacyEmbeddingsQueueConfig {
            automatically_purge: true,
            kind: "EmbeddingsQueueConfigurationInternal".into(),
        })
        .await
        .unwrap();
        let system = System::new();
        let handle = system.start_component(LocalCompactionManager {
            log: Log::Sqlite(log.clone()),
            sqlite_db: db.clone(),
            hnsw_segment_manager: manager.clone(),
            metrics: LocalCompactionMetrics::default(),
            sysdb: SysDb::Test(sysdb.clone()),
        });
        Self {
            db,
            log,
            collection,
            metadata,
            vector,
            sysdb,
            handle,
            system,
            compactor_joined: false,
            path,
            persistent,
            manager,
        }
    }

    async fn watermark(&self, segment: SegmentUuid, seq: i64) {
        sqlx::query("INSERT OR REPLACE INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
            .bind(segment.to_string())
            .bind(seq)
            .execute(self.db.get_conn())
            .await
            .unwrap();
    }

    async fn seed(&mut self) -> Vec<LogRecord> {
        self.log
            .push_logs(
                self.collection.collection_id,
                (0..5).map(operation).collect(),
            )
            .await
            .unwrap();
        self.records().await
    }

    // Materialize real files for tests that authorize deleting checkpointed logs.
    // Keep watermarks under each test's control, including missing-watermark cases.
    async fn checkpoint(&mut self) {
        let mut collection = self.collection.clone();
        if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
            &mut collection.config.vector_index
        {
            config.sync_threshold = 2;
        }
        collection.schema = Some(Schema::try_from(&collection.config).unwrap());
        let segments = SysDb::Test(self.sysdb.clone())
            .get_collection_with_segments(None, collection.collection_id)
            .await
            .unwrap();
        let mut writer = chroma_segment::local_hnsw::LocalHnswSegmentWriter::from_segment(
            &collection,
            &segments.vector_segment,
            3,
            Some(self.path.path().to_str().unwrap().into()),
            self.db.clone(),
        )
        .await
        .unwrap();
        writer
            .apply_log_chunk(Chunk::new(self.records().await.into()))
            .await
            .unwrap();
        writer.index.close().await;
        sqlx::query("DELETE FROM max_seq_id WHERE segment_id = ?")
            .bind(self.vector.to_string())
            .execute(self.db.get_conn())
            .await
            .unwrap();
    }

    fn break_collection_configuration(&mut self) {
        self.collection.dimension = Some(4);
        self.sysdb.add_collection(self.collection.clone());
    }

    async fn records(&mut self) -> Vec<LogRecord> {
        self.log
            .read(self.collection.collection_id, 0, -1, None)
            .await
            .unwrap()
    }

    async fn purge(&self) -> Result<(), CompactionManagerError> {
        self.handle
            .request(
                PurgeLogsMessage {
                    collection_id: self.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap()
    }

    async fn stop_compactor(&mut self) {
        if !self.compactor_joined {
            self.handle.stop();
            self.handle.join().await.unwrap();
            self.compactor_joined = true;
        }
    }

    async fn restart_compactor(&mut self) {
        self.stop_compactor().await;
        let registry = Registry::new();
        registry.register(self.db.clone());
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": self.persistent.then(|| self.path.path().to_str().unwrap())
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
        self.compactor_joined = false;
        self.manager = manager.clone();
        self.handle = self.system.start_component(LocalCompactionManager {
            log: Log::Sqlite(SqliteLog::new(
                self.db.clone(),
                "default".into(),
                "default".into(),
            )),
            sqlite_db: self.db.clone(),
            hnsw_segment_manager: manager.clone(),
            metrics: LocalCompactionMetrics::default(),
            sysdb: SysDb::Test(self.sysdb.clone()),
        });
    }

    async fn stop(mut self) {
        self.stop_compactor().await;
        self.system.stop().await;
        self.system.join().await;
    }
}

fn operation(id: usize) -> OperationRecord {
    OperationRecord {
        id: format!("id-{id}"),
        embedding: Some(vec![1.0, 2.0, 3.0]),
        encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
        metadata: None,
        document: Some(format!("document-{id}")),
        operation: Operation::Add,
    }
}

#[tokio::test]
async fn purge_watermark_matrix_preserves_boundary_and_other_collections() {
    // Missing, zero, equal, and either segment lagging must all be conservative.
    for (metadata, vector) in [
        (None, None),
        (Some(4), None),
        (None, Some(4)),
        (Some(0), Some(4)),
        (Some(4), Some(0)),
        (Some(4), Some(2)),
        (Some(2), Some(4)),
        (Some(3), Some(3)),
    ] {
        let mut f = Fixture::new().await;
        let original = f.seed().await;
        f.checkpoint().await;
        let other = CollectionUuid::new();
        f.log.push_logs(other, vec![operation(99)]).await.unwrap();
        let other_records = f.log.read(other, 0, -1, None).await.unwrap();
        // Unrelated watermarks must not influence the target collection.
        f.watermark(SegmentUuid::new(), 999).await;
        if let Some(seq) = metadata {
            f.watermark(f.metadata, seq).await;
        }
        if let Some(seq) = vector {
            f.watermark(f.vector, seq).await;
        }
        let boundary = metadata.unwrap_or(0).min(vector.unwrap_or(0));
        let expected: Vec<_> = original
            .into_iter()
            .filter(|r| r.log_offset >= boundary)
            .collect();
        f.purge().await.unwrap();
        assert_records(f.records().await, expected.clone());
        assert_records(f.log.read(other, 0, -1, None).await.unwrap(), other_records);
        f.purge().await.unwrap();
        assert_records(f.records().await, expected);
        f.stop().await;
    }
}

#[tokio::test]
async fn purge_disabled_preserves_all_records() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 4).await;
    f.log
        .update_legacy_embeddings_queue_config(LegacyEmbeddingsQueueConfig {
            automatically_purge: false,
            kind: "EmbeddingsQueueConfigurationInternal".into(),
        })
        .await
        .unwrap();
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

#[tokio::test]
async fn purge_does_not_require_dimension_or_schema() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.collection.dimension = None;
    f.collection.schema = None;
    f.sysdb.add_collection(f.collection.clone());
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 3).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn purge_preserves_all_logs_when_checkpoint_files_are_missing_or_corrupt() {
    for file in [
        "index_metadata.pickle",
        "header.bin",
        "data_level0.bin",
        "length.bin",
        "link_lists.bin",
    ] {
        for missing in [false, true] {
            let mut f = Fixture::new().await;
            let original = f.seed().await;
            f.checkpoint().await;
            f.watermark(f.metadata, 4).await;
            f.watermark(f.vector, 3).await;
            let path = f.path.path().join(f.vector.to_string()).join(file);
            let saved = std::fs::read(&path).unwrap();
            if missing {
                std::fs::remove_file(&path).unwrap();
            } else {
                std::fs::write(&path, b"bad").unwrap();
            }
            let backfill = f
                .handle
                .request(
                    BackfillMessage {
                        collection_id: f.collection.collection_id,
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(
                matches!(
                    backfill,
                    Err(CompactionManagerError::HnswReaderConstructionError(_))
                ),
                "{file}, missing={missing}: {backfill:?}"
            );
            for _ in 0..2 {
                let result = f.purge().await;
                assert!(
                    matches!(result, Err(CompactionManagerError::UnsafeHnswCheckpoint(_))),
                    "{file}, missing={missing}: {result:?}"
                );
                assert_records(f.records().await, original.clone());
            }
            // Restoring a usable checkpoint permits purging again.
            std::fs::write(&path, saved).unwrap();
            f.purge().await.unwrap();
            assert_records(
                f.records().await,
                original.into_iter().filter(|r| r.log_offset >= 3).collect(),
            );
            f.stop().await;
        }
    }
}

#[tokio::test]
async fn missing_checkpoint_directory_preserves_logs_and_new_writes() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn cached_index_does_not_hide_corruption_from_purge() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 5).await;
    f.watermark(f.vector, 5).await;
    // Load the healthy checkpoint into the manager's cache.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let header = f.path.path().join(f.vector.to_string()).join("header.bin");
    let saved = std::fs::read(&header).unwrap();
    std::fs::write(&header, b"bad").unwrap();
    // The live index still works, but cannot establish that disk is recoverable.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original.clone());
    std::fs::write(&header, saved).unwrap();
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn interrupted_checkpoint_preserves_logs_until_files_are_consistent() {
    let mut f = Fixture::new().await;
    f.seed().await;
    f.checkpoint().await;
    f.watermark(f.vector, 5).await;
    let folder = f.path.path().join(f.vector.to_string());
    let pickle = folder.join("index_metadata.pickle");
    let old_map = std::fs::read(&pickle).unwrap();
    f.log
        .push_logs(f.collection.collection_id, vec![operation(5), operation(6)])
        .await
        .unwrap();
    let original = f.records().await;
    f.checkpoint().await;
    let new_map = std::fs::read(&pickle).unwrap();
    // Simulate native files being saved before the ID map and SQLite watermark.
    std::fs::write(&pickle, old_map).unwrap();
    f.watermark(f.metadata, 7).await;
    f.watermark(f.vector, 5).await;
    assert!(
        chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
            .unwrap()
            .recovery_required
    );
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original.clone());
    std::fs::write(&pickle, new_map).unwrap();
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn retained_logs_allow_full_vector_rebuild_after_checkpoint_loss() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    let original = f.seed().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let folder = f.path.path().join(f.vector.to_string());
    std::fs::write(folder.join("header.bin"), b"bad").unwrap();
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original);

    // Model an operator discarding the unusable vector checkpoint to rebuild
    // from the preserved log. Production purging never removes these files or
    // resets watermarks on its own.
    f.stop_compactor().await;
    std::fs::remove_dir_all(&folder).unwrap();
    sqlx::query("DELETE FROM max_seq_id WHERE segment_id = ?")
        .bind(f.vector.to_string())
        .execute(f.db.get_conn())
        .await
        .unwrap();
    f.restart_compactor().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let watermarks = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            watermarks.current_max_seq_id(&f.metadata).await.unwrap(),
            watermarks.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 5)
    );
    f.purge().await.unwrap();
    f.stop_compactor().await;
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let reader = chroma_segment::local_hnsw::LocalHnswSegmentReader::from_segment(
        &f.collection,
        &segments.vector_segment,
        3,
        Some(f.path.path().to_str().unwrap().into()),
        f.db.clone(),
    )
    .await
    .unwrap();
    let mut embeddings = Vec::new();
    for id in 0..5 {
        embeddings.push(
            reader
                .get_embedding_by_user_id(&format!("id-{id}"))
                .await
                .unwrap(),
        );
    }
    assert_eq!(embeddings, vec![vec![1.0, 2.0, 3.0]; 5]);
    reader.index.close().await;
    f.stop().await;
}

#[tokio::test]
async fn purge_errors_leave_logs_intact() {
    for failure in ["watermark", "collection", "delete", "config"] {
        let mut f = Fixture::new().await;
        let original = f.seed().await;
        f.checkpoint().await;
        f.watermark(f.metadata, 4).await;
        f.watermark(f.vector, 4).await;
        match failure {
            "watermark" => {
                sqlx::query("DROP TABLE max_seq_id")
                    .execute(f.db.get_conn())
                    .await
                    .unwrap();
            }
            "collection" => f.sysdb.remove_collection(f.collection.collection_id),
            "delete" => {
                sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
                .execute(f.db.get_conn()).await.unwrap();
            }
            "config" => {
                sqlx::query("UPDATE embeddings_queue_config SET config_json_str = 'invalid json'")
                    .execute(f.db.get_conn())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let result = f.purge().await;
        match failure {
            "watermark" => assert!(
                matches!(result, Err(CompactionManagerError::MetadataReaderError(_))),
                "{result:?}"
            ),
            "collection" => assert!(
                matches!(
                    result,
                    Err(CompactionManagerError::GetCollectionWithSegmentsError(_))
                ),
                "{result:?}"
            ),
            _ => assert!(
                matches!(result, Err(CompactionManagerError::PurgeLogsFailure)),
                "{result:?}"
            ),
        }
        assert_records(f.records().await, original);
        f.stop().await;
    }
}

#[tokio::test]
async fn push_attempts_purge_after_backfill_failure_and_retains_new_records() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.break_collection_configuration();
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    let mut expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn push_returns_purge_failure_when_backfill_succeeds() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    // An uninitialized collection makes backfill a successful no-op.
    f.collection.dimension = None;
    f.sysdb.add_collection(f.collection.clone());
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
        .execute(f.db.get_conn()).await.unwrap();
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::PurgeLogsFailure
            ))
        ),
        "{result:?}"
    );
    let mut expected = original;
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn watermark_supports_sqlite_integer_limit() {
    let f = Fixture::new().await;
    f.watermark(f.metadata, i64::MAX).await;
    f.watermark(f.vector, i64::MAX - 1).await;
    assert_eq!(
        max_purge_seq_id(&f.db, &f.metadata, &f.vector)
            .await
            .unwrap(),
        (i64::MAX - 1) as u64
    );
    f.stop().await;
}

// Compare the complete wire representation because LogRecord lacks PartialEq.
fn assert_records(actual: Vec<LogRecord>, expected: Vec<LogRecord>) {
    fn wire(records: Vec<LogRecord>) -> Vec<chroma_types::chroma_proto::LogRecord> {
        records
            .into_iter()
            .map(|r| chroma_types::chroma_proto::LogRecord {
                log_offset: r.log_offset,
                record: Some(r.record.try_into().unwrap()),
            })
            .collect()
    }
    assert_eq!(wire(actual), wire(expected));
}

#[derive(Clone, Default)]
struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn push_logs_both_failures_but_returns_backfill_error() {
    // Keep a process-wide subscriber alive: scoped subscriber registration can
    // race with the same callsite firing in parallel tests without a subscriber.
    // Filter captured lines by this test's unique collection to avoid accepting
    // another test's purge failure as evidence.
    static CAPTURE: std::sync::OnceLock<Capture> = std::sync::OnceLock::new();
    let capture = CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        let writer = capture.clone();
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::ERROR)
                .with_writer(move || writer.clone())
                .finish(),
        )
        .unwrap();
        capture
    });

    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.checkpoint().await;
    f.break_collection_configuration();
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
        .execute(f.db.get_conn()).await.unwrap();
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let collection_id = f.collection.collection_id.to_string();
    let output = output
        .lines()
        .filter(|line| line.contains(&collection_id))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        output.contains("Failed to purge logs after backfill attempt"),
        "{output}"
    );
    assert!(output.contains("Error purging logs"), "{output}");
    assert!(
        output.contains(&f.collection.collection_id.to_string()),
        "{output}"
    );
    f.stop().await;
}

#[tokio::test]
async fn push_to_stopped_compactor_preserves_committed_records() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.stop_compactor().await;
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(result, Err(SqlitePushLogsError::MessageSendingError(_))),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn empty_push_does_not_request_backfill_or_purge() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.stop_compactor().await;
    f.log
        .push_logs(f.collection.collection_id, vec![])
        .await
        .unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

#[tokio::test]
async fn successful_push_purges_and_keeps_new_record() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.collection.dimension = None;
    f.sysdb.add_collection(f.collection.clone());
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 4).await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await
        .unwrap();
    let mut expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn uncheckpointed_vectors_keep_all_logs_for_replay() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    // The batch is below HNSW's persistence threshold: metadata has advanced,
    // but the vector checkpoint must still be absent.
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 0)
    );
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

#[tokio::test]
async fn incompatible_index_configuration_never_purges_from_metadata_alone() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.watermark(f.metadata, 5).await;
    f.collection.schema = Some(Schema::new_default(KnnIndex::Spann));
    f.sysdb.add_collection(f.collection.clone());
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn vector_checkpoint_sqlite_failure_preserves_replay_logs() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    } else {
        panic!("expected HNSW configuration");
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    let original = f.seed().await;
    // Fail only vector checkpoint publication, after native persistence and
    // metadata compaction have succeeded.
    sqlx::query(&format!("CREATE TRIGGER reject_vector_checkpoint BEFORE INSERT ON max_seq_id WHEN NEW.segment_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END", f.vector))
        .execute(f.db.get_conn()).await.unwrap();
    let result = f
        .handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(result, Err(CompactionManagerError::HnswApplyLogsError)),
        "{result:?}"
    );
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 0)
    );
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    sqlx::query("DROP TRIGGER reject_vector_checkpoint")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    // Retrying with no new logs must still publish the failed checkpoint.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader.current_max_seq_id(&f.vector).await.unwrap(), 5);
    f.purge().await.unwrap();
    assert_eq!(f.records().await.len(), 1);
    f.stop().await;
}

#[tokio::test]
async fn valid_checkpoint_recovers_retained_tail_after_configuration_is_repaired() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    } else {
        panic!("expected HNSW configuration");
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    f.log
        .push_logs(f.collection.collection_id, (0..3).map(operation).collect())
        .await
        .unwrap();
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (3, 3)
    );
    f.log
        .push_logs(f.collection.collection_id, (3..5).map(operation).collect())
        .await
        .unwrap();
    let original = f.records().await;

    // A wrong collection dimension must not prevent purging the checkpointed
    // prefix, or discard the uncheckpointed tail needed after configuration repair.
    f.restart_compactor().await;
    f.collection.dimension = Some(4);
    f.sysdb.add_collection(f.collection.clone());
    let result = f
        .handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            Err(CompactionManagerError::HnswReaderConstructionError(_))
        ),
        "{result:?}"
    );
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 3).collect(),
    );
    f.collection.dimension = Some(3);
    f.sysdb.add_collection(f.collection.clone());
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 5)
    );

    // Reopen from disk through a fresh manager; checking only the live writer
    // would miss a watermark that advanced ahead of persisted vectors.
    f.stop_compactor().await;
    let registry = Registry::new();
    registry.register(f.db.clone());
    let manager = LocalSegmentManager::try_from_config(
        &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
            "persist_path": f.path.path().to_str().unwrap()
        }))
        .unwrap(),
        &registry,
    )
    .await
    .unwrap();
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let hnsw = manager
        .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
        .await
        .unwrap();
    let mut embeddings = Vec::new();
    for id in 0..5 {
        embeddings.push(
            hnsw.get_embedding_by_user_id(&format!("id-{id}"))
                .await
                .unwrap(),
        );
    }
    assert_eq!(embeddings, vec![vec![1.0, 2.0, 3.0]; 5]);
    f.stop().await;
}

#[tokio::test]
async fn live_backfill_skips_applied_logs_but_reload_replays_retained_history() {
    for persistent in [false, true] {
        let mut f = Fixture::with_persistence(persistent).await;
        let original = f.seed().await;
        let message = || BackfillMessage {
            collection_id: f.collection.collection_id,
        };
        f.handle.request(message(), None).await.unwrap().unwrap();
        assert_eq!(
            max_purge_seq_id(&f.db, &f.metadata, &f.vector)
                .await
                .unwrap(),
            0
        );
        // Poison only an already-applied log's encoding: decoding old records
        // again would fail, even though filtering later skips their mutations.
        sqlx::query("UPDATE embeddings_queue SET encoding = 'invalid' WHERE seq_id = ?")
            .bind(original[0].log_offset)
            .execute(f.db.get_conn())
            .await
            .unwrap();
        f.log
            .push_logs(f.collection.collection_id, vec![operation(99)])
            .await
            .unwrap();
        f.handle.request(message(), None).await.unwrap().unwrap();
        f.purge().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
            .fetch_one(f.db.get_conn())
            .await
            .unwrap();
        assert_eq!(
            count, 6,
            "live replay progress must never authorize purging"
        );
        sqlx::query("UPDATE embeddings_queue SET encoding = 'FLOAT32' WHERE seq_id = ?")
            .bind(original[0].log_offset)
            .execute(f.db.get_conn())
            .await
            .unwrap();
        f.restart_compactor().await;
        f.handle
            .request(
                BackfillMessage {
                    collection_id: f.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.records().await.len(), 6);
        let segments = SysDb::Test(f.sysdb.clone())
            .get_collection_with_segments(None, f.collection.collection_id)
            .await
            .unwrap();
        let reader = f
            .manager
            .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
            .await
            .unwrap();
        for id in [0, 1, 2, 3, 4, 99] {
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&format!("id-{id}"))
                    .await
                    .unwrap(),
                vec![1.0, 2.0, 3.0]
            );
        }
        drop(reader);
        f.stop().await;
    }
}

#[tokio::test]
async fn recovered_short_tail_checkpoints_before_purge_and_retries_publication() {
    for fail_publication in [false, true] {
        let mut f = Fixture::new().await;
        f.seed().await;
        f.checkpoint().await;
        let folder = f.path.path().join(f.vector.to_string());
        let pickle = folder.join("index_metadata.pickle");
        let old_map = std::fs::read(&pickle).unwrap();
        f.log
            .push_logs(f.collection.collection_id, vec![operation(5), operation(6)])
            .await
            .unwrap();
        f.checkpoint().await;
        // Interrupt the save after native files, before the pickle and watermark.
        std::fs::write(&pickle, old_map).unwrap();
        f.watermark(f.metadata, 5).await;
        f.watermark(f.vector, 5).await;
        assert!(
            chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        // The checkpoint helper used a threshold of 2. Reopen with a higher
        // threshold so replaying the two retained records cannot normally save.
        if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
            &mut f.collection.config.vector_index
        {
            config.sync_threshold = 1000;
        }
        f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
        f.sysdb.add_collection(f.collection.clone());
        f.restart_compactor().await;
        if fail_publication {
            sqlx::query(&format!("CREATE TRIGGER reject_recovery_checkpoint BEFORE INSERT ON max_seq_id WHEN NEW.segment_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END", f.vector))
                .execute(f.db.get_conn()).await.unwrap();
        }
        let replay = f
            .handle
            .request(
                BackfillMessage {
                    collection_id: f.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap();
        if fail_publication {
            assert!(matches!(
                replay,
                Err(CompactionManagerError::HnswApplyLogsError)
            ));
            assert_eq!(
                SqliteMetadataReader::new(f.db.clone())
                    .current_max_seq_id(&f.vector)
                    .await
                    .unwrap(),
                5
            );
            sqlx::query("DROP TRIGGER reject_recovery_checkpoint")
                .execute(f.db.get_conn())
                .await
                .unwrap();
            // No new logs: the recovery checkpoint must remain pending even
            // though native mutations and file publication already succeeded.
            f.handle
                .request(
                    BackfillMessage {
                        collection_id: f.collection.collection_id,
                    },
                    None,
                )
                .await
                .unwrap()
                .unwrap();
        } else {
            replay.unwrap();
        }
        assert_eq!(
            SqliteMetadataReader::new(f.db.clone())
                .current_max_seq_id(&f.vector)
                .await
                .unwrap(),
            7
        );
        assert!(
            !chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        f.purge().await.unwrap();
        let retained = f.records().await;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].log_offset, 7);
        f.stop().await;
    }
}

#[tokio::test]
async fn watermark_read_failure_invalidates_replay_completion() {
    let mut f = Fixture::new().await;
    f.seed().await;
    let message = BackfillMessage {
        collection_id: f.collection.collection_id,
    };
    f.handle
        .request(message.clone(), None)
        .await
        .unwrap()
        .unwrap();
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let reader = f
        .manager
        .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
        .await
        .unwrap();
    assert!(reader.index.replay_complete().await);
    f.log
        .push_logs(f.collection.collection_id, vec![operation(99)])
        .await
        .unwrap();

    // Fail the metadata watermark query while keeping the loaded index usable.
    sqlx::query("ALTER TABLE max_seq_id RENAME TO saved_max_seq_id")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    let result = f.handle.request(message.clone(), None).await.unwrap();
    assert!(matches!(
        result,
        Err(CompactionManagerError::MetadataReaderError(_))
    ));
    assert!(!reader.index.replay_complete().await);

    sqlx::query("ALTER TABLE saved_max_seq_id RENAME TO max_seq_id")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    f.handle.request(message, None).await.unwrap().unwrap();
    assert!(reader.index.replay_complete().await);
    assert_eq!(
        reader
            .get_embedding_by_user_id(&"id-99".to_string())
            .await
            .unwrap(),
        vec![1.0, 2.0, 3.0]
    );
    drop(reader);
    f.stop().await;
}
