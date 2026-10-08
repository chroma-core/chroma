use super::*;
use chroma_log::{
    local_compaction_manager::LocalCompactionManagerConfig, sqlite_log::SqliteLog, Log,
};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sysdb::{test_sysdb::TestSysDb, SysDb};
use chroma_system::System;
use chroma_types::{
    Chunk, Collection, KnnIndex, LogRecord, Operation, OperationRecord, Schema, Segment,
    SegmentScope, SegmentUuid,
};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Fixture {
    executor: LocalExecutor,
    db: SqliteDb,
    sysdb: TestSysDb,
    segments: CollectionAndSegments,
    system: System,
    compactor_joined: bool,
    path: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let registry = Registry::new();
        let db = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
        registry.register(db.clone());
        let path = tempfile::tempdir().unwrap();
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": path.path().to_str().unwrap()
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        // A successful backfill does no work; failures below use a wrong dimension.
        collection.dimension = None;
        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::METADATA,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let mut checkpoint_collection = collection.clone();
        if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
            &mut checkpoint_collection.config.vector_index
        {
            config.sync_threshold = 2;
        }
        checkpoint_collection.schema =
            Some(Schema::try_from(&checkpoint_collection.config).unwrap());
        let mut writer = chroma_segment::local_hnsw::LocalHnswSegmentWriter::from_segment(
            &checkpoint_collection,
            &vector_segment,
            3,
            Some(path.path().to_str().unwrap().into()),
            db.clone(),
        )
        .await
        .unwrap();
        writer
            .apply_log_chunk(Chunk::new(
                (1..=3)
                    .map(|seq| LogRecord {
                        log_offset: seq,
                        record: OperationRecord {
                            id: format!("id-{seq}"),
                            embedding: Some(vec![1.0, 2.0, 3.0]),
                            encoding: None,
                            document: None,
                            metadata: None,
                            operation: Operation::Add,
                        },
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ))
            .await
            .unwrap();
        writer.index.close().await;
        drop(writer);
        let mut sysdb = TestSysDb::new();
        sysdb.add_collection(collection.clone());
        sysdb.add_segment(metadata_segment.clone());
        sysdb.add_segment(vector_segment.clone());
        registry.register(SysDb::Test(sysdb.clone()));
        registry.register(Log::Sqlite(SqliteLog::new(
            db.clone(),
            "default".into(),
            "default".into(),
        )));
        // Purging requires both durable progress and the usable files written above.
        for segment in [&metadata_segment, &vector_segment] {
            sqlx::query("INSERT OR REPLACE INTO max_seq_id (segment_id, seq_id) VALUES (?, 3)")
                .bind(segment.id.to_string())
                .execute(db.get_conn())
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO embeddings_queue_config (id, config_json_str) VALUES (1, '{\"automatically_purge\":true}')")
            .execute(db.get_conn()).await.unwrap();
        let topic = chroma_sqlite::helpers::get_embeddings_queue_topic_name(
            "default",
            "default",
            collection.collection_id,
        );
        for seq in 1..=4 {
            sqlx::query(
                "INSERT INTO embeddings_queue (seq_id, operation, topic, id) VALUES (?, 0, ?, ?)",
            )
            .bind(seq)
            .bind(&topic)
            .bind(format!("id-{seq}"))
            .execute(db.get_conn())
            .await
            .unwrap();
        }
        let compactor =
            LocalCompactionManager::try_from_config(&LocalCompactionManagerConfig {}, &registry)
                .await
                .unwrap();
        let system = System::new();
        let handle = system.start_component(compactor);
        Self {
            executor: LocalExecutor::new(manager, db.clone(), handle),
            db,
            sysdb,
            segments: CollectionAndSegments {
                collection,
                record_segment: metadata_segment.clone(),
                metadata_segment,
                vector_segment,
            },
            system,
            compactor_joined: false,
            path,
        }
    }

    async fn offsets(&self) -> Vec<i64> {
        sqlx::query_scalar("SELECT seq_id FROM embeddings_queue ORDER BY seq_id")
            .fetch_all(self.db.get_conn())
            .await
            .unwrap()
    }

    async fn stop(mut self) {
        if !self.compactor_joined {
            self.executor.compactor_handle.stop();
            self.executor.compactor_handle.join().await.unwrap();
        }
        self.system.stop().await;
        self.system.join().await;
    }
}

#[tokio::test]
async fn backfill_and_purge_error_matrix_logs_failures_and_only_caches_success() {
    for (backfill_fails, purge_fails) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let mut f = Fixture::new().await;
        if backfill_fails {
            f.segments.collection.dimension = Some(4);
            f.sysdb.add_collection(f.segments.collection.clone());
        }
        if purge_fails {
            sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
                .execute(f.db.get_conn()).await.unwrap();
        }
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let result = f
            .executor
            .try_backfill_collection(&f.segments)
            .with_subscriber(subscriber)
            .await;
        let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        if backfill_fails {
            let error = result.unwrap_err();
            assert!(
                matches!(error, ExecutorError::BackfillError(_)),
                "{error:?}"
            );
            assert!(
                error
                    .to_string()
                    .contains("Error constructing hnsw segment reader"),
                "{error}"
            );
        } else if purge_fails {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Error purging logs"));
        } else {
            result.unwrap();
        }
        assert_eq!(
            output.contains("Failed to purge logs after backfill attempt"),
            purge_fails,
            "{output}"
        );
        if purge_fails {
            assert!(
                output.contains(&f.segments.collection.collection_id.to_string()),
                "{output}"
            );
            assert!(output.contains("Error purging logs"), "{output}");
        }
        let successful = !backfill_fails && !purge_fails;
        assert_eq!(
            *f.executor.backfilled_collections.lock(),
            if successful {
                HashSet::from([f.segments.collection.collection_id])
            } else {
                HashSet::new()
            }
        );
        assert_eq!(
            f.offsets().await,
            if purge_fails {
                vec![1, 2, 3, 4]
            } else {
                vec![3, 4]
            }
        );

        // Repair the failure and prove failed attempts remain eligible for retry.
        f.segments.collection.dimension = None;
        f.sysdb.add_collection(f.segments.collection.clone());
        sqlx::query("DROP TRIGGER IF EXISTS reject_purge")
            .execute(f.db.get_conn())
            .await
            .unwrap();
        f.executor
            .try_backfill_collection(&f.segments)
            .await
            .unwrap();
        assert_eq!(
            *f.executor.backfilled_collections.lock(),
            HashSet::from([f.segments.collection.collection_id])
        );
        assert_eq!(f.offsets().await, vec![3, 4]);

        // Once successful, a cloned executor shares the cache and makes no requests.
        f.sysdb
            .remove_collection(f.segments.collection.collection_id);
        let mut clone = f.executor.clone();
        clone.try_backfill_collection(&f.segments).await.unwrap();
        // A different collection must not be covered by that cache entry.
        let mut other = f.segments.clone();
        other.collection.collection_id = CollectionUuid::new();
        assert!(clone.try_backfill_collection(&other).await.is_err());
        assert_eq!(
            *clone.backfilled_collections.lock(),
            HashSet::from([f.segments.collection.collection_id])
        );
        f.stop().await;
    }
}

#[tokio::test]
async fn stopped_compactor_reports_request_failures_without_caching_success() {
    let mut f = Fixture::new().await;
    f.executor.compactor_handle.stop();
    f.executor.compactor_handle.join().await.unwrap();
    f.compactor_joined = true;
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let result = f
        .executor
        .try_backfill_collection(&f.segments)
        .with_subscriber(subscriber)
        .await;
    assert!(
        matches!(result, Err(ExecutorError::BackfillError(_))),
        "{result:?}"
    );
    assert_eq!(*f.executor.backfilled_collections.lock(), HashSet::new());
    assert_eq!(f.offsets().await, vec![1, 2, 3, 4]);
    let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("Failed to purge logs after backfill attempt"),
        "{output}"
    );
    f.stop().await;
}

#[tokio::test]
async fn corrupt_checkpoint_blocks_purge_and_success_caching_until_repaired() {
    for backfill_fails in [false, true] {
        let mut f = Fixture::new().await;
        let header = f
            .path
            .path()
            .join(f.segments.vector_segment.id.to_string())
            .join("header.bin");
        let saved = std::fs::read(&header).unwrap();
        std::fs::write(&header, b"bad").unwrap();
        if backfill_fails {
            f.segments.collection.dimension = Some(3);
            f.sysdb.add_collection(f.segments.collection.clone());
        }
        let result = f.executor.try_backfill_collection(&f.segments).await;
        assert!(
            matches!(result, Err(ExecutorError::BackfillError(_))),
            "{result:?}"
        );
        assert_eq!(*f.executor.backfilled_collections.lock(), HashSet::new());
        assert_eq!(f.offsets().await, vec![1, 2, 3, 4]);
        std::fs::write(&header, saved).unwrap();
        f.segments.collection.dimension = None;
        f.sysdb.add_collection(f.segments.collection.clone());
        f.executor
            .try_backfill_collection(&f.segments)
            .await
            .unwrap();
        assert_eq!(f.offsets().await, vec![3, 4]);
        assert_eq!(
            *f.executor.backfilled_collections.lock(),
            HashSet::from([f.segments.collection.collection_id])
        );
        f.stop().await;
    }
}
