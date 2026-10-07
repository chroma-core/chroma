use super::*;
use chroma_log::{
    local_compaction_manager::LocalCompactionManagerConfig, sqlite_log::SqliteLog, Log,
};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sysdb::{test_sysdb::TestSysDb, SysDb};
use chroma_system::System;
use chroma_types::{
    Chunk, Collection, CollectionUuid, KnnIndex, LogRecord, Operation, OperationRecord, Schema,
    Segment, SegmentScope, SegmentUuid,
};
use std::sync::Arc;
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
    metrics: MetricCapture,
}

impl Fixture {
    async fn new() -> Self {
        let registry = Registry::new();
        let metrics = MetricCapture::new(&registry);
        let db = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
        registry.register(db.clone());
        let path = tempfile::tempdir().unwrap();
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
        // The segment manager uses SQLite even with TestSysDb. Register the
        // segments before creating index files so cleanup sees a live checkpoint.
        for segment in [&metadata_segment, &vector_segment] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment.id.to_string())
                .bind(String::from(segment.r#type))
                .bind(String::from(segment.scope.clone()))
                .bind(collection.collection_id.to_string())
                .execute(db.get_conn())
                .await
                .unwrap();
        }
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": path.path().to_str().unwrap()
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
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
            metrics,
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
async fn backfill_and_purge_error_matrix_logs_failures_and_retries() {
    for (backfill_fails, purge_fails) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let mut f = Fixture::new().await;
        if backfill_fails {
            let mut collection = f.segments.collection.clone();
            collection.dimension = Some(4);
            f.sysdb.add_collection(collection);
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
            let error = result.err().unwrap();
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
            let error = result.err().unwrap();
            assert!(error.to_string().contains("Error purging logs"), "{error}");
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
        assert_eq!(
            f.offsets().await,
            if purge_fails {
                vec![1, 2, 3, 4]
            } else {
                vec![3, 4]
            }
        );

        assert_eq!(f.metrics.counts(), (1, 1));

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
        assert_eq!(f.offsets().await, vec![3, 4]);
        assert_eq!(f.metrics.counts(), (2, if purge_fails { 2 } else { 1 }));

        // Even after success, a cloned executor must attempt replay again.
        f.sysdb
            .remove_collection(f.segments.collection.collection_id);
        let mut clone = f.executor.clone();
        assert!(clone.try_backfill_collection(&f.segments).await.is_err());
        // Missing collections must also fail replay.
        let mut other = f.segments.clone();
        other.collection.collection_id = CollectionUuid::new();
        assert!(clone.try_backfill_collection(&other).await.is_err());
        f.stop().await;
    }
}

#[tokio::test]
async fn stopped_compactor_reports_request_failures() {
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
        "{:?}",
        result.err()
    );
    assert_eq!(f.offsets().await, vec![1, 2, 3, 4]);
    let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("Failed to purge logs after backfill attempt"),
        "{output}"
    );
    f.stop().await;
}

#[tokio::test]
async fn corrupt_checkpoint_blocks_purge_until_repaired() {
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
            matches!(
                result,
                Err(ExecutorError::BackfillError(_) | ExecutorError::Internal(_))
            ),
            "{:?}",
            result.err()
        );
        assert_eq!(f.offsets().await, vec![1, 2, 3, 4]);
        std::fs::write(&header, saved).unwrap();
        f.segments.collection.dimension = None;
        f.sysdb.add_collection(f.segments.collection.clone());
        f.executor
            .try_backfill_collection(&f.segments)
            .await
            .unwrap();
        assert_eq!(f.offsets().await, vec![3, 4]);
        f.stop().await;
    }
}

// Each fixture owns its provider: parallel tests never replace global telemetry.
#[derive(Clone, Debug)]
struct SharedMetricReader(Arc<opentelemetry_sdk::metrics::ManualReader>);

impl opentelemetry_sdk::metrics::reader::MetricReader for SharedMetricReader {
    fn register_pipeline(&self, pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>) {
        self.0.register_pipeline(pipeline);
    }
    fn collect(
        &self,
        metrics: &mut opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::metrics::MetricResult<()> {
        self.0.collect(metrics)
    }
    fn force_flush(&self) -> opentelemetry_sdk::metrics::MetricResult<()> {
        self.0.force_flush()
    }
    fn shutdown(&self) -> opentelemetry_sdk::metrics::MetricResult<()> {
        self.0.shutdown()
    }
    fn temporality(
        &self,
        kind: opentelemetry_sdk::metrics::InstrumentKind,
    ) -> opentelemetry_sdk::metrics::Temporality {
        self.0.temporality(kind)
    }
}

struct MetricCapture {
    _provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    reader: SharedMetricReader,
}

impl MetricCapture {
    fn new(registry: &Registry) -> Self {
        use opentelemetry::metrics::MeterProvider;
        let reader =
            SharedMetricReader(Arc::new(opentelemetry_sdk::metrics::ManualReader::default()));
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        registry.register(
            chroma_log::local_compaction_manager::LocalCompactionMetrics::new(
                &provider.meter("local-recovery-test"),
            ),
        );
        Self {
            _provider: provider,
            reader,
        }
    }

    fn counts(&self) -> (u64, u64) {
        use opentelemetry_sdk::metrics::{
            data::{ResourceMetrics, Sum},
            reader::MetricReader,
        };
        let mut exported = ResourceMetrics {
            resource: opentelemetry_sdk::Resource::empty(),
            scope_metrics: Vec::new(),
        };
        self.reader.collect(&mut exported).unwrap();
        let count = |name: &str| {
            exported
                .scope_metrics
                .iter()
                .flat_map(|scope| &scope.metrics)
                .filter(|metric| metric.name == name)
                .map(|metric| {
                    metric
                        .data
                        .as_any()
                        .downcast_ref::<Sum<u64>>()
                        .unwrap()
                        .data_points
                        .iter()
                        .map(|point| point.value)
                        .sum::<u64>()
                })
                .sum()
        };
        (
            count("local_log_replay_attempts"),
            count("local_purge_checkpoint_inspections"),
        )
    }
}

#[tokio::test]
async fn repeated_reads_skip_replay_and_inspection_but_eviction_replays_tail() {
    use chroma_types::operator::{KnnBatch, KnnProjection, Scan};
    let mut f = Fixture::new().await;
    f.segments.collection.dimension = Some(3);
    f.sysdb.add_collection(f.segments.collection.clone());
    sqlx::query("DELETE FROM embeddings_queue WHERE seq_id = 4")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    let mut log = Log::Sqlite(SqliteLog::new(
        f.db.clone(),
        "default".into(),
        "default".into(),
    ));
    log.push_logs(
        &f.segments.collection.tenant,
        chroma_types::DatabaseName::new(f.segments.collection.database.clone()).unwrap(),
        f.segments.collection.collection_id,
        vec![OperationRecord {
            id: "tail".into(),
            embedding: Some(vec![4.0; 3]),
            encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
            document: None,
            metadata: None,
            operation: Operation::Add,
        }],
        None,
        None,
    )
    .await
    .unwrap();
    let scan = Scan {
        collection_and_segments: f.segments.clone(),
        shard_index: 0,
        num_shards: 1,
        log_upper_bound_offset: 0,
    };
    let get = Get {
        scan: scan.clone(),
        filter: Filter {
            query_ids: None,
            where_clause: None,
        },
        limit: Limit {
            offset: 0,
            limit: None,
        },
        proj: Projection {
            embedding: true,
            document: false,
            metadata: false,
        },
    };
    let knn = Knn {
        scan,
        filter: Filter {
            query_ids: None,
            where_clause: None,
        },
        knn: KnnBatch {
            embeddings: vec![vec![4.0; 3]],
            fetch: 1,
        },
        proj: KnnProjection {
            projection: Projection {
                embedding: true,
                document: false,
                metadata: false,
            },
            distance: true,
        },
    };
    assert_eq!(f.metrics.counts(), (0, 0));
    f.executor
        .get(get.clone(), |_| async { unreachable!() })
        .await
        .unwrap();
    assert_eq!(f.metrics.counts(), (1, 1));
    for _ in 0..10 {
        let mut executor = f.executor.clone();
        let records = executor
            .get(get.clone(), |_| async { unreachable!() })
            .await
            .unwrap();
        assert_eq!(records.result.records[0].embedding, Some(vec![4.0; 3]));
        let nearest = executor
            .knn(knn.clone(), |_| async { unreachable!() })
            .await
            .unwrap();
        assert_eq!(nearest.results[0].records[0].record.id, "tail");
    }
    assert_eq!(
        f.metrics.counts(),
        (1, 1),
        "cached reads must do no replay or checkpoint inspection"
    );
    // reset evicts cache entries; the asynchronous eviction listener may briefly
    // retain the old instance, so wait until a new load actually occurs.
    f.executor.hnsw_manager.reset().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let reader = f
                .executor
                .try_backfill_collection(&f.segments)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"tail".to_string())
                    .await
                    .unwrap(),
                vec![4.0; 3]
            );
            if f.metrics.counts().0 == 2 {
                break;
            }
            drop(reader);
            f.executor.hnsw_manager.reset().await.unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("eviction must make the next loaded instance replay");
    assert_eq!(
        f.metrics.counts(),
        (2, 1),
        "reload replays the unpersisted tail without reinspecting a checkpoint with no purge work"
    );
    f.stop().await;
}
