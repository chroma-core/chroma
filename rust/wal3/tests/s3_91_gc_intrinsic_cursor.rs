use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, Cursor, CursorName, CursorStore, CursorStoreOptions, Error,
    GarbageCollectionOptions, LogPosition, LogReaderOptions, LogWriter, LogWriterOptions,
    SnapshotOptions,
};

/// GC fails with NoSuchCursor when no cursors exist, but succeeds when only the intrinsic cursor
/// (compaction cursor) is present.
#[tokio::test]
async fn test_k8s_integration_91_gc_with_only_intrinsic_cursor() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_91_gc_only_intrinsic";
    const WRITER: &str = "test_k8s_integration_91_gc_only_intrinsic writer";
    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log =
        LogWriter::open_or_initialize(options, WRITER, fragment_factory, manifest_factory, None)
            .await
            .unwrap();

    // Write some data.
    for i in 0..20 {
        let messages: Vec<Vec<u8>> = (0..10)
            .map(|j| Vec::from(format!("key:i={i},j={j}")))
            .collect();
        log.append_many(messages).await.unwrap();
    }

    // GC should fail: no cursors at all.
    let res = log
        .garbage_collect(&GarbageCollectionOptions::default(), None)
        .await;
    assert!(
        matches!(res, Err(Error::NoSuchCursor(_))),
        "GC should fail without any cursor: {res:?}"
    );

    // Set the intrinsic cursor (compaction) at offset 100, which is well past the start.
    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
    );
    cursors
        .init(
            &CursorName::new("compaction").unwrap(),
            Cursor {
                position: LogPosition::from_offset(100),
                epoch_us: 100,
                writer: WRITER.to_string(),
            },
        )
        .await
        .unwrap();

    // GC should now succeed because the intrinsic cursor is present.
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap();

    println!("test_k8s_integration_91_gc_with_only_intrinsic_cursor: passed");
}

/// When both a regular cursor and an intrinsic cursor exist, GC cutoff is the min of both.
/// If the intrinsic cursor is lower, it constrains GC.
#[tokio::test]
async fn test_k8s_integration_91_gc_cutoff_prefers_lower_intrinsic_cursor() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_91_gc_cutoff_intrinsic_lower";
    const WRITER: &str = "test_k8s_integration_91_gc_cutoff_intrinsic_lower writer";
    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log =
        LogWriter::open_or_initialize(options, WRITER, fragment_factory, manifest_factory, None)
            .await
            .unwrap();

    // Write enough data to have something to GC.
    let mut last_position = LogPosition::default();
    for i in 0..50 {
        let messages: Vec<Vec<u8>> = (0..10)
            .map(|j| Vec::from(format!("key:i={i},j={j}")))
            .collect();
        last_position = log.append_many(messages).await.unwrap();
    }

    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
    );

    // Regular cursor far ahead (at the end).
    cursors
        .init(
            &CursorName::new("reader_cursor").unwrap(),
            Cursor {
                position: last_position,
                epoch_us: last_position.offset(),
                writer: WRITER.to_string(),
            },
        )
        .await
        .unwrap();

    // Intrinsic cursor at a low position.
    cursors
        .init(
            &CursorName::new("compaction").unwrap(),
            Cursor {
                position: LogPosition::from_offset(50),
                epoch_us: 50,
                writer: WRITER.to_string(),
            },
        )
        .await
        .unwrap();

    // GC should succeed. It picks the min of reader_cursor (high) and compaction (50).
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap();

    // After GC, verify the log is still readable from offset 50.
    let reader = log.reader(LogReaderOptions::default()).await.unwrap();
    let oldest = reader.oldest_timestamp().await.unwrap();
    assert!(
        oldest.offset() <= 50,
        "oldest timestamp ({}) should be at or below the intrinsic cursor (50)",
        oldest.offset()
    );

    println!("test_k8s_integration_91_gc_cutoff_prefers_lower_intrinsic_cursor: passed");
}

/// Test that update_intrinsic_cursor through LogReader works end-to-end on S3.
#[tokio::test]
async fn test_k8s_integration_91_log_reader_update_intrinsic_cursor() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_91_reader_update";
    const WRITER: &str = "test_k8s_integration_91_reader_update writer";
    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log =
        LogWriter::open_or_initialize(options, WRITER, fragment_factory, manifest_factory, None)
            .await
            .unwrap();

    // Write some data so the log is not empty.
    log.append_many(vec![b"hello".to_vec()]).await.unwrap();

    let reader = log.reader(LogReaderOptions::default()).await.unwrap();

    // First update: initializes the cursor.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(10), 1000, WRITER, false)
        .await
        .unwrap()
        .expect("first update should initialize the cursor");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(10));

    // Forward update.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(20), 2000, WRITER, false)
        .await
        .unwrap()
        .expect("forward update should succeed");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(20));

    // Rollback guard: going backward without allow_rollback returns None.
    let result = reader
        .update_intrinsic_cursor(LogPosition::from_offset(5), 3000, WRITER, false)
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "backward update should be blocked without allow_rollback"
    );

    // With allow_rollback: going backward succeeds.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(5), 4000, WRITER, true)
        .await
        .unwrap()
        .expect("backward update should succeed with allow_rollback");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(5));

    println!("test_k8s_integration_91_log_reader_update_intrinsic_cursor: passed");
}
