use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Cursor, CursorName, CursorStore, CursorStoreOptions, Error,
    GarbageCollectionOptions, LogPosition, LogReaderOptions, LogWriter, LogWriterOptions,
    SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

/// GC fails with NoSuchCursor when no cursors exist, but succeeds when only the intrinsic cursor
/// (written via update_intrinsic_cursor to Spanner) is visible through load_intrinsic_cursor.
///
/// For repl, the intrinsic cursor lives in Spanner (manifest_regions.intrinsic_cursor), while
/// regular cursors live in S3.  GC consults both.
#[tokio::test]
async fn test_k8s_mcmr_integration_repl_91_gc_with_only_intrinsic_cursor() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_91_gc_only_intrinsic/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_91_gc_only_intrinsic writer";

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );

    let log =
        LogWriter::open_or_initialize(options, writer, fragment_factory, manifest_factory, None)
            .await
            .expect("LogWriter::open_or_initialize should succeed");

    // Write some data.
    for i in 0..20 {
        let messages: Vec<Vec<u8>> = (0..10)
            .map(|j| Vec::from(format!("key:i={i},j={j}")))
            .collect();
        log.append_many(messages)
            .await
            .expect("append_many should succeed");
    }

    // GC should fail: no cursors at all.
    let res = log
        .garbage_collect(&GarbageCollectionOptions::default(), None)
        .await;
    assert!(
        matches!(res, Err(Error::NoSuchCursor(_))),
        "GC should fail without any cursor: {res:?}"
    );

    // Set the intrinsic cursor via the LogReader (writes to Spanner).
    let reader = log
        .reader(LogReaderOptions::default())
        .await
        .expect("reader should be available");
    reader
        .update_intrinsic_cursor(LogPosition::from_offset(100), 100, writer, false)
        .await
        .expect("update_intrinsic_cursor should succeed")
        .expect("first update should return Some");

    // GC should now succeed because load_intrinsic_cursor reads from Spanner.
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .expect("GC should succeed with intrinsic cursor set");

    println!(
        "repl_91_gc_with_only_intrinsic_cursor: passed, log_id={}",
        log_id
    );
}

/// When both a regular S3 cursor and the Spanner intrinsic cursor exist, GC cutoff is the min.
#[tokio::test]
async fn test_k8s_mcmr_integration_repl_91_gc_cutoff_prefers_lower_intrinsic_cursor() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_91_gc_cutoff_intrinsic_lower/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_91_gc_cutoff_intrinsic_lower writer";

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );

    let log =
        LogWriter::open_or_initialize(options, writer, fragment_factory, manifest_factory, None)
            .await
            .expect("LogWriter::open_or_initialize should succeed");

    // Write enough data.
    let mut last_position = LogPosition::default();
    for i in 0..50 {
        let messages: Vec<Vec<u8>> = (0..10)
            .map(|j| Vec::from(format!("key:i={i},j={j}")))
            .collect();
        last_position = log
            .append_many(messages)
            .await
            .expect("append_many should succeed");
    }

    // Regular S3 cursor far ahead.
    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage.clone()),
        prefix.clone(),
        writer.to_string(),
    );
    cursors
        .init(
            &CursorName::new("reader_cursor").expect("cursor name"),
            Cursor {
                position: last_position,
                epoch_us: last_position.offset(),
                writer: writer.to_string(),
            },
        )
        .await
        .expect("cursor init should succeed");

    // Intrinsic cursor at a low position via the LogReader (writes to Spanner).
    let reader = log
        .reader(LogReaderOptions::default())
        .await
        .expect("reader should be available");
    reader
        .update_intrinsic_cursor(LogPosition::from_offset(50), 50, writer, false)
        .await
        .expect("update_intrinsic_cursor should succeed")
        .expect("first update should return Some");

    // GC should succeed; the cutoff is min(reader_cursor=high, intrinsic=50) = 50.
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .expect("GC should succeed with both cursors");

    // After GC, the log should still be readable from offset 50.
    let oldest = reader.oldest_timestamp().await.expect("oldest_timestamp");
    assert!(
        oldest.offset() <= 50,
        "oldest timestamp ({}) should be at or below the intrinsic cursor (50)",
        oldest.offset()
    );

    println!(
        "repl_91_gc_cutoff_prefers_lower_intrinsic_cursor: passed, log_id={}",
        log_id
    );
}

/// Test that update_intrinsic_cursor through LogReader works end-to-end on repl (Spanner).
#[tokio::test]
async fn test_k8s_mcmr_integration_repl_91_log_reader_update_intrinsic_cursor() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_91_reader_update/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_91_reader_update writer";

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );

    let log =
        LogWriter::open_or_initialize(options, writer, fragment_factory, manifest_factory, None)
            .await
            .expect("LogWriter::open_or_initialize should succeed");

    // Write some data so the log is not empty.
    log.append_many(vec![b"hello".to_vec()])
        .await
        .expect("append should succeed");

    let reader = log
        .reader(LogReaderOptions::default())
        .await
        .expect("reader should be available");

    // First update: initializes the cursor in Spanner.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(10), 1000, writer, false)
        .await
        .expect("update should succeed")
        .expect("first update should return Some");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(10));

    // Forward update.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(20), 2000, writer, false)
        .await
        .expect("update should succeed")
        .expect("forward update should return Some");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(20));

    // Rollback guard: going backward without allow_rollback returns None.
    let result = reader
        .update_intrinsic_cursor(LogPosition::from_offset(5), 3000, writer, false)
        .await
        .expect("update should succeed");
    assert!(
        result.is_none(),
        "backward update should be blocked without allow_rollback"
    );

    // With allow_rollback: going backward succeeds.
    let witness = reader
        .update_intrinsic_cursor(LogPosition::from_offset(5), 4000, writer, true)
        .await
        .expect("update should succeed")
        .expect("backward update should succeed with allow_rollback");
    assert_eq!(witness.cursor().position, LogPosition::from_offset(5));

    println!(
        "repl_91_log_reader_update_intrinsic_cursor: passed, log_id={}",
        log_id
    );
}
