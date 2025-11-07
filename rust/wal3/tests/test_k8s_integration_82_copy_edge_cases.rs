use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, CursorStoreOptions, GarbageCollectionOptions, Limits, LogPosition,
    LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_copy_single_fragment() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_single_fragment_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "copy_single_fragment_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    log.append_many(vec![Vec::from("single-record")])
        .await
        .unwrap();
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_single_fragment_source".to_string(),
    )
    .await
    .unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_single_fragment_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_single_fragment_target".to_string(),
    )
    .await
    .unwrap();
    let manifest = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        1,
        "Should have exactly one fragment"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_immediately_after_initialization() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_immediate_source",
        "init",
    )
    .await
    .unwrap();
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_immediate_source".to_string(),
    )
    .await
    .unwrap();
    let source_manifest = reader.manifest().await.unwrap().unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_immediate_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_immediate_target".to_string(),
    )
    .await
    .unwrap();
    let manifest = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        0,
        "Newly initialized log should have no fragments"
    );
    assert_eq!(
        source_manifest.next_write_timestamp(),
        manifest.next_write_timestamp(),
        "Next write timestamp should be preserved"
    );
    assert_eq!(
        source_manifest.next_fragment_seq_no(),
        manifest.next_fragment_seq_no(),
        "Next fragment seq no should be preserved"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_after_garbage_collection_leaves_empty() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let log = LogWriter::open_or_initialize(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "copy_gc_empty_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    let mut position = LogPosition::default();
    for i in 0..20 {
        let batch = vec![Vec::from(format!("gc-test:i={}", i))];
        position = log.append_many(batch).await.unwrap() + 1u64;
    }
    let cursors = log.cursors(CursorStoreOptions::default()).unwrap();
    cursors
        .init(
            &CursorName::new("test_cursor").unwrap(),
            Cursor {
                position,
                epoch_us: 42,
                writer: "unit tests".to_string(),
            },
        )
        .await
        .unwrap();
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap();
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_gc_empty_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest_before.fragments.len(),
        0,
        "All fragments should be garbage collected"
    );
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_gc_empty_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_gc_empty_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest_after.fragments.len(),
        0,
        "Target should also have no fragments"
    );
    assert_eq!(
        manifest_after.initial_offset, manifest_before.initial_offset,
        "Initial offset should match"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_preserves_fragment_boundaries() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_boundaries_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 100,
                fragment_rollover_threshold: 5,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "copy_boundaries_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..10 {
        let mut batch = Vec::new();
        for j in 0..10 {
            batch.push(Vec::from(format!("boundary:i={},j={}", i, j)));
        }
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_boundaries_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    println!("Source has {} fragments", manifest_before.fragments.len());
    assert!(
        manifest_before.fragments.len() >= 2,
        "Should have multiple fragments"
    );
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_boundaries_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_boundaries_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert!(
        manifest_after.fragments.len() >= manifest_before.fragments.len(),
        "Target should have at least as many fragments (may have more if snapshots were expanded)"
    );
    let scrub_before = reader.scrub(wal3::Limits::default()).await.unwrap();
    let scrub_after = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrub_before.calculated_setsum, scrub_after.calculated_setsum,
        "Data integrity should be preserved despite fragment count differences"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_with_partial_offset_splits_correctly() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_partial_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 100,
                fragment_rollover_threshold: 10,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "copy_partial_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..30 {
        let batch = vec![Vec::from(format!("partial:i={}", i))];
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_partial_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    let mid_fragment = &manifest_before.fragments[manifest_before.fragments.len() / 2];
    let mid_offset = mid_fragment.start + (mid_fragment.limit - mid_fragment.start) / 2;
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        mid_offset,
        "copy_partial_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_partial_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert!(
        manifest_after.fragments.len() < manifest_before.fragments.len(),
        "Target should have fewer fragments when copying from middle"
    );
    assert_eq!(
        manifest_after.initial_offset,
        Some(mid_fragment.start),
        "Initial offset should be start of first included fragment"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_multiple_times_creates_independent_copies() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_multiple_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "copy_multiple_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..10 {
        log.append_many(vec![Vec::from(format!("multi:i={}", i))])
            .await
            .unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_multiple_source".to_string(),
    )
    .await
    .unwrap();
    let scrub_source = reader.scrub(Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_multiple_target1".to_string(),
    )
    .await
    .unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_multiple_target2".to_string(),
    )
    .await
    .unwrap();
    let copied1 = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_multiple_target1".to_string(),
    )
    .await
    .unwrap();
    let copied2 = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_multiple_target2".to_string(),
    )
    .await
    .unwrap();
    let scrub1 = copied1.scrub(Limits::default()).await.unwrap();
    let scrub2 = copied2.scrub(Limits::default()).await.unwrap();
    assert_eq!(
        scrub_source.calculated_setsum, scrub1.calculated_setsum,
        "First copy should match source"
    );
    assert_eq!(
        scrub_source.calculated_setsum, scrub2.calculated_setsum,
        "Second copy should match source"
    );
    assert_eq!(
        scrub1.calculated_setsum, scrub2.calculated_setsum,
        "Both copies should match each other"
    );
}
