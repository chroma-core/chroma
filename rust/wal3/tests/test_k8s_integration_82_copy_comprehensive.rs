use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, CursorStoreOptions, LogPosition, LogReader, LogReaderOptions, LogWriter,
    LogWriterOptions, Manifest, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_copy_with_deep_snapshots() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_with_deep_snapshots_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 3,
                fragment_rollover_threshold: 3,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "copy_with_deep_snapshots_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..200 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("snapshot-test:i={},j={}", i, j)));
        }
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_with_deep_snapshots_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    println!(
        "Source manifest has {} snapshots and {} fragments",
        manifest_before.snapshots.len(),
        manifest_before.fragments.len()
    );
    assert!(
        !manifest_before.snapshots.is_empty(),
        "Expected snapshots to be created"
    );
    let scrubbed_source = reader.scrub(wal3::Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_with_deep_snapshots_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_with_deep_snapshots_target".to_string(),
    )
    .await
    .unwrap();
    // TODO(claude): use Limits::UNLIMITED everywhere you scrub.
    let scrubbed_target = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_target.calculated_setsum,
        "Setsum should match after copy with snapshots"
    );
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest_before.acc_bytes, manifest_after.acc_bytes,
        "Accumulated bytes should match"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_at_specific_offset() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_at_specific_offset_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 5,
                fragment_rollover_threshold: 5,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "copy_at_specific_offset_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    let mut offset_at_50 = LogPosition::default();
    for i in 0..100 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("offset-test:i={},j={}", i, j)));
        }
        let pos = log.append_many(batch).await.unwrap();
        if i == 49 {
            offset_at_50 = pos + 10u64;
        }
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_at_specific_offset_source".to_string(),
    )
    .await
    .unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        offset_at_50,
        "copy_at_specific_offset_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_at_specific_offset_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_source = reader.manifest().await.unwrap().unwrap();
    let manifest_target = copied.manifest().await.unwrap().unwrap();
    assert!(
        manifest_target.acc_bytes < manifest_source.acc_bytes,
        "Target should have fewer bytes since it started at offset {}",
        offset_at_50.offset()
    );
    assert_eq!(
        manifest_target.initial_offset,
        Some(offset_at_50),
        "Initial offset should be set to copy offset"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_verifies_manifest_consistency() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_verifies_manifest_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "copy_verifies_manifest_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..50 {
        let batch = vec![Vec::from(format!("consistency:i={}", i))];
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_verifies_manifest_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_verifies_manifest_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_verifies_manifest_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest_before.setsum, manifest_after.setsum,
        "Setsum should be preserved"
    );
    assert_eq!(
        manifest_before.acc_bytes, manifest_after.acc_bytes,
        "Accumulated bytes should match"
    );
    assert_eq!(
        manifest_before.fragments.len(),
        manifest_after.fragments.len(),
        "Fragment count should match"
    );
    for (src_frag, dst_frag) in manifest_before
        .fragments
        .iter()
        .zip(manifest_after.fragments.iter())
    {
        assert_eq!(
            src_frag.start, dst_frag.start,
            "Fragment start positions should match"
        );
        assert_eq!(
            src_frag.limit, dst_frag.limit,
            "Fragment limit positions should match"
        );
        assert_eq!(
            src_frag.num_bytes, dst_frag.num_bytes,
            "Fragment byte counts should match"
        );
        assert_eq!(
            src_frag.setsum, dst_frag.setsum,
            "Fragment setsums should match"
        );
    }
}

#[tokio::test]
async fn test_k8s_integration_copy_empty_with_advanced_manifest() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let log = LogWriter::open_or_initialize(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "copy_empty_advanced_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    let mut position = LogPosition::default();
    for i in 0..50 {
        let batch = vec![Vec::from(format!("advanced:i={}", i))];
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
    log.garbage_collect(&wal3::GarbageCollectionOptions::default(), None)
        .await
        .unwrap();
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_empty_advanced_source".to_string(),
    )
    .await
    .unwrap();
    let manifest_before = reader.manifest().await.unwrap().unwrap();
    let copy_offset = manifest_before.next_write_timestamp();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        copy_offset,
        "copy_empty_advanced_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_empty_advanced_target".to_string(),
    )
    .await
    .unwrap();
    let manifest_after = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest_after.fragments.len(),
        0,
        "Target should have no fragments when copying from end"
    );
    assert_eq!(
        manifest_after.initial_offset,
        Some(manifest_before.next_write_timestamp()),
        "Initial offset should match source next_write_timestamp"
    );
    assert_eq!(
        manifest_after.next_fragment_seq_no(),
        manifest_before.next_fragment_seq_no(),
        "Next fragment seq no should match"
    );
}

#[tokio::test]
async fn test_k8s_integration_copy_with_large_fragments() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "copy_large_fragments_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 10,
                fragment_rollover_threshold: 10,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "copy_large_fragments_source",
        "writer",
        (),
    )
    .await
    .unwrap();
    for _i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for _j in 0..100 {
            batch.push(vec![0u8; 1024]);
        }
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_large_fragments_source".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_source = reader.scrub(wal3::Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "copy_large_fragments_target".to_string(),
    )
    .await
    .unwrap();
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "copy_large_fragments_target".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_target = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_target.calculated_setsum,
        "Large fragment copy should preserve data integrity"
    );
}
