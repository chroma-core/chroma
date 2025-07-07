use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, CursorStoreOptions, GarbageCollectionOptions, Limits, LogPosition,
    LogReader, LogReaderOptions, LogWriter, LogWriterOptions, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_82_copy_empty_log_initializes() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let log = LogWriter::open_or_initialize(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_empty_log_initializes_source",
        "load and scrub writer",
        (),
    )
    .await
    .unwrap();
    let mut position: LogPosition = LogPosition::default();
    for i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        position = log.append_many(batch).await.unwrap() + 10u64;
    }
    let cursors = log.cursors(CursorStoreOptions::default()).unwrap();
    cursors
        .init(
            &CursorName::new("writer").unwrap(),
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
        "test_k8s_integration_82_copy_empty_log_initializes_source".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_source = reader.scrub(wal3::Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "test_k8s_integration_82_copy_empty_log_initializes_target".to_string(),
    )
    .await
    .unwrap();
    // Scrub the copy.
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_empty_log_initializes_target".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_target = copied.scrub(Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_target.calculated_setsum,
    );
    let before_mani = reader.manifest().await.unwrap().unwrap();
    let after_mani = copied.manifest().await.unwrap().unwrap();
    assert_eq!(
        before_mani.oldest_timestamp(),
        before_mani.next_write_timestamp()
    );
    assert_eq!(
        before_mani.oldest_timestamp(),
        after_mani.oldest_timestamp()
    );
    assert_eq!(
        before_mani.next_write_timestamp(),
        after_mani.next_write_timestamp()
    );
    assert_eq!(
        before_mani.next_fragment_seq_no(),
        after_mani.next_fragment_seq_no()
    );
}
