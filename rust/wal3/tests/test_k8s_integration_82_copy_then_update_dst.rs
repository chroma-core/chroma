use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Limits, LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest,
    SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_82_copy_then_update_dst() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_82_copy_then_update_dst_source",
        "init",
    )
    .await
    .unwrap();
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_then_update_dst_source",
        "load and scrub writer",
        (),
    )
    .await
    .unwrap();
    for i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(batch).await.unwrap();
    }
    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_then_update_dst_source".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_source = reader.scrub(Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "test_k8s_integration_82_copy_then_update_dst_target".to_string(),
    )
    .await
    .unwrap();
    // Scrub the copy.
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_then_update_dst_target".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_target = copied.scrub(Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_target.calculated_setsum,
    );
    // Append to the new log
    let log = LogWriter::open(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        "test_k8s_integration_82_copy_then_update_dst_target",
        "load and scrub writer",
        (),
    )
    .await
    .unwrap();
    log.append_many(vec![Vec::from("fresh-write".to_string())])
        .await
        .unwrap();
    // Scrub the old log.
    let scrubbed_source2 = reader.scrub(Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_source2.calculated_setsum
    );
    // Scrub the new log.
    let scrubbed_target2 = copied.scrub(Limits::default()).await.unwrap();
    assert_ne!(
        scrubbed_target.calculated_setsum,
        scrubbed_target2.calculated_setsum
    );
}
