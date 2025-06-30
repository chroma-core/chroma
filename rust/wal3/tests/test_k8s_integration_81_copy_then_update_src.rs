use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest,
    SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_81_copy_then_update_src() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_80_copy_source",
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
        "test_k8s_integration_80_copy_source",
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
        "test_k8s_integration_80_copy_source".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_source = reader.scrub(wal3::Limits::default()).await.unwrap();
    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        "test_k8s_integration_80_copy_target".to_string(),
    )
    .await
    .unwrap();
    // Scrub the copy.
    let copied = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_80_copy_target".to_string(),
    )
    .await
    .unwrap();
    let scrubbed_target = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_target.calculated_setsum,
    );
    // Append to the old log
    log.append_many(vec![Vec::from("late-arrival".to_string())])
        .await
        .unwrap();
    // Scrub the new old log.
    let scrubbed_source2 = reader.scrub(wal3::Limits::default()).await.unwrap();
    assert_ne!(
        scrubbed_source.calculated_setsum,
        scrubbed_source2.calculated_setsum
    );
    // Scrub the new log.
    let scrubbed_target2 = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_target.calculated_setsum,
        scrubbed_target2.calculated_setsum
    );
}
