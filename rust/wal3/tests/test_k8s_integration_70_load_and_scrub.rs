use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest, SnapshotOptions};

#[tokio::test]
async fn test_k8s_integration_70_load_and_scrub() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_70_load_and_scrub",
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
        "test_k8s_integration_70_load_and_scrub",
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
    let log = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_70_load_and_scrub".to_string(),
    )
    .await
    .unwrap();
    println!("{:?}", log.scrub(wal3::Limits::default()).await.unwrap());
}
