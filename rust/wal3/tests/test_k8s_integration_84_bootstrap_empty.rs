use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Limits, LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_84_bootstrap_empty() {
    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_84_bootstrap_empty";
    const WRITER: &str = "test_k8s_integration_84_bootstrap writer";
    let mark_dirty = ();
    let first_record_offset_position = LogPosition::from_offset(42);
    let messages = vec![];
    LogWriter::bootstrap(
        &options,
        &storage,
        PREFIX,
        WRITER,
        mark_dirty,
        first_record_offset_position,
        messages.clone(),
    )
    .await
    .unwrap();

    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_84_bootstrap_empty".to_string(),
    )
    .await
    .unwrap();

    let scan = reader
        .scan(LogPosition::from_offset(42), Limits::default())
        .await
        .unwrap();
    assert_eq!(0, scan.len());
}
