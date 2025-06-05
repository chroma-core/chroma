use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Limits, LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_83_bootstrap() {
    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_83_bootstrap";
    const WRITER: &str = "test_k8s_integration_83_bootstrap writer";
    let mark_dirty = ();
    let first_record_offset_position = LogPosition::from_offset(42);
    let mut messages = Vec::with_capacity(1000);
    for i in 0..100 {
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
    }
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
        "test_k8s_integration_83_bootstrap".to_string(),
    )
    .await
    .unwrap();

    let scan = reader
        .scan(LogPosition::from_offset(42), Limits::default())
        .await
        .unwrap();
    assert_eq!(1, scan.len());
    let (_, records, _) = reader.read_parquet(&scan[0]).await.unwrap();
    for (returned, expected) in std::iter::zip(records, messages) {
        assert_eq!(returned.1, expected);
    }
}
