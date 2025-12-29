use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions,
    ManifestManagerFactory, S3ManifestManagerFactory, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_80_copy() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_80_copy_source";
    let writer = "load and scrub writer";
    let init_manifest_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: "init".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    init_manifest_factory
        .init_manifest(&wal3::Manifest::new_empty("init"))
        .await
        .unwrap();
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
        prefix.to_string(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log = LogWriter::open(
        options,
        Arc::clone(&storage),
        prefix,
        writer,
        fragment_factory,
        manifest_factory,
        None,
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
    let reader = LogReader::open_classic(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
    )
    .await
    .unwrap();
    let scrubbed_source = reader.scrub(wal3::Limits::default()).await.unwrap();
    let target_prefix = "test_k8s_integration_80_copy_target";
    let target_manifest_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: target_prefix.to_string(),
        writer: "copy".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    wal3::copy(
        &storage,
        &reader,
        LogPosition::default(),
        target_prefix.to_string(),
        target_manifest_factory,
    )
    .await
    .unwrap();
    // Scrub the copy.
    let copied = LogReader::open_classic(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        target_prefix.to_string(),
    )
    .await
    .unwrap();
    let scrubbed_target = copied.scrub(wal3::Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_target.calculated_setsum,
    );
}
