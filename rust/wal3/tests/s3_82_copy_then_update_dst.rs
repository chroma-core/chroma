use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, FragmentManagerFactory, Limits, LogPosition, LogReader, LogReaderOptions,
    LogWriter, LogWriterOptions, Manifest, ManifestManagerFactory, S3ManifestManagerFactory,
    SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_82_copy_then_update_dst() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_82_copy_then_update_dst_source";
    let writer = "load and scrub writer";
    let init_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: "init".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
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
    let scrubbed_source = reader.scrub(Limits::default()).await.unwrap();
    let target_prefix = "test_k8s_integration_82_copy_then_update_dst_target";
    let (target_fragment_factory, target_manifest_factory) = create_s3_factories(
        LogWriterOptions::default(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        target_prefix.to_string(),
        "copy".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let target_fragment_publisher = target_fragment_factory
        .make_publisher()
        .await
        .expect("make_publisher should succeed");
    wal3::copy(
        &reader,
        LogPosition::default(),
        &target_fragment_publisher,
        target_manifest_factory,
        None,
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
    let scrubbed_target = copied.scrub(Limits::default()).await.unwrap();
    assert_eq!(
        scrubbed_source.calculated_setsum,
        scrubbed_target.calculated_setsum,
    );
    // Append to the new log
    let options2 = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory2, manifest_factory2) = create_s3_factories(
        options2.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        target_prefix.to_string(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log = LogWriter::open(
        options2,
        Arc::clone(&storage),
        target_prefix,
        writer,
        fragment_factory2,
        manifest_factory2,
        None,
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
