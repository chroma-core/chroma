use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, FragmentManagerFactory, Limits, LogPosition, LogReader,
    LogReaderOptions, LogWriter, LogWriterOptions, Manifest, ManifestManagerFactory,
    ReplicatedManifestManagerFactory, SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_82_copy_then_update_dst() {
    // Test copying a log and then appending to the destination.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_82_copy_then_update_dst_source/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "load and scrub writer";

    // Initialize the source manifest.
    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["test-region".to_string()],
        "test-region".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        Arc::clone(&storages),
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );

    let log = LogWriter::open(options, writer, fragment_factory, manifest_factory, None)
        .await
        .expect("LogWriter::open should succeed");

    for i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    // Open a reader using repl factories.
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let reader_storages = Arc::new(vec![wrapper]);
    let (reader_fragment_factory, reader_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        0,
        reader_storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );
    let reader_fragment_consumer = reader_fragment_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let reader_manifest_consumer = reader_manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let reader = LogReader::open(
        LogReaderOptions::default(),
        reader_fragment_consumer,
        reader_manifest_consumer,
    )
    .await
    .expect("LogReader::open should succeed");

    let scrubbed_source = reader
        .scrub(Limits::default())
        .await
        .expect("scrub should succeed");

    // Copy to target using repl factories.
    let target_log_id = Uuid::new_v4();
    let target_prefix = format!("repl_82_copy_then_update_dst_target/{}", target_log_id);
    let target_wrapper = StorageWrapper::new(
        "test-region".to_string(),
        storage.clone(),
        target_prefix.clone(),
    );
    let copy_target_storages = Arc::new(vec![target_wrapper]);
    let (copy_target_fragment_factory, copy_target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        0,
        copy_target_storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        target_log_id,
    );
    let copy_target_fragment_publisher = copy_target_fragment_factory
        .make_publisher()
        .await
        .expect("make_publisher should succeed");

    wal3::copy(
        &reader,
        LogPosition::default(),
        &copy_target_fragment_publisher,
        copy_target_manifest_factory,
        None,
    )
    .await
    .expect("copy should succeed");

    // Scrub the copy using repl factories.
    let target_wrapper = StorageWrapper::new(
        "test-region".to_string(),
        storage.clone(),
        target_prefix.clone(),
    );
    let target_storages = Arc::new(vec![target_wrapper]);
    let (target_fragment_factory, target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        0,
        Arc::clone(&target_storages),
        Arc::clone(&client),
        vec!["test-region".to_string()],
        target_log_id,
    );
    let target_fragment_consumer = target_fragment_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let target_manifest_consumer = target_manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let copied = LogReader::open(
        LogReaderOptions::default(),
        target_fragment_consumer,
        target_manifest_consumer,
    )
    .await
    .expect("LogReader::open for target should succeed");

    let scrubbed_target = copied
        .scrub(Limits::default())
        .await
        .expect("target scrub should succeed");
    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_target.calculated_setsum,
        "source and target setsums should match after copy"
    );

    // Append to the new log using repl factories for the target.
    let options2 = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory2, manifest_factory2) = create_repl_factories(
        options2.clone(),
        default_repl_options(),
        0,
        target_storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        target_log_id,
    );

    let log2 = LogWriter::open(options2, writer, fragment_factory2, manifest_factory2, None)
        .await
        .expect("LogWriter::open for target should succeed");

    log2.append_many(vec![Vec::from("fresh-write".to_string())])
        .await
        .expect("append_many to target should succeed");

    // Scrub the old log (should be unchanged).
    let scrubbed_source2 = reader
        .scrub(Limits::default())
        .await
        .expect("second source scrub should succeed");
    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_source2.calculated_setsum,
        "source should be unchanged after writing to target"
    );

    // Scrub the new log (should be different).
    let scrubbed_target2 = copied
        .scrub(Limits::default())
        .await
        .expect("second target scrub should succeed");
    assert_ne!(
        scrubbed_target.calculated_setsum, scrubbed_target2.calculated_setsum,
        "target should have changed after new write"
    );

    println!(
        "repl_82_copy_then_update_dst: passed, log_id={}, source_setsum={}, target_setsum={}",
        log_id,
        scrubbed_source.calculated_setsum.hexdigest(),
        scrubbed_target2.calculated_setsum.hexdigest()
    );
}
