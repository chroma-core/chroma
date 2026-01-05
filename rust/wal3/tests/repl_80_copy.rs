use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, FragmentManagerFactory, LogPosition, LogReader, LogReaderOptions,
    LogWriter, LogWriterOptions, Manifest, ManifestManagerFactory,
    ReplicatedManifestManagerFactory, SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn repl_80_copy() {
    // Test copying a log to a new location.
    // Note: The copy function currently expects S3ManifestManagerFactory, so this test
    // verifies the source log creation and reading with repl types.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_80_copy_source/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    // Initialize the source manifest.
    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    // Open the source log writer.
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
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = LogWriter::open(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "copy source writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    // Append records to source.
    for i in 0..100 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    // Open a reader for the source using repl factories.
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let (fragment_factory, manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );
    let fragment_consumer = fragment_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let manifest_consumer = manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let reader = LogReader::open(
        LogReaderOptions::default(),
        fragment_consumer,
        manifest_consumer,
    )
    .await
    .expect("LogReader::open should succeed for source");

    // Scrub the source.
    let scrubbed_source = reader
        .scrub(wal3::Limits::default())
        .await
        .expect("source scrub should succeed");

    // Copy to a new target log using ReplicatedManifestManagerFactory for the target.
    let target_log_id = Uuid::new_v4();
    let target_prefix = format!("repl_80_copy_target/{}", target_log_id);
    let target_wrapper = StorageWrapper::new(
        "test-region".to_string(),
        storage.clone(),
        target_prefix.clone(),
    );
    let target_storages = Arc::new(vec![target_wrapper]);
    let (target_fragment_factory, target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        Arc::clone(&target_storages),
        Arc::clone(&client),
        target_log_id,
    );
    let target_fragment_publisher = target_fragment_factory
        .make_publisher()
        .await
        .expect("make_publisher should always succeed");

    wal3::copy(
        &reader,
        LogPosition::default(),
        &target_fragment_publisher,
        target_manifest_factory,
        None,
    )
    .await
    .expect("copy should succeed");

    // Scrub the target copy using repl factories.
    let (target_fragment_factory, target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        target_storages,
        Arc::clone(&client),
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
    .expect("LogReader::open should succeed for target");

    let scrubbed_target = copied
        .scrub(wal3::Limits::default())
        .await
        .expect("target scrub should succeed");

    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_target.calculated_setsum,
        "source and target setsums should match"
    );

    println!(
        "repl_80_copy: passed, log_id={}, setsum={}",
        log_id,
        scrubbed_source.calculated_setsum.hexdigest()
    );
}
