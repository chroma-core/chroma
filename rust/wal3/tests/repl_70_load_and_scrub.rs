use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, FragmentManagerFactory, LogReader, LogReaderOptions, LogWriter,
    LogWriterOptions, Manifest, ManifestManagerFactory, ReplicatedManifestManagerFactory,
    SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_70_load_and_scrub() {
    // Test that we can load and scrub a log after appending many records.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_70_load_and_scrub/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    // Initialize the manifest.
    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    // Open the log writer with snapshot options.
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
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log = LogWriter::open(
        options,
        "load and scrub writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    // Append many records.
    for i in 0..100 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    // Open a reader and scrub using repl factories.
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let (fragment_factory, manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
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
    .expect("LogReader::open should succeed");

    let scrub_result = reader.scrub(wal3::Limits::default()).await;
    println!("repl_70_load_and_scrub: scrub result = {:?}", scrub_result);

    println!("repl_70_load_and_scrub: passed, log_id={}", log_id);
}
