use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, LogWriter, LogWriterOptions, Manifest, ManifestConsumer,
    ManifestManagerFactory, ReplicatedManifestManagerFactory, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn repl_03_initialized_append_succeeds() {
    // Appending to an initialized log should succeed.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_03_initialized_append_succeeds/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    // Initialize the manifest.
    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    // Open the log writer.
    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = LogWriter::open(
        options,
        Arc::new(storage),
        &prefix,
        "test writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    // Append data.
    let position = log
        .append(vec![42, 43, 44, 45])
        .await
        .expect("append should succeed");

    // Verify the manifest was updated with the fragment.
    let consumer_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    let consumer = consumer_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let (manifest, _) = consumer
        .manifest_load()
        .await
        .expect("manifest_load should succeed")
        .expect("manifest should exist");

    assert_eq!(
        manifest.fragments.len(),
        1,
        "should have one fragment after append"
    );
    let fragment = &manifest.fragments[0];
    assert!(fragment.num_bytes > 0, "fragment should have bytes");
    assert!(
        fragment.seq_no.as_uuid().is_some(),
        "repl fragments use UUID identifiers"
    );

    println!(
        "repl_03_initialized_append_succeeds: passed, log_id={}, position={}, fragment_path={}",
        log_id,
        position.offset(),
        fragment.path
    );
}
