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
async fn repl_05_crash_safety_initialize_fails() {
    // Test that after initializing a log and appending data, re-opening the log
    // allows continued progress. This tests basic crash safety where the manifest
    // in Spanner persists across "crashes" (log reopens).
    //
    // Note: The S3 version of this test simulates a crash by uploading a fragment
    // directly without updating the manifest. For repl, the manifest is in Spanner
    // and fragments are stored separately, so we test a simpler scenario: initialize,
    // append, close, reopen, append again.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_05_crash_safety_initialize_fails/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    // Initialize the manifest.
    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    // First log writer session.
    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        Arc::clone(&storages),
        Arc::clone(&client),
        log_id,
    );

    let log = LogWriter::open(
        options.clone(),
        Arc::new(storage.clone()),
        &prefix,
        "writer1",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("first LogWriter::open should succeed");

    // Append data in first session.
    let position1 = log
        .append(vec![42, 43, 44, 45])
        .await
        .expect("first append should succeed");
    println!("repl_05: first append at position {}", position1.offset());

    // Drop the log to simulate a crash/restart.
    drop(log);

    // Verify the first fragment was persisted.
    let consumer_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    let consumer = consumer_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        1,
        "should have one fragment after first session"
    );

    // Second log writer session (after "crash").
    let storage2 = s3_client_for_test_with_new_bucket().await;
    let wrapper2 = StorageWrapper::new("test-region".to_string(), storage2.clone(), prefix.clone());
    let storages2 = Arc::new(vec![wrapper2]);

    let (fragment_factory2, manifest_factory2) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages2,
        Arc::clone(&client),
        log_id,
    );

    let log2 = LogWriter::open(
        options,
        Arc::new(storage2),
        &prefix,
        "writer2",
        fragment_factory2,
        manifest_factory2,
        None,
    )
    .await
    .expect("second LogWriter::open should succeed after crash");

    // Append data in second session.
    let position2 = log2
        .append(vec![81, 82, 83, 84])
        .await
        .expect("second append should succeed");
    println!("repl_05: second append at position {}", position2.offset());

    // Verify both fragments are persisted.
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        2,
        "should have two fragments after second session"
    );

    println!(
        "repl_05_crash_safety_initialize_fails: passed, log_id={}, positions=[{}, {}]",
        log_id,
        position1.offset(),
        position2.offset()
    );
}
