use std::sync::Arc;

use uuid::Uuid;

use wal3::{Manifest, ManifestConsumer, ManifestManagerFactory, ReplicatedManifestManagerFactory};

mod common;
use common::setup_spanner_client;

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_02_initialized_init_again() {
    // Double initialization should fail and the second failure should not touch the log's content.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let first_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);

    // First initialization should succeed.
    first_factory
        .init_manifest(&Manifest::new_empty("first"))
        .await
        .expect("first init should succeed");

    // Verify manifest exists with first writer's data.
    let consumer = first_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let (manifest, _) = consumer
        .manifest_load()
        .await
        .expect("manifest_load should succeed")
        .expect("manifest should exist");
    assert_eq!(manifest.acc_bytes, 0);
    assert!(manifest.fragments.is_empty());

    // Second initialization with same log_id should fail.
    let second_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    let result = second_factory
        .init_manifest(&Manifest::new_empty("second"))
        .await;
    assert!(
        result.is_err(),
        "second init should fail for duplicate log_id"
    );

    // Verify manifest still has first writer's data (unchanged).
    let consumer = first_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let (manifest, _) = consumer
        .manifest_load()
        .await
        .expect("manifest_load should succeed")
        .expect("manifest should still exist");
    assert_eq!(manifest.acc_bytes, 0);
    assert!(manifest.fragments.is_empty());

    println!("repl_02_initialized_init_again: passed, log_id={}", log_id);
}
