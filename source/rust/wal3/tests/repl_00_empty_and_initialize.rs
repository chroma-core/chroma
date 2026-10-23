use uuid::Uuid;

use wal3::{Manifest, ManifestConsumer, ManifestManagerFactory, ReplicatedManifestManagerFactory};

mod common;
use common::setup_spanner_client;

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_00_empty_and_initialize() {
    // Test that a manifest does not exist and comes into existence after initialization.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let manifest_factory = ReplicatedManifestManagerFactory::new(
        client,
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );

    // Initialize the manifest.
    manifest_factory
        .init_manifest(&Manifest::new_empty("test"))
        .await
        .expect("init_manifest should succeed");

    // Verify the manifest exists by opening a consumer.
    let consumer = manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");

    let loaded = consumer
        .manifest_load()
        .await
        .expect("manifest_load should succeed");

    assert!(loaded.is_some(), "manifest should exist after init");
    let (manifest, _witness) = loaded.unwrap();
    assert_eq!(manifest.acc_bytes, 0);
    assert!(manifest.fragments.is_empty());
    assert!(manifest.snapshots.is_empty());

    println!("repl_00_empty_and_initialize: passed, log_id={}", log_id);
}
