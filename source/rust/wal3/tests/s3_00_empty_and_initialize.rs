use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, FragmentManagerFactory, LogReaderOptions, LogWriterOptions, Manifest,
    ManifestManagerFactory,
};

mod common;

use common::{assert_conditions, Condition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_00_empty_and_initialize() {
    // Test that a manifest does not exist and comes into existence after initialization.
    let preconditions = [Condition::PathNotExist("manifest/MANIFEST".to_string())];
    let postconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "test".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_00_empty_and_initialize";
    let (fragment_factory, manifest_factory) = create_s3_factories(
        LogWriterOptions::default(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        "test".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let fragment_publisher = fragment_factory.make_publisher().await.unwrap();
    assert_conditions(&fragment_publisher, &preconditions).await;
    manifest_factory
        .init_manifest(&Manifest::new_empty("test"))
        .await
        .unwrap();
    assert_conditions(&fragment_publisher, &postconditions).await;
}
