use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{LogWriterOptions, Manifest};

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
    let storage = s3_client_for_test_with_new_bucket().await;
    assert_conditions(
        &storage,
        "test_k8s_integration_00_empty_and_initialize",
        &preconditions,
    )
    .await;
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_00_empty_and_initialize",
        "test",
    )
    .await
    .unwrap();
    assert_conditions(
        &storage,
        "test_k8s_integration_00_empty_and_initialize",
        &postconditions,
    )
    .await;
}
