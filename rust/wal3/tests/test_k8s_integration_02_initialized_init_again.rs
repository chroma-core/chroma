use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{LogWriterOptions, Manifest};

mod common;

use common::{assert_conditions, Condition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_02_initialized_init_again() {
    // Double initialization should fail and the second failure should not touch the log's content.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_02_initialized_init_again",
        "first",
    )
    .await
    .unwrap();
    let preconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "first".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    let postconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "first".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    assert_conditions(
        &storage,
        "test_k8s_integration_02_initialized_init_again",
        &preconditions,
    )
    .await;
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_02_initialized_init_again",
        "second",
    )
    .await
    .unwrap_err();
    // NOTE(rescrv):  This is a workaround for the fact that the storage pool will not immediately
    // remove a connection that fails to put-if-match.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    assert_conditions(
        &storage,
        "test_k8s_integration_02_initialized_init_again",
        &postconditions,
    )
    .await;
}
