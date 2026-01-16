use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, FragmentManagerFactory, LogReaderOptions, LogWriterOptions, Manifest,
    ManifestManagerFactory,
};

mod common;

use common::{assert_conditions, Condition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_02_initialized_init_again() {
    // Double initialization should fail and the second failure should not touch the log's content.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_02_initialized_init_again";
    let (fragment_factory, manifest_factory) = create_s3_factories(
        LogWriterOptions::default(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        "first".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let fragment_publisher = fragment_factory.make_publisher().await.unwrap();
    manifest_factory
        .init_manifest(&Manifest::new_empty("first"))
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
    assert_conditions(&fragment_publisher, &preconditions).await;
    let (_fragment_factory2, manifest_factory2) = create_s3_factories(
        LogWriterOptions::default(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        "second".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    manifest_factory2
        .init_manifest(&Manifest::new_empty("second"))
        .await
        .unwrap_err();
    // NOTE(rescrv):  This is a workaround for the fact that the storage pool will not immediately
    // remove a connection that fails to put-if-match.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    assert_conditions(&fragment_publisher, &postconditions).await;
}
