use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    LogReaderOptions, LogWriterOptions, Manifest, ManifestManagerFactory, S3ManifestManagerFactory,
};

mod common;

use common::{assert_conditions, Condition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_02_initialized_init_again() {
    // Double initialization should fail and the second failure should not touch the log's content.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_02_initialized_init_again";
    let first_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: "first".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    first_factory
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
    assert_conditions(&storage, prefix, &preconditions).await;
    let second_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: "second".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    second_factory
        .init_manifest(&Manifest::new_empty("second"))
        .await
        .unwrap_err();
    // NOTE(rescrv):  This is a workaround for the fact that the storage pool will not immediately
    // remove a connection that fails to put-if-match.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    assert_conditions(&storage, prefix, &postconditions).await;
}
