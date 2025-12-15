use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{FragmentPublisherFactory, LogWriter, LogWriterOptions, ManifestPublisherFactory};

mod common;

use common::{assert_conditions, Condition};

#[tokio::test]
async fn test_k8s_integration_01_empty_and_open() {
    // Opening a log that hasn't been initialized should fail.
    let preconditions = [Condition::PathNotExist("manifest/MANIFEST".to_string())];
    let postconditions = [Condition::PathNotExist("manifest/MANIFEST".to_string())];
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_01_empty_and_append";
    let writer = "test writer";
    assert_conditions(&storage, prefix, &preconditions).await;
    let options = LogWriterOptions::default();
    let fragment_factory = FragmentPublisherFactory {
        options: options.clone(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        mark_dirty: Arc::new(()),
    };
    let manifest_factory = ManifestPublisherFactory {
        options: options.clone(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: writer.to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    let _ = LogWriter::open(
        options,
        Arc::clone(&storage),
        prefix,
        writer,
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .unwrap_err();
    assert_conditions(&storage, prefix, &postconditions).await;
}
