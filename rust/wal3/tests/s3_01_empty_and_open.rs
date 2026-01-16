use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, FragmentManagerFactory, LogReaderOptions, LogWriter, LogWriterOptions,
};

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
    let options = LogWriterOptions::default();
    let (fragment_factory, _manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let fragment_publisher = fragment_factory.make_publisher().await.unwrap();
    assert_conditions(&fragment_publisher, &preconditions).await;
    let (fragment_factory2, manifest_factory2) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let _ = LogWriter::open(options, writer, fragment_factory2, manifest_factory2, None)
        .await
        .unwrap_err();
    assert_conditions(&fragment_publisher, &postconditions).await;
}
