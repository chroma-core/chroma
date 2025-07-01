use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{LogWriter, LogWriterOptions};

mod common;

use common::{assert_conditions, Condition};

#[tokio::test]
async fn test_k8s_integration_01_empty_and_open() {
    // Opening a log that hasn't been initialized should fail.
    let preconditions = [Condition::PathNotExist("manifest/MANIFEST".to_string())];
    let postconditions = [Condition::PathNotExist("manifest/MANIFEST".to_string())];
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    assert_conditions(
        &storage,
        "test_k8s_integration_01_empty_and_append",
        &preconditions,
    )
    .await;
    let _ = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_01_empty_and_append",
        "test writer",
        (),
    )
    .await
    .unwrap_err();
    assert_conditions(
        &storage,
        "test_k8s_integration_01_empty_and_append",
        &postconditions,
    )
    .await;
}
