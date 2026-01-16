use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{create_repl_factories, LogWriter, LogWriterOptions, StorageWrapper};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_01_empty_and_open() {
    // Opening a log that hasn't been initialized should fail.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_01_empty_and_open/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        client,
        vec!["dummy".to_string()],
        log_id,
    );

    // Opening a log that hasn't been initialized should fail.
    let result = LogWriter::open(
        options,
        "test writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "opening an uninitialized log should fail, but got: {:?}",
        result
    );

    println!("repl_01_empty_and_open: passed, log_id={}", log_id);
}
