use std::sync::Arc;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, s3_client_for_test_with_new_bucket, PutOptions,
};

use wal3::{GarbageCollectionOptions, LogWriter, LogWriterOptions};

#[tokio::test]
async fn test_k8s_integration_89_hung_gc() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    storage
        .put_file(
            "test_k8s_integration_89_hung_gc/gc/GARBAGE",
            "tests/test_k8s_integration_89_hung_gc/GARBAGE",
            PutOptions::if_not_exists(StorageRequestPriority::P0),
        )
        .await
        .unwrap();
    storage
        .put_file(
            "test_k8s_integration_89_hung_gc/cursor/stable_prefix.json",
            "tests/test_k8s_integration_89_hung_gc/stable_prefix.json",
            PutOptions::if_not_exists(StorageRequestPriority::P0),
        )
        .await
        .unwrap();
    storage
        .put_file(
            "test_k8s_integration_89_hung_gc/manifest/MANIFEST",
            "tests/test_k8s_integration_89_hung_gc/MANIFEST",
            PutOptions::if_not_exists(StorageRequestPriority::P0),
        )
        .await
        .unwrap();

    let writer = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_89_hung_gc",
        "tester",
        (),
    )
    .await
    .unwrap();
    // NOTE: The hardcoded garbage file is broken by itself
    // writer
    //     .garbage_collect(&GarbageCollectionOptions::default(), None)
    //     .await
    //     .unwrap();
}
