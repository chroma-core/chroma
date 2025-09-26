use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
use s3heap::{HeapReader, HeapWriter};

mod common;

use common::MockHeapScheduler;

#[tokio::test]
async fn test_k8s_integration_01_empty_heap() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_01_empty_heap";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create reader and verify empty heap
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());

    // Peek should return empty results
    let items = reader.peek(|_| true).await.unwrap();
    assert_eq!(items.len(), 0, "Empty heap should return no items");

    // Verify no buckets exist
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(buckets.len(), 0, "Empty heap should have no buckets");
}

#[tokio::test]
async fn test_k8s_integration_01_empty_writer() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_01_empty_writer";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create writer and push empty list
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());

    // Push empty list should succeed
    writer.push(&[]).await.unwrap();

    // Verify no buckets were created
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        buckets.len(),
        0,
        "Pushing empty list should create no buckets"
    );
}
