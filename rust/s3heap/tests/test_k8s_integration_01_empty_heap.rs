use s3heap::{HeapReader, HeapWriter, Limits};

mod common;

use common::{setup_test_environment, verify_bucket_count};

#[tokio::test]
async fn test_k8s_integration_01_empty_heap() {
    let prefix = "test_k8s_integration_01_empty_heap";
    let (storage, scheduler) = setup_test_environment().await;

    // Initialize heap with writer first
    let _writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Create reader and verify empty heap
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Peek should return empty results
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 0, "Empty heap should return no items");

    // Verify no buckets exist
    verify_bucket_count(&storage, prefix, 0, "Empty heap should have no buckets").await;
}

#[tokio::test]
async fn test_k8s_integration_01_empty_writer() {
    let prefix = "test_k8s_integration_01_empty_writer";
    let (storage, scheduler) = setup_test_environment().await;

    // Create writer and push empty list
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Push empty list should succeed
    writer.push(&[]).await.unwrap();

    // Verify no buckets were created
    verify_bucket_count(
        &storage,
        prefix,
        0,
        "Pushing empty list should create no buckets",
    )
    .await;
}
