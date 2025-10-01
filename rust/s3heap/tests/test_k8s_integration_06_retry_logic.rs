// This test is simplified since we can't easily simulate storage failures in integration tests.
// The retry logic with backon is tested implicitly through other tests that perform operations.
// For thorough retry testing, unit tests with mocked storage would be more appropriate.

use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use chrono::Utc;
use s3heap::{HeapPruner, HeapWriter, Limits};

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_06_concurrent_writes_with_retry() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_06_retry";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create multiple writers that will potentially conflict
    let writer1 = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .unwrap();
    let writer2 = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .unwrap();

    // Create items that go to same bucket
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 5);

    let item1 = create_test_triggerable(1, "writer1_task");
    let item2 = create_test_triggerable(2, "writer2_task");

    scheduler.set_next_time(&item1, Some((time, test_nonce(1))));
    scheduler.set_next_time(&item2, Some((time, test_nonce(2))));

    // Push concurrently - retry logic should handle any conflicts
    let handle1 = tokio::spawn(async move { writer1.push(&[item1]).await });

    let handle2 = tokio::spawn(async move { writer2.push(&[item2]).await });

    // Both should succeed despite potential conflicts
    handle1.await.unwrap().unwrap();
    handle2.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_k8s_integration_06_prune_with_retry() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_06_prune_retry";
    let scheduler = Arc::new(MockHeapScheduler::new());
    for _ in 0..1000 {
        // Setup data
        let item = create_test_triggerable(1, "task");
        let nonce = test_nonce(1);
        let now = Utc::now();
        scheduler.set_next_time(&item, Some((test_time_at_minute_offset(now, 3), nonce)));
        scheduler.set_done(&item, nonce, true);

        let writer = HeapWriter::new(
            storage.clone(),
            prefix.to_string().clone(),
            scheduler.clone(),
        )
        .unwrap();
        writer.push(&[item]).await.unwrap();

        // Create multiple pruners that might conflict
        let pruner1 = HeapPruner::new(
            storage.clone(),
            prefix.to_string().clone(),
            scheduler.clone(),
        )
        .unwrap();
        let pruner2 = HeapPruner::new(
            storage.clone(),
            prefix.to_string().clone(),
            scheduler.clone(),
        )
        .unwrap();

        // Prune concurrently - retry logic should handle conflicts
        let handle1 = tokio::spawn(async move { pruner1.prune(Limits::default()).await });

        let handle2 = tokio::spawn(async move { pruner2.prune(Limits::default()).await });

        // Both should succeed
        handle1.await.unwrap().unwrap();
        handle2.await.unwrap().unwrap();
    }
}
