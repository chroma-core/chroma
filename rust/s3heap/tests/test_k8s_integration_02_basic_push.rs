use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
use chrono::Utc;
use s3heap::{HeapReader, HeapWriter, Limits};
use uuid::Uuid;

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_02_basic_push() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_02_basic_push";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items
    let item1 = create_test_triggerable(1, "task1");
    let item2 = create_test_triggerable(2, "task2");

    // Schedule items at different times
    let now = Utc::now();
    let time1 = test_time_at_minute_offset(now, 5);
    let time2 = test_time_at_minute_offset(now, 10);

    scheduler.set_next_time(&item1, Some((time1, test_nonce(1))));
    scheduler.set_next_time(&item2, Some((time2, test_nonce(2))));

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&[item1.clone(), item2.clone()]).await.unwrap();

    // Verify buckets were created
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        buckets.len(),
        2,
        "Should create 2 buckets for items at different times"
    );

    // Verify we can read the items back
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 2, "Should read 2 items back");

    // Verify items have correct data
    let uuids: Vec<Uuid> = items.iter().map(|i| i.trigger.uuid).collect();
    assert!(uuids.contains(&item1.uuid), "Should contain item1");
    assert!(uuids.contains(&item2.uuid), "Should contain item2");
}

#[tokio::test]
async fn test_k8s_integration_02_push_with_no_schedule() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_02_push_no_schedule";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items with no schedule
    let item1 = create_test_triggerable(1, "unscheduled1");
    let item2 = create_test_triggerable(2, "scheduled");
    let item3 = create_test_triggerable(3, "unscheduled2");

    // Only schedule item2
    scheduler.set_next_time(&item1, None);
    let now = Utc::now();
    scheduler.set_next_time(
        &item2,
        Some((test_time_at_minute_offset(now, 5), test_nonce(2))),
    );
    scheduler.set_next_time(&item3, None);

    // Push all items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&[item1, item2.clone(), item3]).await.unwrap();

    // Verify only one bucket was created
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        buckets.len(),
        1,
        "Should create 1 bucket for scheduled item only"
    );

    // Verify only scheduled item is in heap
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 1, "Should have only 1 scheduled item");
    assert_eq!(
        items[0].trigger.uuid, item2.uuid,
        "Should be the scheduled item"
    );
}
