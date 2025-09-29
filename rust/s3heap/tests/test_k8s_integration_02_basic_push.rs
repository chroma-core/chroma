use chrono::Utc;
use s3heap::{HeapReader, HeapWriter, Limits};
use uuid::Uuid;

mod common;

use common::{setup_test_environment, verify_bucket_count, TestItemBuilder};

#[tokio::test]
async fn test_k8s_integration_02_basic_push() {
    let prefix = "test_k8s_integration_02_basic_push";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items using builder
    let now = Utc::now();
    let item1 = TestItemBuilder::new(&scheduler, 1, "task1")
        .with_base_time(now)
        .at_minute_offset(5)
        .build();
    let item2 = TestItemBuilder::new(&scheduler, 2, "task2")
        .with_base_time(now)
        .at_minute_offset(10)
        .build();

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&[item1.clone(), item2.clone()]).await.unwrap();

    // Verify buckets were created
    verify_bucket_count(
        &storage,
        prefix,
        2,
        "Should create 2 buckets for items at different times",
    )
    .await;

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
    let prefix = "test_k8s_integration_02_push_no_schedule";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items with no schedule
    let item1 = TestItemBuilder::new(&scheduler, 1, "unscheduled1").build_unscheduled();
    let now = Utc::now();
    let item2 = TestItemBuilder::new(&scheduler, 2, "scheduled")
        .with_base_time(now)
        .at_minute_offset(5)
        .build();
    let item3 = TestItemBuilder::new(&scheduler, 3, "unscheduled2").build_unscheduled();

    // Push all items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&[item1, item2.clone(), item3]).await.unwrap();

    // Verify only one bucket was created
    verify_bucket_count(
        &storage,
        prefix,
        1,
        "Should create 1 bucket for scheduled item only",
    )
    .await;

    // Verify only scheduled item is in heap
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 1, "Should have only 1 scheduled item");
    assert_eq!(
        items[0].trigger.uuid, item2.uuid,
        "Should be the scheduled item"
    );
}
