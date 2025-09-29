use chrono::{Duration, DurationRound, TimeDelta, Utc};
use s3heap::{HeapReader, HeapWriter, Limits};

mod common;

use common::{
    create_test_triggerable, setup_test_environment, test_nonce, test_time_at_minute_offset,
    verify_bucket_count,
};

#[tokio::test]
async fn test_k8s_integration_03_merge_same_bucket() {
    let prefix = "test_k8s_integration_03_merge_same_bucket";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items that will go to the same bucket (same minute)
    let item1 = create_test_triggerable(1, "task1");
    let item2 = create_test_triggerable(2, "task2");
    let item3 = create_test_triggerable(3, "task3");

    // Schedule all items in the same minute (but different seconds)
    let now = Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap();
    let base_time = test_time_at_minute_offset(now, 5);
    scheduler.set_next_time(&item1, Some((base_time, test_nonce(1))));
    scheduler.set_next_time(
        &item2,
        Some((base_time + Duration::seconds(10), test_nonce(2))),
    );
    scheduler.set_next_time(
        &item3,
        Some((base_time + Duration::seconds(30), test_nonce(3))),
    );

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone()])
        .await
        .unwrap();

    // Verify only one bucket was created (items in same minute)
    verify_bucket_count(
        &storage,
        prefix,
        1,
        "Items in same minute should create only 1 bucket",
    )
    .await;

    // Verify all items are in the heap
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 3, "Should read all 3 items from single bucket");
}

#[tokio::test]
async fn test_k8s_integration_03_merge_multiple_pushes() {
    let prefix = "test_k8s_integration_03_merge_multiple_pushes";
    let (storage, scheduler) = setup_test_environment().await;

    // Create writer
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());

    // First push - 2 items to same bucket
    let item1 = create_test_triggerable(1, "task1");
    let item2 = create_test_triggerable(2, "task2");
    let now = Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap();
    let push_time = test_time_at_minute_offset(now, 10);

    scheduler.set_next_time(&item1, Some((push_time, test_nonce(1))));
    scheduler.set_next_time(
        &item2,
        Some((push_time + Duration::seconds(5), test_nonce(2))),
    );

    writer.push(&[item1.clone(), item2.clone()]).await.unwrap();

    // Second push - 2 more items to same bucket
    let item3 = create_test_triggerable(3, "task3");
    let item4 = create_test_triggerable(4, "task4");

    scheduler.set_next_time(
        &item3,
        Some((push_time + Duration::seconds(20), test_nonce(3))),
    );
    scheduler.set_next_time(
        &item4,
        Some((push_time + Duration::seconds(40), test_nonce(4))),
    );

    writer.push(&[item3.clone(), item4.clone()]).await.unwrap();

    // Verify still only one bucket
    verify_bucket_count(
        &storage,
        prefix,
        1,
        "Multiple pushes to same minute should still have 1 bucket",
    )
    .await;

    // Verify all 4 items are in the heap
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 4, "Should have all 4 items after merging");

    // Verify all items are present
    let uuids: Vec<_> = items.iter().map(|i| i.trigger.uuid).collect();
    assert!(uuids.contains(&item1.uuid));
    assert!(uuids.contains(&item2.uuid));
    assert!(uuids.contains(&item3.uuid));
    assert!(uuids.contains(&item4.uuid));
}
