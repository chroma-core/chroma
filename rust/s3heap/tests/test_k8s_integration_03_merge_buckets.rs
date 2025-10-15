use chrono::{Duration, DurationRound, TimeDelta, Utc};
use s3heap::{HeapReader, HeapWriter, Limits, Schedule};

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
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);

    // Schedule all items in the same minute (but different seconds)
    let now = Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap();
    let base_time = test_time_at_minute_offset(now, 5);
    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: base_time,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: base_time + Duration::seconds(10),
        nonce: test_nonce(2),
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: base_time + Duration::seconds(30),
        nonce: test_nonce(3),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));

    // Push items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[schedule1.clone(), schedule2.clone(), schedule3.clone()])
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
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 3, "Should read all 3 items from single bucket");
}

#[tokio::test]
async fn test_k8s_integration_03_merge_multiple_pushes() {
    let prefix = "test_k8s_integration_03_merge_multiple_pushes";
    let (storage, scheduler) = setup_test_environment().await;

    // Create writer
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // First push - 2 items to same bucket
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let now = Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap();
    let push_time = test_time_at_minute_offset(now, 10);

    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: push_time,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: push_time + Duration::seconds(5),
        nonce: test_nonce(2),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));

    writer
        .push(&[schedule1.clone(), schedule2.clone()])
        .await
        .unwrap();

    // Second push - 2 more items to same bucket
    let item3 = create_test_triggerable(3, 3);
    let item4 = create_test_triggerable(4, 4);

    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: push_time + Duration::seconds(20),
        nonce: test_nonce(3),
    };
    let schedule4 = Schedule {
        triggerable: item4,
        next_scheduled: push_time + Duration::seconds(40),
        nonce: test_nonce(4),
    };
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));
    scheduler.set_schedule(*item4.scheduling.as_uuid(), Some(schedule4.clone()));

    writer
        .push(&[schedule3.clone(), schedule4.clone()])
        .await
        .unwrap();

    // Verify still only one bucket
    verify_bucket_count(
        &storage,
        prefix,
        1,
        "Multiple pushes to same minute should still have 1 bucket",
    )
    .await;

    // Verify all 4 items are in the heap
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 4, "Should have all 4 items after merging");

    // Verify all items are present
    let uuids: Vec<_> = items
        .iter()
        .map(|(_bucket, item)| *item.trigger.scheduling.as_uuid())
        .collect();
    assert!(uuids.contains(item1.scheduling.as_uuid()));
    assert!(uuids.contains(item2.scheduling.as_uuid()));
    assert!(uuids.contains(item3.scheduling.as_uuid()));
    assert!(uuids.contains(item4.scheduling.as_uuid()));
}
