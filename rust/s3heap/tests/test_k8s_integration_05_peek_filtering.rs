use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use chrono::Utc;
use s3heap::{HeapReader, HeapWriter, Limits, Schedule};

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_05_peek_all_items() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_all";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items with different task types
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);
    let item4 = create_test_triggerable(4, 4);
    let item5 = create_test_triggerable(5, 5);

    // Schedule all items
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 5);
    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: time,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: time,
        nonce: test_nonce(2),
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: time,
        nonce: test_nonce(3),
    };
    let schedule4 = Schedule {
        triggerable: item4,
        next_scheduled: time,
        nonce: test_nonce(4),
    };
    let schedule5 = Schedule {
        triggerable: item5,
        next_scheduled: time,
        nonce: test_nonce(5),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));
    scheduler.set_schedule(*item4.scheduling.as_uuid(), Some(schedule4.clone()));
    scheduler.set_schedule(*item5.scheduling.as_uuid(), Some(schedule5.clone()));

    // Push all items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[
            schedule1.clone(),
            schedule2.clone(),
            schedule3.clone(),
            schedule4.clone(),
            schedule5.clone(),
        ])
        .await
        .unwrap();

    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Verify all items are present
    let all_items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(all_items.len(), 5, "Should have all 5 items");
}

#[tokio::test]
async fn test_k8s_integration_05_peek_with_filter() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_filter";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items with different task types
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);
    let item4 = create_test_triggerable(4, 4);
    let item5 = create_test_triggerable(5, 5);

    // Schedule all items
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 5);
    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: time,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: time,
        nonce: test_nonce(2),
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: time,
        nonce: test_nonce(3),
    };
    let schedule4 = Schedule {
        triggerable: item4,
        next_scheduled: time,
        nonce: test_nonce(4),
    };
    let schedule5 = Schedule {
        triggerable: item5,
        next_scheduled: time,
        nonce: test_nonce(5),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));
    scheduler.set_schedule(*item4.scheduling.as_uuid(), Some(schedule4.clone()));
    scheduler.set_schedule(*item5.scheduling.as_uuid(), Some(schedule5.clone()));

    // Push all items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[
            schedule1.clone(),
            schedule2.clone(),
            schedule3.clone(),
            schedule4.clone(),
            schedule5.clone(),
        ])
        .await
        .unwrap();

    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Filter to only get items 2 and 4 by their scheduling UUIDs
    let target_uuid2 = *item2.scheduling.as_uuid();
    let target_uuid4 = *item4.scheduling.as_uuid();
    let filtered_items = reader
        .peek(
            |triggerable, _| {
                let uuid = *triggerable.scheduling.as_uuid();
                uuid == target_uuid2 || uuid == target_uuid4
            },
            Limits::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        filtered_items.len(),
        2,
        "Should have exactly 2 filtered items"
    );
    let returned_uuids: Vec<_> = filtered_items
        .iter()
        .map(|(_bucket, item)| *item.trigger.scheduling.as_uuid())
        .collect();
    assert!(
        returned_uuids.contains(&target_uuid2),
        "Should contain item2"
    );
    assert!(
        returned_uuids.contains(&target_uuid4),
        "Should contain item4"
    );
}

#[tokio::test]
async fn test_k8s_integration_05_peek_filters_completed() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_completed";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);

    // Schedule all items
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 3);
    let nonce1 = test_nonce(1);
    let nonce2 = test_nonce(2);
    let nonce3 = test_nonce(3);

    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: time,
        nonce: nonce1,
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: time,
        nonce: nonce2,
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: time,
        nonce: nonce3,
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));

    // Mark some as done
    scheduler.set_done(&item1, nonce1, true);
    scheduler.set_done(&item2, nonce2, false);
    scheduler.set_done(&item3, nonce3, true);

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

    // Peek should automatically filter out completed items
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 1, "Should only return incomplete items");
    assert_eq!(
        items[0].1.trigger.scheduling.as_uuid(),
        item2.scheduling.as_uuid(),
        "Should be the pending task"
    );
}

#[tokio::test]
async fn test_k8s_integration_05_peek_across_buckets() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_buckets";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create items for different buckets
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);
    let item4 = create_test_triggerable(4, 4);

    // Schedule in different buckets
    let now = Utc::now();
    let time1 = test_time_at_minute_offset(now, 5);
    let time2 = test_time_at_minute_offset(now, 10);

    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: time1,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: time1,
        nonce: test_nonce(2),
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: time2,
        nonce: test_nonce(3),
    };
    let schedule4 = Schedule {
        triggerable: item4,
        next_scheduled: time2,
        nonce: test_nonce(4),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));
    scheduler.set_schedule(*item4.scheduling.as_uuid(), Some(schedule4.clone()));

    // Push items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[
            schedule1.clone(),
            schedule2.clone(),
            schedule3.clone(),
            schedule4.clone(),
        ])
        .await
        .unwrap();

    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();

    // Verify all items across buckets
    let all_items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(all_items.len(), 4, "Should find all items across buckets");
}
