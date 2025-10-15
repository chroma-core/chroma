use chrono::Utc;
use s3heap::{HeapPruner, HeapReader, HeapWriter, Limits};

mod common;

use common::{setup_test_environment, verify_bucket_count, TestItemBuilder};

#[tokio::test]
async fn test_k8s_integration_04_prune_completed_items() {
    let prefix = "test_k8s_integration_04_prune_completed";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items with done states
    let now = Utc::now();
    let schedule1 = TestItemBuilder::new(&scheduler, 1, 1)
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();
    let schedule2 = TestItemBuilder::new(&scheduler, 2, 2)
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(false)
        .build();
    let schedule3 = TestItemBuilder::new(&scheduler, 3, 3)
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();

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

    // Prune completed items
    let pruner = HeapPruner::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify only incomplete item remains
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(
        items.len(),
        1,
        "Only incomplete item should remain after pruning"
    );
    assert_eq!(
        items[0].1.trigger.scheduling.as_uuid(),
        schedule2.triggerable.scheduling.as_uuid(),
        "Should be the incomplete item"
    );
}

#[tokio::test]
async fn test_k8s_integration_04_prune_empty_bucket() {
    let prefix = "test_k8s_integration_04_prune_empty";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items all marked as done
    let now = Utc::now();
    let schedule1 = TestItemBuilder::new(&scheduler, 1, 1)
        .with_base_time(now)
        .at_minute_offset(3)
        .mark_done(true)
        .build();
    let schedule2 = TestItemBuilder::new(&scheduler, 2, 2)
        .with_base_time(now)
        .at_minute_offset(3)
        .mark_done(true)
        .build();

    // Push items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[schedule1.clone(), schedule2.clone()])
        .await
        .unwrap();

    // Verify bucket exists
    verify_bucket_count(&storage, prefix, 1, "Should have 1 bucket after push").await;

    // Prune - should clear the bucket
    let pruner = HeapPruner::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify bucket was cleared
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(
        items.len(),
        0,
        "No items should remain after pruning all completed"
    );
}

#[tokio::test]
async fn test_k8s_integration_04_prune_multiple_buckets() {
    let prefix = "test_k8s_integration_04_prune_multiple";
    let (storage, scheduler) = setup_test_environment().await;

    // Create items for different buckets
    let now = Utc::now();
    let schedule1 = TestItemBuilder::new(&scheduler, 1, 1)
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();
    let schedule2 = TestItemBuilder::new(&scheduler, 2, 2)
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(false)
        .build();
    let schedule3 = TestItemBuilder::new(&scheduler, 3, 3)
        .with_base_time(now)
        .at_minute_offset(10)
        .mark_done(true)
        .build();
    let schedule4 = TestItemBuilder::new(&scheduler, 4, 4)
        .with_base_time(now)
        .at_minute_offset(10)
        .mark_done(false)
        .build();

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
        ])
        .await
        .unwrap();

    // Prune
    let pruner = HeapPruner::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify correct items remain
    let reader = HeapReader::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 2, "Two incomplete items should remain");

    let uuids: Vec<_> = items
        .iter()
        .map(|(_bucket, item)| *item.trigger.scheduling.as_uuid())
        .collect();
    assert!(
        uuids.contains(schedule2.triggerable.scheduling.as_uuid()),
        "item2 should remain"
    );
    assert!(
        uuids.contains(schedule4.triggerable.scheduling.as_uuid()),
        "item4 should remain"
    );
}
