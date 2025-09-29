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
    let item1 = TestItemBuilder::new(&scheduler, 1, "completed_task")
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();
    let item2 = TestItemBuilder::new(&scheduler, 2, "incomplete_task")
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(false)
        .build();
    let item3 = TestItemBuilder::new(&scheduler, 3, "another_completed")
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone()])
        .await
        .unwrap();

    // Prune completed items
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify only incomplete item remains
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(
        items.len(),
        1,
        "Only incomplete item should remain after pruning"
    );
    assert_eq!(
        items[0].trigger.uuid, item2.uuid,
        "Should be the incomplete item"
    );
}

#[tokio::test]
async fn test_k8s_integration_04_prune_empty_bucket() {
    let prefix = "test_k8s_integration_04_prune_empty";
    let (storage, scheduler) = setup_test_environment().await;

    // Create test items all marked as done
    let now = Utc::now();
    let item1 = TestItemBuilder::new(&scheduler, 1, "done1")
        .with_base_time(now)
        .at_minute_offset(3)
        .mark_done(true)
        .build();
    let item2 = TestItemBuilder::new(&scheduler, 2, "done2")
        .with_base_time(now)
        .at_minute_offset(3)
        .mark_done(true)
        .build();

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer.push(&[item1.clone(), item2.clone()]).await.unwrap();

    // Verify bucket exists
    verify_bucket_count(&storage, prefix, 1, "Should have 1 bucket after push").await;

    // Prune - should clear the bucket
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify bucket was cleared
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
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
    let item1 = TestItemBuilder::new(&scheduler, 1, "bucket1_done")
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(true)
        .build();
    let item2 = TestItemBuilder::new(&scheduler, 2, "bucket1_keep")
        .with_base_time(now)
        .at_minute_offset(5)
        .mark_done(false)
        .build();
    let item3 = TestItemBuilder::new(&scheduler, 3, "bucket2_done")
        .with_base_time(now)
        .at_minute_offset(10)
        .mark_done(true)
        .build();
    let item4 = TestItemBuilder::new(&scheduler, 4, "bucket2_keep")
        .with_base_time(now)
        .at_minute_offset(10)
        .mark_done(false)
        .build();

    // Push all items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone(), item4.clone()])
        .await
        .unwrap();

    // Prune
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    pruner.prune(Limits::default()).await.unwrap();

    // Verify correct items remain
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 2, "Two incomplete items should remain");

    let uuids: Vec<_> = items.iter().map(|i| i.trigger.uuid).collect();
    assert!(uuids.contains(&item2.uuid), "item2 should remain");
    assert!(uuids.contains(&item4.uuid), "item4 should remain");
}
