use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
use chrono::Utc;
use s3heap::{HeapPruner, HeapReader, HeapWriter, Limits};

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_04_prune_completed_items() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_04_prune_completed";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items
    let item1 = create_test_triggerable(1, "completed_task");
    let item2 = create_test_triggerable(2, "incomplete_task");
    let item3 = create_test_triggerable(3, "another_completed");

    // Schedule all items at same time
    let now = Utc::now();
    let schedule_time = test_time_at_minute_offset(now, 5);
    let nonce1 = test_nonce(1);
    let nonce2 = test_nonce(2);
    let nonce3 = test_nonce(3);

    scheduler.set_next_time(&item1, Some((schedule_time, nonce1)));
    scheduler.set_next_time(&item2, Some((schedule_time, nonce2)));
    scheduler.set_next_time(&item3, Some((schedule_time, nonce3)));

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone()])
        .await
        .unwrap();

    // Mark item1 and item3 as done
    scheduler.set_done(&item1, nonce1, true);
    scheduler.set_done(&item2, nonce2, false);
    scheduler.set_done(&item3, nonce3, true);

    // Prune completed items
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone());
    pruner.prune(Limits::default()).await.unwrap();

    // Verify only incomplete item remains
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
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
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_04_prune_empty";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items all marked as done
    let item1 = create_test_triggerable(1, "done1");
    let item2 = create_test_triggerable(2, "done2");

    let now = Utc::now();
    let schedule_time = test_time_at_minute_offset(now, 3);
    let nonce1 = test_nonce(1);
    let nonce2 = test_nonce(2);

    scheduler.set_next_time(&item1, Some((schedule_time, nonce1)));
    scheduler.set_next_time(&item2, Some((schedule_time, nonce2)));

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&[item1.clone(), item2.clone()]).await.unwrap();

    // Verify bucket exists
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1, "Should have 1 bucket after push");

    // Mark all items as done
    scheduler.set_done(&item1, nonce1, true);
    scheduler.set_done(&item2, nonce2, true);

    // Prune - should clear the bucket
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone());
    pruner.prune(Limits::default()).await.unwrap();

    // Verify bucket was cleared
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(
        items.len(),
        0,
        "No items should remain after pruning all completed"
    );
}

#[tokio::test]
async fn test_k8s_integration_04_prune_multiple_buckets() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_04_prune_multiple";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create items for different buckets
    let item1 = create_test_triggerable(1, "bucket1_done");
    let item2 = create_test_triggerable(2, "bucket1_keep");
    let item3 = create_test_triggerable(3, "bucket2_done");
    let item4 = create_test_triggerable(4, "bucket2_keep");

    // Schedule in different minutes
    let now = Utc::now();
    let time1 = test_time_at_minute_offset(now, 5);
    let time2 = test_time_at_minute_offset(now, 10);

    let nonce1 = test_nonce(1);
    let nonce2 = test_nonce(2);
    let nonce3 = test_nonce(3);
    let nonce4 = test_nonce(4);

    scheduler.set_next_time(&item1, Some((time1, nonce1)));
    scheduler.set_next_time(&item2, Some((time1, nonce2)));
    scheduler.set_next_time(&item3, Some((time2, nonce3)));
    scheduler.set_next_time(&item4, Some((time2, nonce4)));

    // Push all items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone(), item4.clone()])
        .await
        .unwrap();

    // Mark some as done
    scheduler.set_done(&item1, nonce1, true);
    scheduler.set_done(&item2, nonce2, false);
    scheduler.set_done(&item3, nonce3, true);
    scheduler.set_done(&item4, nonce4, false);

    // Prune
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone());
    pruner.prune(Limits::default()).await.unwrap();

    // Verify correct items remain
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 2, "Two incomplete items should remain");

    let uuids: Vec<_> = items.iter().map(|i| i.trigger.uuid).collect();
    assert!(uuids.contains(&item2.uuid), "item2 should remain");
    assert!(uuids.contains(&item4.uuid), "item4 should remain");
}
