use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use chrono::Utc;
use s3heap::{HeapReader, HeapWriter, Limits};

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_05_peek_with_filter() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_filter";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items with different task types
    let item1 = create_test_triggerable(1, "process_payment");
    let item2 = create_test_triggerable(2, "send_email");
    let item3 = create_test_triggerable(3, "process_payment");
    let item4 = create_test_triggerable(4, "generate_report");
    let item5 = create_test_triggerable(5, "send_email");

    // Schedule all items
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 5);
    scheduler.set_next_time(&item1, Some((time, test_nonce(1))));
    scheduler.set_next_time(&item2, Some((time, test_nonce(2))));
    scheduler.set_next_time(&item3, Some((time, test_nonce(3))));
    scheduler.set_next_time(&item4, Some((time, test_nonce(4))));
    scheduler.set_next_time(&item5, Some((time, test_nonce(5))));

    // Push all items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer
        .push(&[
            item1.clone(),
            item2.clone(),
            item3.clone(),
            item4.clone(),
            item5.clone(),
        ])
        .await
        .unwrap();

    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();

    // Filter for payment processing tasks
    let payment_items = reader
        .peek(|t| t.name.contains("payment"), Limits::default())
        .await
        .unwrap();
    assert_eq!(payment_items.len(), 2, "Should have 2 payment tasks");
    assert!(payment_items
        .iter()
        .all(|i| i.trigger.name.contains("payment")));

    // Filter for email tasks
    let email_items = reader
        .peek(|t| t.name.contains("email"), Limits::default())
        .await
        .unwrap();
    assert_eq!(email_items.len(), 2, "Should have 2 email tasks");
    assert!(email_items.iter().all(|i| i.trigger.name.contains("email")));

    // Filter for report tasks
    let report_items = reader
        .peek(|t| t.name.contains("report"), Limits::default())
        .await
        .unwrap();
    assert_eq!(report_items.len(), 1, "Should have 1 report task");
    assert_eq!(report_items[0].trigger.name, "generate_report");

    // Filter that matches nothing
    let no_items = reader
        .peek(|t| t.name.contains("nonexistent"), Limits::default())
        .await
        .unwrap();
    assert_eq!(
        no_items.len(),
        0,
        "Should have no matches for nonexistent filter"
    );
}

#[tokio::test]
async fn test_k8s_integration_05_peek_filters_completed() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_completed";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test items
    let item1 = create_test_triggerable(1, "task_done");
    let item2 = create_test_triggerable(2, "task_pending");
    let item3 = create_test_triggerable(3, "task_also_done");

    // Schedule all items
    let now = Utc::now();
    let time = test_time_at_minute_offset(now, 3);
    let nonce1 = test_nonce(1);
    let nonce2 = test_nonce(2);
    let nonce3 = test_nonce(3);

    scheduler.set_next_time(&item1, Some((time, nonce1)));
    scheduler.set_next_time(&item2, Some((time, nonce2)));
    scheduler.set_next_time(&item3, Some((time, nonce3)));

    // Mark some as done
    scheduler.set_done(&item1, nonce1, true);
    scheduler.set_done(&item2, nonce2, false);
    scheduler.set_done(&item3, nonce3, true);

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone()])
        .await
        .unwrap();

    // Peek should automatically filter out completed items
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(items.len(), 1, "Should only return incomplete items");
    assert_eq!(
        items[0].trigger.uuid, item2.uuid,
        "Should be the pending task"
    );

    // Even with specific filter, completed items shouldn't appear
    let done_items = reader
        .peek(|t| t.name.contains("done"), Limits::default())
        .await
        .unwrap();
    assert_eq!(
        done_items.len(),
        0,
        "Completed items should not be returned even if name matches"
    );
}

#[tokio::test]
async fn test_k8s_integration_05_peek_across_buckets() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_05_peek_buckets";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create items for different buckets
    let item1 = create_test_triggerable(1, "type_a");
    let item2 = create_test_triggerable(2, "type_b");
    let item3 = create_test_triggerable(3, "type_a");
    let item4 = create_test_triggerable(4, "type_b");

    // Schedule in different buckets
    let now = Utc::now();
    let time1 = test_time_at_minute_offset(now, 5);
    let time2 = test_time_at_minute_offset(now, 10);

    scheduler.set_next_time(&item1, Some((time1, test_nonce(1))));
    scheduler.set_next_time(&item2, Some((time1, test_nonce(2))));
    scheduler.set_next_time(&item3, Some((time2, test_nonce(3))));
    scheduler.set_next_time(&item4, Some((time2, test_nonce(4))));

    // Push items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();
    writer
        .push(&[item1.clone(), item2.clone(), item3.clone(), item4.clone()])
        .await
        .unwrap();

    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone()).unwrap();

    // Filter across buckets
    let type_a_items = reader
        .peek(|t| t.name.contains("type_a"), Limits::default())
        .await
        .unwrap();
    assert_eq!(
        type_a_items.len(),
        2,
        "Should find type_a items across buckets"
    );

    let type_b_items = reader
        .peek(|t| t.name.contains("type_b"), Limits::default())
        .await
        .unwrap();
    assert_eq!(
        type_b_items.len(),
        2,
        "Should find type_b items across buckets"
    );
}
