use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use chrono::Utc;
use s3heap::{HeapPruner, HeapReader, HeapWriter, Limits};

mod common;

use common::{create_test_triggerable, test_nonce, test_time_at_minute_offset, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_08_concurrent_pushes() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_08_concurrent_push";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create multiple writers
    let num_writers = 5;
    let items_per_writer = 10;
    let now = Utc::now();
    let bucket_time = test_time_at_minute_offset(now, 5);

    // Setup items for each writer
    for i in 0..(num_writers * items_per_writer) {
        let item = create_test_triggerable(i, &format!("task_{}", i));
        scheduler.set_next_time(&item, Some((bucket_time, test_nonce(i))));
    }

    // Launch concurrent writers
    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
        let items: Vec<_> = (0..items_per_writer)
            .map(|j| {
                let idx = writer_id * items_per_writer + j;
                create_test_triggerable(idx, &format!("task_{}", idx))
            })
            .collect();

        handles.push(tokio::spawn(async move { writer.push(&items).await }));
    }

    // Wait for all writers
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // Verify all items are present
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(
        items.len(),
        (num_writers * items_per_writer) as usize,
        "All items from concurrent writers should be present"
    );
}

#[tokio::test]
async fn test_k8s_integration_08_concurrent_read_write() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_08_read_write";
    let scheduler = Arc::new(MockHeapScheduler::new());

    let now = Utc::now();
    let bucket_time = test_time_at_minute_offset(now, 3);

    // Start with some initial items
    let initial_items: Vec<_> = (0..5)
        .map(|i| {
            let item = create_test_triggerable(i, &format!("initial_{}", i));
            scheduler.set_next_time(&item, Some((bucket_time, test_nonce(i))));
            item
        })
        .collect();

    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&initial_items).await.unwrap();

    // Launch concurrent readers and writers
    let mut write_handles = vec![];
    let mut read_handles = vec![];

    // Writers adding more items
    for batch in 0..3 {
        let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
        let scheduler_clone = scheduler.clone();

        write_handles.push(tokio::spawn(async move {
            let new_items: Vec<_> = (0..5)
                .map(|i| {
                    let idx = 100 + batch * 5 + i;
                    let item = create_test_triggerable(idx, &format!("concurrent_{}", idx));
                    scheduler_clone.set_next_time(&item, Some((bucket_time, test_nonce(idx))));
                    item
                })
                .collect();
            writer.push(&new_items).await
        }));
    }

    // Readers checking items
    for _ in 0..3 {
        let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());

        read_handles.push(tokio::spawn(async move {
            let items = reader.peek(|_| true, Limits::default()).await?;
            // Items count will vary as writes complete
            assert!(items.len() >= 5, "Should have at least initial items");
            Ok::<_, s3heap::Error>(items.len())
        }));
    }

    // Wait for all operations
    for handle in write_handles {
        handle.await.unwrap().unwrap();
    }
    for handle in read_handles {
        let _ = handle.await.unwrap();
    }

    // Final check - should have all items
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let final_items = reader.peek(|_| true, Limits::default()).await.unwrap();
    assert_eq!(
        final_items.len(),
        20,
        "Should have all 20 items (5 initial + 3*5 concurrent)"
    );
}

#[tokio::test]
async fn test_k8s_integration_08_concurrent_prune_push() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_08_prune_push";
    let scheduler = Arc::new(MockHeapScheduler::new());

    let now = Utc::now();
    let bucket_time = test_time_at_minute_offset(now, 5);

    // Setup initial items (some done, some not)
    let initial_items: Vec<_> = (0..10)
        .map(|i| {
            let item = create_test_triggerable(i, &format!("item_{}", i));
            let nonce = test_nonce(i);
            scheduler.set_next_time(&item, Some((bucket_time, nonce)));
            // Mark even items as done
            if i % 2 == 0 {
                scheduler.set_done(&item, nonce, true);
            }
            item
        })
        .collect();

    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    writer.push(&initial_items).await.unwrap();

    // Launch concurrent operations
    // Pruner removing completed items
    let pruner = HeapPruner::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let prune_handle = tokio::spawn(async move { pruner.prune(Limits::default()).await });

    // Writer adding new items
    let writer = HeapWriter::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let scheduler_clone = scheduler.clone();
    let write_handle = tokio::spawn(async move {
        let new_items: Vec<_> = (100..105)
            .map(|i| {
                let item = create_test_triggerable(i, &format!("new_item_{}", i));
                scheduler_clone.set_next_time(&item, Some((bucket_time, test_nonce(i))));
                item
            })
            .collect();
        writer.push(&new_items).await
    });

    // Wait for operations
    prune_handle.await.unwrap().unwrap();
    write_handle.await.unwrap().unwrap();

    // Check final state
    let reader = HeapReader::new(prefix.to_string(), storage.clone(), scheduler.clone());
    let final_items = reader.peek(|_| true, Limits::default()).await.unwrap();

    // Should have: 5 incomplete initial items (odds) + 5 new items
    assert!(
        final_items.len() >= 5,
        "Should have at least incomplete items"
    );
    assert!(
        final_items.len() <= 10,
        "Should have at most incomplete + new items"
    );
}
