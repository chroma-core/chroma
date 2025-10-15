use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
use chrono::{DateTime, Duration, DurationRound, TimeDelta, Utc};
use s3heap::{HeapWriter, Schedule};

mod common;

use common::{create_test_triggerable, test_nonce, MockHeapScheduler};

#[tokio::test]
async fn test_k8s_integration_07_bucket_rounding() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_07_rounding";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test times within same minute
    let base_time =
        Utc::now().duration_round(TimeDelta::minutes(1)).unwrap() + Duration::minutes(5);

    // Items at different seconds within same minute
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);
    let item3 = create_test_triggerable(3, 3);
    let item4 = create_test_triggerable(4, 4);

    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: base_time,
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: base_time + Duration::seconds(15),
        nonce: test_nonce(2),
    };
    let schedule3 = Schedule {
        triggerable: item3,
        next_scheduled: base_time + Duration::seconds(30),
        nonce: test_nonce(3),
    };
    let schedule4 = Schedule {
        triggerable: item4,
        next_scheduled: base_time + Duration::seconds(59),
        nonce: test_nonce(4),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));
    scheduler.set_schedule(*item3.scheduling.as_uuid(), Some(schedule3.clone()));
    scheduler.set_schedule(*item4.scheduling.as_uuid(), Some(schedule4.clone()));

    // Push all items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer
        .push(&[schedule1, schedule2, schedule3, schedule4])
        .await
        .unwrap();

    // Should create only one bucket (all in same minute)
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|x| !x.ends_with("INIT"))
        .collect::<Vec<_>>();
    assert_eq!(
        buckets.len(),
        1,
        "All items in same minute should create 1 bucket"
    );
}

#[tokio::test]
async fn test_k8s_integration_07_bucket_boundaries() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_07_boundaries";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create test times at minute boundaries
    let minute1 = Utc::now().duration_round(TimeDelta::minutes(1)).unwrap() + Duration::minutes(10);
    let minute2 = minute1 + Duration::minutes(1);

    // Items right at boundary
    let item1 = create_test_triggerable(1, 1);
    let item2 = create_test_triggerable(2, 2);

    let schedule1 = Schedule {
        triggerable: item1,
        next_scheduled: minute1 + Duration::seconds(59),
        nonce: test_nonce(1),
    };
    let schedule2 = Schedule {
        triggerable: item2,
        next_scheduled: minute2,
        nonce: test_nonce(2),
    };
    scheduler.set_schedule(*item1.scheduling.as_uuid(), Some(schedule1.clone()));
    scheduler.set_schedule(*item2.scheduling.as_uuid(), Some(schedule2.clone()));

    // Push items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer.push(&[schedule1, schedule2]).await.unwrap();

    // Should create two buckets (different minutes)
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|x| !x.ends_with("INIT"))
        .collect::<Vec<_>>();
    assert_eq!(
        buckets.len(),
        2,
        "Items in different minutes should create 2 buckets"
    );
}

#[tokio::test]
async fn test_k8s_integration_07_bucket_path_format() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_07_path";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create item with known time
    let item = create_test_triggerable(1, 1);
    let scheduled_time = DateTime::parse_from_rfc3339("2024-01-15T10:30:45Z")
        .unwrap()
        .with_timezone(&Utc);

    let schedule = Schedule {
        triggerable: item,
        next_scheduled: scheduled_time,
        nonce: test_nonce(1),
    };
    scheduler.set_schedule(*item.scheduling.as_uuid(), Some(schedule.clone()));

    // Push item
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer.push(&[schedule]).await.unwrap();

    // Check bucket path format
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|x| !x.ends_with("INIT"))
        .collect::<Vec<_>>();

    assert_eq!(buckets.len(), 1);
    let bucket_path = &buckets[0];

    // The bucket should be truncated to the current minute (10:30:00)
    assert!(
        bucket_path.contains("2024-01-15T10:30:00"),
        "Bucket path should be truncated to minute boundary: {}",
        bucket_path
    );
}

#[tokio::test]
async fn test_k8s_integration_07_multiple_buckets_ordering() {
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = "test_k8s_integration_07_ordering";
    let scheduler = Arc::new(MockHeapScheduler::new());

    // Create items for different minutes
    let base_time = Utc::now().duration_round(TimeDelta::minutes(1)).unwrap();

    let schedules: Vec<_> = (0..5)
        .map(|i| {
            let item = create_test_triggerable(i as u32, i as u32);
            let time = base_time + Duration::minutes(i * 5);
            let schedule = Schedule {
                triggerable: item,
                next_scheduled: time,
                nonce: test_nonce(i as u32),
            };
            scheduler.set_schedule(*item.scheduling.as_uuid(), Some(schedule.clone()));
            schedule
        })
        .collect();

    // Push all items
    let writer = HeapWriter::new(
        storage.clone(),
        prefix.to_string().clone(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    writer.push(&schedules).await.unwrap();

    // Verify bucket count
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|x| !x.ends_with("INIT"))
        .collect::<Vec<_>>();
    assert_eq!(
        buckets.len(),
        5,
        "Should create 5 buckets for 5 different minutes"
    );

    // Buckets should be in lexicographic order (time order)
    let mut sorted_buckets = buckets.clone();
    sorted_buckets.sort();
    assert_eq!(
        buckets, sorted_buckets,
        "Buckets should be returned in time order"
    );
}
