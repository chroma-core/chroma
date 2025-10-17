use parking_lot::Mutex;
use s3heap::{
    Error, HeapPruner, HeapReader, HeapScheduler, HeapWriter, Limits, PruneStats, RetryConfig,
    Schedule, Triggerable,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Dummy scheduler for testing purposes
struct DummyScheduler;

#[async_trait::async_trait]
impl HeapScheduler for DummyScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
        Ok(vec![false; items.len()])
    }

    async fn get_schedules(&self, _ids: &[Uuid]) -> Result<Vec<Schedule>, Error> {
        Ok(vec![])
    }
}

// More sophisticated test scheduler for comprehensive testing
struct ConfigurableScheduler {
    #[allow(clippy::type_complexity)]
    done_items: Arc<Mutex<HashMap<(Uuid, Uuid, Uuid), bool>>>,
    scheduled_items: Arc<Mutex<HashMap<Uuid, Option<Schedule>>>>,
    error_on_done: Arc<Mutex<bool>>,
}

impl ConfigurableScheduler {
    fn new() -> Self {
        Self {
            done_items: Arc::new(Mutex::new(HashMap::new())),
            scheduled_items: Arc::new(Mutex::new(HashMap::new())),
            error_on_done: Arc::new(Mutex::new(false)),
        }
    }
}

#[async_trait::async_trait]
impl HeapScheduler for ConfigurableScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
        if *self.error_on_done.lock() {
            return Err(Error::Internal("Simulated error in is_done".to_string()));
        }
        let done_items = self.done_items.lock();
        Ok(items
            .iter()
            .map(|(item, nonce)| {
                done_items
                    .get(&(
                        *item.partitioning.as_uuid(),
                        *item.scheduling.as_uuid(),
                        *nonce,
                    ))
                    .copied()
                    .unwrap_or(false)
            })
            .collect())
    }

    async fn get_schedules(&self, ids: &[Uuid]) -> Result<Vec<Schedule>, Error> {
        let scheduled_items = self.scheduled_items.lock();
        Ok(ids
            .iter()
            .filter_map(|id| scheduled_items.get(id).cloned().flatten())
            .collect())
    }
}

// Tests for prefix validation
#[tokio::test]
async fn heap_components_error_on_empty_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);

    let writer_result = HeapWriter::new(storage.clone(), String::new(), scheduler.clone()).await;
    assert!(writer_result.is_err());
    match writer_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error for HeapWriter"),
    }

    let pruner_result = HeapPruner::new(storage.clone(), String::new(), scheduler.clone());
    assert!(pruner_result.is_err());
    match pruner_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error for HeapPruner"),
    }

    let reader_result = HeapReader::new(storage, String::new(), scheduler).await;
    assert!(reader_result.is_err());
    match reader_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error for HeapReader"),
    }
}

#[tokio::test]
async fn heap_components_accept_valid_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);

    // These should not error
    let _writer = HeapWriter::new(
        storage.clone(),
        "valid-prefix".to_string(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let _pruner = HeapPruner::new(
        storage.clone(),
        "valid-prefix".to_string(),
        scheduler.clone(),
    )
    .unwrap();
    let _reader = HeapReader::new(storage, "valid-prefix".to_string(), scheduler)
        .await
        .unwrap();
}

#[tokio::test]
async fn heap_components_error_on_double_slash() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);

    let writer_result = HeapWriter::new(
        storage.clone(),
        "prefix//with//slashes".to_string(),
        scheduler.clone(),
    )
    .await;
    assert!(writer_result.is_err());
    match writer_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error for HeapWriter"),
    }

    let pruner_result = HeapPruner::new(
        storage.clone(),
        "prefix//with//slashes".to_string(),
        scheduler.clone(),
    );
    assert!(pruner_result.is_err());
    match pruner_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error for HeapPruner"),
    }

    let reader_result =
        HeapReader::new(storage, "prefix//with//slashes".to_string(), scheduler).await;
    assert!(reader_result.is_err());
    match reader_result {
        Err(Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error for HeapReader"),
    }
}

// Tests for RetryConfig
#[test]
fn retry_config_default_values() {
    let config = RetryConfig::default();
    assert_eq!(config.min_delay, Duration::from_millis(100));
    assert_eq!(config.max_delay, Duration::from_secs(10));
    assert_eq!(config.factor, 2.0);
    assert_eq!(config.max_retries, 10);
}

// Tests for Limits
#[test]
fn limits_default_is_none() {
    let limits = Limits::default();
    assert_eq!(limits.buckets_to_read, None);
    assert_eq!(limits.max_items, None);
}

#[test]
fn limits_equality() {
    let limits1 = Limits {
        buckets_to_read: Some(100),
        max_items: None,
        time_cut_off: None,
    };
    let limits2 = Limits {
        buckets_to_read: Some(100),
        max_items: None,
        time_cut_off: None,
    };
    let limits3 = Limits {
        buckets_to_read: Some(200),
        max_items: None,
        time_cut_off: None,
    };
    let limits4 = Limits {
        buckets_to_read: None,
        max_items: None,
        time_cut_off: None,
    };

    assert_eq!(limits1, limits2);
    assert_ne!(limits1, limits3);
    assert_ne!(limits1, limits4);
}

#[test]
fn limits_clone() {
    let original = Limits {
        buckets_to_read: Some(500),
        max_items: None,
        time_cut_off: None,
    };
    let cloned = original.clone();
    assert_eq!(original, cloned);
    assert_eq!(cloned.buckets_to_read, Some(500));
}

// Tests for Triggerable
#[test]
fn triggerable_creation_and_equality() {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let t1 = Triggerable {
        partitioning: uuid1.into(),
        scheduling: uuid2.into(),
    };
    let t2 = Triggerable {
        partitioning: uuid1.into(),
        scheduling: uuid2.into(),
    };
    let t3 = Triggerable {
        partitioning: Uuid::new_v4().into(),
        scheduling: uuid2.into(),
    };
    let t4 = Triggerable {
        partitioning: uuid1.into(),
        scheduling: Uuid::new_v4().into(),
    };

    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
    assert_ne!(t1, t4);
}

#[test]
fn triggerable_clone() {
    let original = Triggerable {
        partitioning: Uuid::new_v4().into(),
        scheduling: Uuid::new_v4().into(),
    };
    let cloned = original;
    assert_eq!(original, cloned);
    assert_eq!(original.partitioning, cloned.partitioning);
    assert_eq!(original.scheduling, cloned.scheduling);
}

#[test]
fn triggerable_default() {
    let t = Triggerable::default();
    assert_eq!(t.partitioning.as_uuid(), &Uuid::nil());
    assert_eq!(t.scheduling.as_uuid(), &Uuid::nil());
}

// Tests for Error enum
#[test]
fn error_display() {
    // Test various error types display correctly
    let etag_error = Error::ETagConflict;
    assert_eq!(format!("{}", etag_error), "e_tag conflict");

    let missing_etag = Error::MissingETag("test-path".to_string());
    assert_eq!(format!("{}", missing_etag), "missing e_tag: test-path");

    let internal_error = Error::Internal("something went wrong".to_string());
    assert_eq!(
        format!("{}", internal_error),
        "internal error: something went wrong"
    );

    let arrow_error = Error::Arrow("column missing".to_string());
    assert_eq!(format!("{}", arrow_error), "arrow error: column missing");

    // We can't easily construct a uuid::Error, so just test the formatting with a real parse error
    let uuid_result = Uuid::from_str("invalid-uuid");
    if let Err(uuid_err) = uuid_result {
        let error = Error::Uuid(uuid_err);
        assert!(format!("{}", error).contains("uuid error"));
    }
}

// Async tests for HeapWriter
#[tokio::test]
async fn writer_push_empty_items() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    let writer = HeapWriter::new(storage, "test-prefix".to_string(), scheduler)
        .await
        .unwrap();

    // Pushing empty items should succeed without doing anything
    let result = writer.push(&[]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn writer_push_with_no_scheduled_items() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(ConfigurableScheduler::new());
    let writer = HeapWriter::new(storage, "test-no-schedule".to_string(), scheduler)
        .await
        .unwrap();

    // Push empty schedules should succeed but not create any buckets
    let result = writer.push(&[]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn writer_push_empty_schedules_succeeds() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(ConfigurableScheduler::new());

    let writer = HeapWriter::new(storage, "test-empty".to_string(), scheduler)
        .await
        .unwrap();

    let result = writer.push(&[]).await;
    assert!(result.is_ok());
}

// Async tests for HeapPruner
#[tokio::test]
async fn pruner_with_empty_heap() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    let pruner = HeapPruner::new(storage, "empty-heap".to_string(), scheduler).unwrap();

    // Pruning empty heap should succeed
    let result = pruner.prune(Limits::default()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn pruner_respects_limits() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    let pruner = HeapPruner::new(storage, "limited-prune".to_string(), scheduler).unwrap();

    let limits = Limits {
        buckets_to_read: Some(5),
        max_items: None,
        time_cut_off: None,
    };

    // Should respect the limit even if more buckets exist
    let result = pruner.prune(limits).await;
    assert!(result.is_ok());
}

// Async tests for HeapReader
#[tokio::test]
async fn reader_peek_empty_heap() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    // Initialize heap first
    let _writer = HeapWriter::new(
        storage.clone(),
        "empty-reader".to_string(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let reader = HeapReader::new(storage, "empty-reader".to_string(), scheduler)
        .await
        .unwrap();

    let items = reader.peek(|_, _| true, Limits::default()).await;
    assert!(items.is_ok());
    assert_eq!(items.unwrap().len(), 0);
}

#[tokio::test]
async fn reader_peek_with_filter() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    // Initialize heap first
    let _writer = HeapWriter::new(
        storage.clone(),
        "filtered-reader".to_string(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let reader = HeapReader::new(storage, "filtered-reader".to_string(), scheduler)
        .await
        .unwrap();

    // Filter that rejects everything
    let items = reader.peek(|_, _| false, Limits::default()).await;
    assert!(items.is_ok());
    assert_eq!(items.unwrap().len(), 0);
}

#[tokio::test]
async fn reader_respects_limits() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(DummyScheduler);
    // Initialize heap first
    let _writer = HeapWriter::new(
        storage.clone(),
        "limited-reader".to_string(),
        scheduler.clone(),
    )
    .await
    .unwrap();
    let reader = HeapReader::new(storage, "limited-reader".to_string(), scheduler)
        .await
        .unwrap();

    let limits = Limits {
        buckets_to_read: Some(3),
        max_items: None,
        time_cut_off: None,
    };

    // Should respect the bucket limit
    let items = reader.peek(|_, _| true, limits).await;
    assert!(items.is_ok());
}

#[test]
fn retry_config_with_extreme_values() {
    let config = RetryConfig {
        min_delay: Duration::from_nanos(1),
        max_delay: Duration::from_secs(3600),
        factor: 100.0,
        max_retries: 1000,
    };
    assert_eq!(config.min_delay, Duration::from_nanos(1));
    assert_eq!(config.max_delay, Duration::from_secs(3600));
    assert_eq!(config.factor, 100.0);
    assert_eq!(config.max_retries, 1000);
}

#[test]
fn retry_config_with_zero_retries() {
    let config = RetryConfig {
        min_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(1),
        factor: 2.0,
        max_retries: 0,
    };
    assert_eq!(config.max_retries, 0);
}

#[test]
fn limits_with_max_value() {
    let limits = Limits {
        buckets_to_read: Some(usize::MAX),
        max_items: None,
        time_cut_off: None,
    };
    assert_eq!(limits.buckets_to_read, Some(usize::MAX));
}

#[test]
fn limits_builder_methods() {
    let limits = Limits::default().with_buckets(100).with_items(50);
    assert_eq!(limits.buckets_to_read, Some(100));
    assert_eq!(limits.max_items, Some(50));
    assert_eq!(limits.max_buckets(), 100);
}

#[test]
fn limits_max_buckets_returns_default_when_none() {
    let limits = Limits::default();
    assert_eq!(limits.max_buckets(), 1000); // Default value

    let limits_with_value = Limits::default().with_buckets(42);
    assert_eq!(limits_with_value.max_buckets(), 42);
}

// Tests for PruneStats
#[test]
fn prune_stats_default_values() {
    let stats = PruneStats::default();
    assert_eq!(stats.items_pruned, 0);
    assert_eq!(stats.items_retained, 0);
    assert_eq!(stats.buckets_deleted, 0);
    assert_eq!(stats.buckets_updated, 0);
}

#[test]
fn prune_stats_equality() {
    let stats1 = PruneStats {
        items_pruned: 10,
        items_retained: 5,
        buckets_deleted: 2,
        buckets_updated: 3,
    };
    let stats2 = PruneStats {
        items_pruned: 10,
        items_retained: 5,
        buckets_deleted: 2,
        buckets_updated: 3,
    };
    let stats3 = PruneStats {
        items_pruned: 10,
        items_retained: 5,
        buckets_deleted: 2,
        buckets_updated: 4, // Different
    };

    assert_eq!(stats1, stats2);
    assert_ne!(stats1, stats3);
}

#[test]
fn prune_stats_merge() {
    let mut stats1 = PruneStats {
        items_pruned: 10,
        items_retained: 5,
        buckets_deleted: 2,
        buckets_updated: 3,
    };
    let stats2 = PruneStats {
        items_pruned: 7,
        items_retained: 3,
        buckets_deleted: 1,
        buckets_updated: 2,
    };

    let result = stats1.merge(&stats2);

    // Should return mutable reference for chaining
    assert_eq!(result.items_pruned, 17);
    assert_eq!(result.items_retained, 8);
    assert_eq!(result.buckets_deleted, 3);
    assert_eq!(result.buckets_updated, 5);

    // Original should be modified
    assert_eq!(stats1.items_pruned, 17);
}

#[test]
fn prune_stats_merge_chaining() {
    let mut total = PruneStats::default();
    let stats1 = PruneStats {
        items_pruned: 5,
        items_retained: 2,
        buckets_deleted: 1,
        buckets_updated: 1,
    };
    let stats2 = PruneStats {
        items_pruned: 3,
        items_retained: 1,
        buckets_deleted: 0,
        buckets_updated: 2,
    };

    // Test method chaining
    total.merge(&stats1).merge(&stats2);

    assert_eq!(total.items_pruned, 8);
    assert_eq!(total.items_retained, 3);
    assert_eq!(total.buckets_deleted, 1);
    assert_eq!(total.buckets_updated, 3);
}

#[test]
fn prune_stats_display() {
    let stats = PruneStats {
        items_pruned: 42,
        items_retained: 13,
        buckets_deleted: 5,
        buckets_updated: 7,
    };

    let display_str = format!("{}", stats);
    assert_eq!(
        display_str,
        "PruneStats { pruned: 42, retained: 13, buckets_deleted: 5, buckets_updated: 7 }"
    );
}

#[test]
fn prune_stats_clone() {
    let original = PruneStats {
        items_pruned: 100,
        items_retained: 50,
        buckets_deleted: 10,
        buckets_updated: 20,
    };
    let cloned = original.clone();

    assert_eq!(original, cloned);
    assert_eq!(original.items_pruned, cloned.items_pruned);
    assert_eq!(original.items_retained, cloned.items_retained);
    assert_eq!(original.buckets_deleted, cloned.buckets_deleted);
    assert_eq!(original.buckets_updated, cloned.buckets_updated);
}
