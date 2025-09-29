use chrono::{DateTime, Utc};
use s3heap::{
    Error, HeapPruner, HeapReader, HeapScheduler, HeapWriter, Limits, PruneStats, RetryConfig,
    Triggerable,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

struct TestScheduler;

#[async_trait::async_trait]
impl HeapScheduler for TestScheduler {
    async fn is_done(&self, _item: &Triggerable, _nonce: Uuid) -> Result<bool, Error> {
        Ok(false)
    }

    async fn next_time_and_nonce(
        &self,
        _item: &Triggerable,
    ) -> Result<Option<(DateTime<Utc>, Uuid)>, Error> {
        Ok(None)
    }
}

// More sophisticated test scheduler for comprehensive testing
type ScheduleInfo = Option<(DateTime<Utc>, Uuid)>;

struct ConfigurableScheduler {
    done_items: Arc<Mutex<HashMap<(Uuid, Uuid), bool>>>,
    scheduled_items: Arc<Mutex<HashMap<Uuid, ScheduleInfo>>>,
    error_on_done: Arc<Mutex<bool>>,
    error_on_schedule: Arc<Mutex<bool>>,
}

impl ConfigurableScheduler {
    fn new() -> Self {
        Self {
            done_items: Arc::new(Mutex::new(HashMap::new())),
            scheduled_items: Arc::new(Mutex::new(HashMap::new())),
            error_on_done: Arc::new(Mutex::new(false)),
            error_on_schedule: Arc::new(Mutex::new(false)),
        }
    }

    fn set_error_on_schedule(&self, should_error: bool) {
        *self.error_on_schedule.lock().unwrap() = should_error;
    }
}

#[async_trait::async_trait]
impl HeapScheduler for ConfigurableScheduler {
    async fn is_done(&self, item: &Triggerable, nonce: Uuid) -> Result<bool, Error> {
        if *self.error_on_done.lock().unwrap() {
            return Err(Error::Internal("Simulated error in is_done".to_string()));
        }
        Ok(self
            .done_items
            .lock()
            .unwrap()
            .get(&(item.uuid, nonce))
            .copied()
            .unwrap_or(false))
    }

    async fn next_time_and_nonce(&self, item: &Triggerable) -> Result<ScheduleInfo, Error> {
        if *self.error_on_schedule.lock().unwrap() {
            return Err(Error::Internal(
                "Simulated error in next_time_and_nonce".to_string(),
            ));
        }
        Ok(self
            .scheduled_items
            .lock()
            .unwrap()
            .get(&item.uuid)
            .cloned()
            .flatten())
    }
}

// Tests for prefix validation
#[test]
fn writer_errors_on_empty_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapWriter::new(String::new(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error"),
    }
}

#[test]
fn pruner_errors_on_empty_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapPruner::new(String::new(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error"),
    }
}

#[test]
fn reader_errors_on_empty_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapReader::new(String::new(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("empty")),
        _ => panic!("Expected InvalidPrefix error"),
    }
}

#[test]
fn heap_components_accept_valid_prefix() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);

    // These should not error
    let _writer = HeapWriter::new(
        "valid-prefix".to_string(),
        storage.clone(),
        scheduler.clone(),
    )
    .unwrap();
    let _pruner = HeapPruner::new(
        "valid-prefix".to_string(),
        storage.clone(),
        scheduler.clone(),
    )
    .unwrap();
    let _reader = HeapReader::new("valid-prefix".to_string(), storage, scheduler).unwrap();
}

#[test]
fn heap_writer_errors_on_double_slash() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapWriter::new("prefix//with//slashes".to_string(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error"),
    }
}

#[test]
fn heap_pruner_errors_on_double_slash() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapPruner::new("prefix//with//slashes".to_string(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error"),
    }
}

#[test]
fn heap_reader_errors_on_double_slash() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let result = HeapReader::new("prefix//with//slashes".to_string(), storage, scheduler);
    assert!(result.is_err());
    match result {
        Err(s3heap::Error::InvalidPrefix(msg)) => assert!(msg.contains("double slashes")),
        _ => panic!("Expected InvalidPrefix error"),
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
    };
    let limits2 = Limits {
        buckets_to_read: Some(100),
        max_items: None,
    };
    let limits3 = Limits {
        buckets_to_read: Some(200),
        max_items: None,
    };
    let limits4 = Limits {
        buckets_to_read: None,
        max_items: None,
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
    };
    let cloned = original.clone();
    assert_eq!(original, cloned);
    assert_eq!(cloned.buckets_to_read, Some(500));
}

// Tests for Triggerable
#[test]
fn triggerable_creation_and_equality() {
    let uuid = Uuid::new_v4();
    let t1 = Triggerable {
        uuid,
        name: "test-task".to_string(),
    };
    let t2 = Triggerable {
        uuid,
        name: "test-task".to_string(),
    };
    let t3 = Triggerable {
        uuid: Uuid::new_v4(),
        name: "test-task".to_string(),
    };
    let t4 = Triggerable {
        uuid,
        name: "different-task".to_string(),
    };

    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
    assert_ne!(t1, t4);
}

#[test]
fn triggerable_clone() {
    let original = Triggerable {
        uuid: Uuid::new_v4(),
        name: "clone-test".to_string(),
    };
    let cloned = original.clone();
    assert_eq!(original, cloned);
    assert_eq!(original.uuid, cloned.uuid);
    assert_eq!(original.name, cloned.name);
}

#[test]
fn triggerable_default() {
    let t = Triggerable::default();
    assert_eq!(t.uuid, Uuid::nil());
    assert_eq!(t.name, "");
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
    let scheduler = Arc::new(TestScheduler);
    let writer = HeapWriter::new("test-prefix".to_string(), storage, scheduler).unwrap();

    // Pushing empty items should succeed without doing anything
    let result = writer.push(&[]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn writer_push_with_no_scheduled_items() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(ConfigurableScheduler::new());
    let writer = HeapWriter::new("test-no-schedule".to_string(), storage, scheduler).unwrap();

    let item = Triggerable {
        uuid: Uuid::new_v4(),
        name: "unscheduled".to_string(),
    };

    // Item has no schedule, so push should succeed but not create any buckets
    let result = writer.push(&[item]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn writer_push_with_scheduler_error() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(ConfigurableScheduler::new());
    scheduler.set_error_on_schedule(true);

    let writer = HeapWriter::new("test-error".to_string(), storage, scheduler).unwrap();

    let item = Triggerable {
        uuid: Uuid::new_v4(),
        name: "error-item".to_string(),
    };

    // Should propagate the scheduler error
    let result = writer.push(&[item]).await;
    assert!(result.is_err());
    match result {
        Err(Error::Internal(msg)) => assert!(msg.contains("Simulated error")),
        _ => panic!("Expected Internal error"),
    }
}

// Async tests for HeapPruner
#[tokio::test]
async fn pruner_with_empty_heap() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let pruner = HeapPruner::new("empty-heap".to_string(), storage, scheduler).unwrap();

    // Pruning empty heap should succeed
    let result = pruner.prune(Limits::default()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn pruner_respects_limits() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let pruner = HeapPruner::new("limited-prune".to_string(), storage, scheduler).unwrap();

    let limits = Limits {
        buckets_to_read: Some(5),
        max_items: None,
    };

    // Should respect the limit even if more buckets exist
    let result = pruner.prune(limits).await;
    assert!(result.is_ok());
}

// Async tests for HeapReader
#[tokio::test]
async fn reader_peek_empty_heap() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let reader = HeapReader::new("empty-reader".to_string(), storage, scheduler).unwrap();

    let items = reader.peek(|_| true, Limits::default()).await;
    assert!(items.is_ok());
    assert_eq!(items.unwrap().len(), 0);
}

#[tokio::test]
async fn reader_peek_with_filter() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let reader = HeapReader::new("filtered-reader".to_string(), storage, scheduler).unwrap();

    // Filter that rejects everything
    let items = reader.peek(|_| false, Limits::default()).await;
    assert!(items.is_ok());
    assert_eq!(items.unwrap().len(), 0);
}

#[tokio::test]
async fn reader_respects_limits() {
    let (_temp_dir, storage) = chroma_storage::test_storage();
    let scheduler = Arc::new(TestScheduler);
    let reader = HeapReader::new("limited-reader".to_string(), storage, scheduler).unwrap();

    let limits = Limits {
        buckets_to_read: Some(3),
        max_items: None,
    };

    // Should respect the bucket limit
    let items = reader.peek(|_| true, limits).await;
    assert!(items.is_ok());
}

// Edge case tests
#[test]
fn triggerable_with_empty_name() {
    let t = Triggerable {
        uuid: Uuid::new_v4(),
        name: String::new(),
    };
    assert_eq!(t.name, "");

    let t2 = t.clone();
    assert_eq!(t, t2);
}

#[test]
fn triggerable_with_very_long_name() {
    let long_name = "a".repeat(10000);
    let t = Triggerable {
        uuid: Uuid::new_v4(),
        name: long_name.clone(),
    };
    assert_eq!(t.name.len(), 10000);
    assert_eq!(t.name, long_name);
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
