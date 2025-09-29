//! Common test utilities for s3heap integration tests.
//!
//! This module provides mock implementations and helper functions
//! for testing s3heap functionality without requiring actual external
//! dependencies.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chroma_storage::{GetOptions, Storage};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use s3heap::{Error, HeapScheduler, Triggerable};

/// Mock implementation of HeapScheduler for testing.
///
/// This scheduler allows tests to configure exactly which tasks are
/// marked as done and when tasks should be scheduled, providing
/// deterministic behavior for testing.
#[derive(Clone)]
pub struct MockHeapScheduler {
    #[allow(clippy::type_complexity)]
    done_items: Arc<Mutex<HashMap<(Uuid, String, Uuid), bool>>>,
    #[allow(clippy::type_complexity)]
    next_times: Arc<Mutex<HashMap<(Uuid, String), Option<(DateTime<Utc>, Uuid)>>>>,
}

impl MockHeapScheduler {
    /// Create a new mock scheduler with empty state.
    pub fn new() -> Self {
        Self {
            done_items: Arc::new(Mutex::new(HashMap::new())),
            next_times: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Configure whether a specific task invocation is complete.
    ///
    /// # Arguments
    ///
    /// * `item` - The triggerable task
    /// * `nonce` - The invocation nonce
    /// * `done` - Whether this invocation is complete
    pub fn set_done(&self, item: &Triggerable, nonce: Uuid, done: bool) {
        let key = (item.uuid, item.name.clone(), nonce);
        self.done_items.lock().unwrap().insert(key, done);
    }

    /// Configure when a task should next be scheduled.
    ///
    /// # Arguments
    ///
    /// * `item` - The triggerable task
    /// * `when` - The next execution time and nonce, or None if the task shouldn't be scheduled
    pub fn set_next_time(&self, item: &Triggerable, when: Option<(DateTime<Utc>, Uuid)>) {
        let key = (item.uuid, item.name.clone());
        self.next_times.lock().unwrap().insert(key, when);
    }
}

impl Default for MockHeapScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HeapScheduler for MockHeapScheduler {
    async fn is_done(&self, item: &Triggerable, nonce: Uuid) -> Result<bool, Error> {
        let key = (item.uuid, item.name.clone(), nonce);
        Ok(self
            .done_items
            .lock()
            .unwrap()
            .get(&key)
            .copied()
            .unwrap_or(false))
    }

    async fn next_time_and_nonce(
        &self,
        item: &Triggerable,
    ) -> Result<Option<(DateTime<Utc>, Uuid)>, Error> {
        let key = (item.uuid, item.name.clone());
        Ok(self.next_times.lock().unwrap().get(&key).cloned().flatten())
    }
}

/// Create a test triggerable with a predictable UUID.
///
/// This helper generates UUIDs deterministically based on an index,
/// making tests reproducible and debuggable.
///
/// # Arguments
///
/// * `index` - A unique index for this triggerable
/// * `name` - The task name
///
/// # Examples
///
/// ```
/// let task = create_test_triggerable(1, "test_task");
/// // UUID will always be 00000000-0000-0000-0000-000000000001
/// ```
pub fn create_test_triggerable(index: u32, name: &str) -> Triggerable {
    let mut bytes = [0u8; 16];
    bytes[12..16].copy_from_slice(&index.to_be_bytes());
    Triggerable {
        uuid: Uuid::from_bytes(bytes),
        name: name.to_string(),
    }
}

/// Create a test timestamp at a specific minute offset.
///
/// Useful for creating predictable test schedules.
///
/// # Arguments
///
/// * `base` - The base timestamp
/// * `minutes` - Number of minutes to add (can be negative)
///
/// # Examples
///
/// ```
/// let base = Utc::now();
/// let five_minutes_later = test_time_at_minute_offset(base, 5);
/// let five_minutes_earlier = test_time_at_minute_offset(base, -5);
/// ```
pub fn test_time_at_minute_offset(base: DateTime<Utc>, minutes: i64) -> DateTime<Utc> {
    base + chrono::Duration::minutes(minutes)
}

/// Generate a deterministic test nonce.
///
/// Creates nonces predictably based on an index for reproducible tests.
///
/// # Arguments
///
/// * `index` - A unique index for this nonce
///
/// # Examples
///
/// ```
/// let nonce = test_nonce(42);
/// // Nonce will always be 0000002a-0000-0000-0000-000000000000
/// ```
pub fn test_nonce(index: u32) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&index.to_be_bytes());
    Uuid::from_bytes(bytes)
}

/// Setup standard test environment with storage and scheduler.
///
/// Creates a new test bucket and mock scheduler for integration tests.
///
/// # Returns
///
/// A tuple of (storage, scheduler) ready for use in tests
pub async fn setup_test_environment() -> (Storage, Arc<MockHeapScheduler>) {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let scheduler = Arc::new(MockHeapScheduler::new());
    (storage, scheduler)
}

/// Verify bucket count and return bucket list.
///
/// Checks that the number of buckets matches expectations and returns
/// the list for further inspection if needed.
///
/// # Arguments
///
/// * `storage` - The storage backend
/// * `prefix` - The bucket prefix to list
/// * `expected_count` - Expected number of buckets
/// * `message` - Assertion message on failure
///
/// # Panics
///
/// Panics if the bucket count doesn't match expectations
pub async fn verify_bucket_count(
    storage: &Storage,
    prefix: &str,
    expected_count: usize,
    message: &str,
) -> Vec<String> {
    let buckets = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .unwrap();
    assert_eq!(buckets.len(), expected_count, "{}", message);
    buckets
}

/// Builder for creating test items with common setup patterns.
///
/// Simplifies the creation of test triggerables with scheduled times
/// and completion states.
pub struct TestItemBuilder<'a> {
    scheduler: &'a MockHeapScheduler,
    index: u32,
    name: String,
    time_offset_minutes: i64,
    is_done: Option<bool>,
    base_time: Option<DateTime<Utc>>,
}

impl<'a> TestItemBuilder<'a> {
    /// Create a new test item builder.
    ///
    /// # Arguments
    ///
    /// * `scheduler` - The mock scheduler to configure
    /// * `index` - Unique index for this item
    /// * `name` - Task name
    pub fn new(scheduler: &'a MockHeapScheduler, index: u32, name: &str) -> Self {
        Self {
            scheduler,
            index,
            name: name.to_string(),
            time_offset_minutes: 0,
            is_done: None,
            base_time: None,
        }
    }

    /// Set the scheduling time as a minute offset from base time.
    pub fn at_minute_offset(mut self, minutes: i64) -> Self {
        self.time_offset_minutes = minutes;
        self
    }

    /// Set the base time for scheduling calculations.
    pub fn with_base_time(mut self, time: DateTime<Utc>) -> Self {
        self.base_time = Some(time);
        self
    }

    /// Mark the item as done or not done.
    pub fn mark_done(mut self, done: bool) -> Self {
        self.is_done = Some(done);
        self
    }

    /// Build the triggerable with all configured settings.
    ///
    /// Creates the triggerable, sets up scheduling, and optionally
    /// marks it as done.
    pub fn build(self) -> Triggerable {
        let item = create_test_triggerable(self.index, &self.name);
        let nonce = test_nonce(self.index);
        let base = self.base_time.unwrap_or_else(Utc::now);
        let time = test_time_at_minute_offset(base, self.time_offset_minutes);

        self.scheduler.set_next_time(&item, Some((time, nonce)));
        if let Some(done) = self.is_done {
            self.scheduler.set_done(&item, nonce, done);
        }
        item
    }

    /// Build the triggerable without scheduling.
    ///
    /// Creates the triggerable but sets its next_time to None,
    /// indicating it should not be scheduled.
    pub fn build_unscheduled(self) -> Triggerable {
        let item = create_test_triggerable(self.index, &self.name);
        self.scheduler.set_next_time(&item, None);
        item
    }
}
