//! Common test utilities for s3heap integration tests.
//!
//! This module provides mock implementations and helper functions
//! for testing s3heap functionality without requiring actual external
//! dependencies.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use chroma_storage::{GetOptions, Storage};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use uuid::Uuid;

use s3heap::{Error, HeapScheduler, Schedule, Triggerable};

/// Mock implementation of HeapScheduler for testing.
///
/// This scheduler allows tests to configure exactly which tasks are
/// marked as done and when tasks should be scheduled, providing
/// deterministic behavior for testing.
#[derive(Clone)]
pub struct MockHeapScheduler {
    #[allow(clippy::type_complexity)]
    done_items: Arc<Mutex<HashMap<(Uuid, Uuid, Uuid), bool>>>,
    schedules: Arc<Mutex<HashMap<Uuid, Schedule>>>,
}

impl MockHeapScheduler {
    /// Create a new mock scheduler with empty state.
    pub fn new() -> Self {
        Self {
            done_items: Arc::new(Mutex::new(HashMap::new())),
            schedules: Arc::new(Mutex::new(HashMap::new())),
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
        let key = (
            *item.partitioning.as_uuid(),
            *item.scheduling.as_uuid(),
            nonce,
        );
        self.done_items.lock().insert(key, done);
    }

    /// Configure the schedule for a specific task.
    ///
    /// # Arguments
    ///
    /// * `id` - The task UUID
    /// * `schedule` - The task schedule, or None to remove the schedule
    pub fn set_schedule(&self, id: Uuid, schedule: Option<Schedule>) {
        if let Some(sched) = schedule {
            self.schedules.lock().insert(id, sched);
        } else {
            self.schedules.lock().remove(&id);
        }
    }
}

impl Default for MockHeapScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HeapScheduler for MockHeapScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
        let done_items = self.done_items.lock();
        Ok(items
            .iter()
            .map(|(item, nonce)| {
                let key = (
                    *item.partitioning.as_uuid(),
                    *item.scheduling.as_uuid(),
                    *nonce,
                );
                done_items.get(&key).copied().unwrap_or(false)
            })
            .collect())
    }

    async fn get_schedules(&self, ids: &[Uuid]) -> Result<Vec<Schedule>, Error> {
        let schedules = self.schedules.lock();
        Ok(ids
            .iter()
            .filter_map(|id| schedules.get(id).cloned())
            .collect())
    }
}

/// Create a test triggerable with a predictable UUID.
///
/// This helper generates UUIDs deterministically based on indices,
/// making tests reproducible and debuggable.
///
/// # Arguments
///
/// * `partitioning_index` - Index for the partitioning UUID
/// * `scheduling_index` - Index for the scheduling UUID
///
/// # Examples
///
/// ```
/// let task = create_test_triggerable(1, 2);
/// // Partitioning UUID will be 00000000-0000-0000-0000-000000000001
/// // Scheduling UUID will be 00000000-0000-0000-0000-000000000002
/// ```
pub fn create_test_triggerable(partitioning_index: u32, scheduling_index: u32) -> Triggerable {
    let mut partitioning_bytes = [0u8; 16];
    partitioning_bytes[12..16].copy_from_slice(&partitioning_index.to_be_bytes());
    let mut scheduling_bytes = [0u8; 16];
    scheduling_bytes[12..16].copy_from_slice(&scheduling_index.to_be_bytes());
    Triggerable {
        partitioning: Uuid::from_bytes(partitioning_bytes).into(),
        scheduling: Uuid::from_bytes(scheduling_bytes).into(),
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
        .unwrap()
        .into_iter()
        .filter(|x| !x.ends_with("/INIT"))
        .collect::<Vec<_>>();
    assert_eq!(buckets.len(), expected_count, "{}", message);
    buckets
}

/// Builder for creating test items with common setup patterns.
///
/// Simplifies the creation of test triggerables with scheduled times
/// and completion states.
pub struct TestItemBuilder<'a> {
    scheduler: &'a MockHeapScheduler,
    partitioning_index: u32,
    scheduling_index: u32,
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
    /// * `partitioning_index` - Unique index for the partitioning UUID
    /// * `scheduling_index` - Unique index for the scheduling UUID
    pub fn new(
        scheduler: &'a MockHeapScheduler,
        partitioning_index: u32,
        scheduling_index: u32,
    ) -> Self {
        Self {
            scheduler,
            partitioning_index,
            scheduling_index,
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
    pub fn build(self) -> Schedule {
        let item = create_test_triggerable(self.partitioning_index, self.scheduling_index);
        let nonce = test_nonce(self.scheduling_index);
        let base = self.base_time.unwrap_or_else(Utc::now);
        let time = test_time_at_minute_offset(base, self.time_offset_minutes);

        let schedule = Schedule {
            triggerable: item,
            next_scheduled: time,
            nonce,
        };
        self.scheduler.set_schedule(
            *schedule.triggerable.scheduling.as_uuid(),
            Some(schedule.clone()),
        );
        if let Some(done) = self.is_done {
            self.scheduler.set_done(&schedule.triggerable, nonce, done);
        }
        schedule
    }

    /// Build the triggerable without scheduling.
    ///
    /// Creates the triggerable but sets its next_time to None,
    /// indicating it should not be scheduled.
    pub fn build_unscheduled(self) -> Triggerable {
        let item = create_test_triggerable(self.partitioning_index, self.scheduling_index);
        self.scheduler
            .set_schedule(*item.scheduling.as_uuid(), None);
        item
    }
}
