#![warn(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]

//! s3heap - An S3-backed distributed heap for scheduling tasks at scale.
//!
//! # Overview
//!
//! s3heap provides a distributed, persistent heap data structure backed by S3 storage.
//! It enables scheduling and processing of tasks at scale across multiple workers,
//! with automatic deduplication and completion tracking.
//!
//! # Architecture
//!
//! The heap is organized into time-based buckets, where each bucket corresponds to
//! a one-minute window. Tasks scheduled for the same minute are stored together
//! in a single parquet file on S3, enabling efficient batch operations.
//!
//! ## Core Components
//!
//! - **HeapWriter**: Adds new tasks to the heap
//! - **HeapReader**: Reads tasks from the heap for processing
//! - **HeapPruner**: Removes completed tasks and cleans up empty buckets
//! - **HeapScheduler**: User-implemented trait that determines task scheduling and completion
//!
//! ## Data Model
//!
//! Each task in the heap is represented by a `HeapItem` containing:
//! - A `Triggerable` with a UUID and name identifying the task
//! - A nonce (UUID) uniquely identifying each invocation
//!
//! ## Usage Example
//!
//! ```ignore
//! use s3heap::{HeapWriter, HeapScheduler, Triggerable};
//! use std::sync::Arc;
//!
//! // Implement your scheduler
//! struct MyScheduler;
//! impl HeapScheduler for MyScheduler {
//!     // ... implementation
//! }
//!
//! // Create heap components
//! let scheduler = Arc::new(MyScheduler);
//! let writer = HeapWriter::new("my-heap".to_string(), storage, scheduler);
//!
//! // Schedule tasks
//! let tasks = vec![
//!     Triggerable { uuid: task_id, name: "process_order".to_string() }
//! ];
//! writer.push(&tasks).await?;
//! ```
//!
//! # Concurrency and Safety
//!
//! The heap uses optimistic concurrency control via ETags to handle concurrent
//! modifications. Multiple writers can safely append to the same bucket, with
//! automatic retry on conflicts.
//!
//! # Performance Considerations
//!
//! - Tasks are batched by minute for efficient storage
//! - Parquet compression reduces storage costs
//! - List operations are limited to ~1000 buckets for scalability
//! - Exponential backoff prevents thundering herd problems

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use chroma_storage::{Storage, StorageError};
use chrono::{DateTime, Utc};
use uuid::Uuid;

mod internal;

pub use internal::HeapItem;
use internal::Internal;

/////////////////////////////////////////////// Error //////////////////////////////////////////////

/// Errors that can occur in heap operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// ETag conflict during concurrent modification
    #[error("e_tag conflict")]
    ETagConflict,
    /// Missing ETag when one was expected
    #[error("missing e_tag: {0}")]
    MissingETag(String),
    /// Internal implementation error
    #[error("internal error: {0}")]
    Internal(String),
    /// Storage backend error
    #[error("storage error: {0}")]
    Storage(#[from] chroma_storage::StorageError),
    /// UUID parsing error
    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    /// Parquet file format error
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow data processing error
    #[error("arrow error: {0}")]
    Arrow(String),
    /// Date parsing error
    #[error("invalid date: {0}")]
    ParseDate(#[from] chrono::ParseError),
    /// Date rounding error
    #[error("could not round date: {0}")]
    RoundError(#[from] chrono::RoundingError),
}

////////////////////////////////////////// RetryConfig /////////////////////////////////////////////

/// Configuration for retry behavior in heap operations.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Base delay for exponential backoff (default: 100ms)
    pub min_delay: Duration,
    /// Maximum delay between retries (default: 10s)
    pub max_delay: Duration,
    /// Exponential factor for backoff (default: 2.0)
    pub factor: f32,
    /// Maximum number of retry attempts (default: 10)
    pub max_retries: usize,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            max_retries: 10,
        }
    }
}

impl RetryConfig {
    /// Convert to a backon ExponentialBuilder.
    pub fn to_backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_factor(self.factor)
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries)
    }
}

////////////////////////////////////////////// Limits //////////////////////////////////////////////

/// Limits on range-scan-backed operations.  Used to bound costs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Limits {
    buckets_to_read: Option<usize>,
}

impl Limits {
    const DEFAULT_BUCKETS_TO_READ: usize = 1000;
}

//////////////////////////////////////////// Triggerable ///////////////////////////////////////////

/// A Triggerable item is a UUID identifying the unit of scheduling and a name of the triggerable
/// task on that unit of scheduling.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Triggerable {
    /// The UUID identifying the schedulable unit
    pub uuid: Uuid,
    /// The name of the specific task to execute
    pub name: String,
}

/////////////////////////////////////////// HeapScheduler //////////////////////////////////////////

/// A HeapScheduler connects a heap writer to a heap reader.
///
/// This trait must be implemented by the user to define the scheduling behavior
/// for items in the heap. It provides methods to check if tasks are complete
/// and to determine when tasks should next be executed.
#[async_trait::async_trait]
pub trait HeapScheduler: Send + Sync {
    /// Check if a specific invocation of a task has completed.
    ///
    /// # Arguments
    /// * `item` - The triggerable task to check
    /// * `nonce` - The unique identifier for this specific invocation
    ///
    /// # Returns
    /// * `Ok(true)` if the task has completed
    /// * `Ok(false)` if the task is still pending or running
    /// * `Err` if there was an error checking the status
    async fn is_done(&self, item: &Triggerable, nonce: Uuid) -> Result<bool, Error>;
    /// Get the next scheduled execution time and nonce for a task.
    ///
    /// # Arguments
    /// * `item` - The triggerable task to schedule
    ///
    /// # Returns
    /// * `Ok(Some((time, nonce)))` if the task should be scheduled
    /// * `Ok(None)` if the task should not be scheduled
    /// * `Err` if there was an error determining the schedule
    async fn next_time_and_nonce(
        &self,
        item: &Triggerable,
    ) -> Result<Option<(DateTime<Utc>, Uuid)>, Error>;
}

//////////////////////////////////////////// HeapWriter ////////////////////////////////////////////

/// A HeapWriter is assumed to be instantiated 1:1 with a heap for performance reasons.  And for
/// performance reasons the API is batch-centric.
///
/// This is the structure that writes the heap.  All writes happen via this structure.
///
/// # Thread Safety
/// HeapWriter is Send + Sync and can be shared across threads. Multiple concurrent
/// push operations are safe due to optimistic concurrency control at the storage layer.
pub struct HeapWriter {
    internal: Internal,
}

impl HeapWriter {
    /// Create a new HeapWriter.
    ///
    /// # Arguments
    /// * `prefix` - The S3 prefix for storing heap data
    /// * `storage` - The storage backend to use
    /// * `heap_scheduler` - The scheduler implementation for determining task schedules
    ///
    /// # Panics
    /// Panics if `prefix` is empty
    pub fn new(prefix: String, storage: Storage, heap_scheduler: Arc<dyn HeapScheduler>) -> Self {
        assert!(!prefix.is_empty(), "prefix cannot be empty");
        assert!(
            !prefix.contains("//"),
            "prefix cannot contain double slashes"
        );
        Self {
            internal: Internal::new(prefix, storage, heap_scheduler, RetryConfig::default()),
        }
    }

    /// Push a new set of Triggerable items onto the heap.
    ///
    /// Tasks are grouped by their scheduled minute for efficient storage.
    /// The bigger the batch size, the more efficiency wins to be had.
    ///
    /// # Arguments
    /// * `items` - The tasks to schedule
    ///
    /// # Returns
    /// * `Ok(())` if all tasks were successfully scheduled
    /// * `Err` if there was an error scheduling tasks
    pub async fn push(&self, items: &[Triggerable]) -> Result<(), Error> {
        if items.is_empty() {
            return Ok(());
        }

        let heap_scheduler = self.internal.heap_scheduler();
        let mut buckets: BTreeMap<DateTime<Utc>, Vec<HeapItem>> = BTreeMap::new();

        for item in items {
            let Some((when, nonce)) = heap_scheduler.next_time_and_nonce(item).await? else {
                // Skip items that have no next scheduled time
                continue;
            };
            let heap_item = HeapItem {
                trigger: item.clone(),
                nonce,
            };
            let bucket = self.internal.compute_bucket(when)?;
            buckets.entry(bucket).or_default().push(heap_item);
        }

        for (bucket, entries) in buckets {
            self.internal.merge_on_s3(bucket, &entries).await?;
        }

        Ok(())
    }
}

//////////////////////////////////////////// HeapPruner ////////////////////////////////////////////

/// A HeapPruner manages garbage collection of completed tasks from the heap.
///
/// The pruner scans through heap buckets and removes tasks that have been
/// marked as complete by the HeapScheduler. Empty buckets are deleted.
///
/// # Thread Safety
/// HeapPruner is Send + Sync. However, running multiple pruners concurrently
/// on the same prefix may cause conflicts. It's recommended to have a single
/// pruner instance per heap prefix.
pub struct HeapPruner {
    internal: Internal,
}

impl HeapPruner {
    /// Create a new HeapPruner.
    ///
    /// # Arguments
    /// * `prefix` - The S3 prefix for storing heap data
    /// * `storage` - The storage backend to use
    /// * `heap_scheduler` - The scheduler implementation for checking task completion
    ///
    /// # Panics
    /// Panics if `prefix` is empty
    pub fn new(prefix: String, storage: Storage, heap_scheduler: Arc<dyn HeapScheduler>) -> Self {
        assert!(!prefix.is_empty(), "prefix cannot be empty");
        assert!(
            !prefix.contains("//"),
            "prefix cannot contain double slashes"
        );
        Self {
            internal: Internal::new(prefix, storage, heap_scheduler, RetryConfig::default()),
        }
    }

    /// Prune completed tasks from the heap.
    ///
    /// This method scans approximately the first 1000 buckets in the heap,
    /// removes completed tasks from each bucket, and deletes empty buckets.
    ///
    /// # Returns
    /// * `Ok(())` if pruning succeeded
    /// * `Err` if there was an error during pruning
    pub async fn prune(&self, limits: Limits) -> Result<(), Error> {
        let buckets = self.internal.list_approx_first_1k_buckets().await?;
        for bucket in buckets.into_iter().take(
            limits
                .buckets_to_read
                .unwrap_or(Limits::DEFAULT_BUCKETS_TO_READ),
        ) {
            self.prune_bucket(bucket).await?;
        }
        Ok(())
    }

    /// Prune completed tasks from a single bucket.
    ///
    /// This will remove items that are complete. Empty buckets are immediately deleted.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to prune
    ///
    /// # Returns
    /// * `Ok(())` if the bucket was successfully pruned
    /// * `Err` if there was an error during pruning
    async fn prune_bucket(&self, bucket: DateTime<Utc>) -> Result<(), Error> {
        let backoff = RetryConfig::default().to_backoff();

        (|| async { self.prune_bucket_inner(bucket).await })
            .retry(backoff)
            .await
    }

    /// Internal function that performs a single attempt to prune a bucket.
    async fn prune_bucket_inner(&self, bucket: DateTime<Utc>) -> Result<(), Error> {
        let heap_scheduler = self.internal.heap_scheduler();
        let (entries, e_tag) = match self.internal.load_bucket(bucket).await {
            Ok((entries, e_tag)) => (entries, e_tag),
            Err(Error::Storage(StorageError::NotFound { .. })) => return Ok(()),
            Err(err) => {
                return Err(err);
            }
        };

        let mut to_retain = Vec::with_capacity(entries.len());
        for entry in entries {
            if !heap_scheduler.is_done(&entry.trigger, entry.nonce).await? {
                to_retain.push(entry);
            }
        }

        if to_retain.is_empty() {
            self.internal.clear_bucket(bucket).await?;
        } else {
            self.internal
                .store_bucket(bucket, &to_retain, e_tag)
                .await?;
        }

        Ok(())
    }
}

//////////////////////////////////////////// HeapReader ////////////////////////////////////////////

/// The reader allows for peeking at the first N tasks to be scheduled.
///
/// # Thread Safety
/// HeapReader is Send + Sync and can be safely shared across threads.
/// Multiple concurrent peek operations are safe as they only perform reads.
pub struct HeapReader {
    internal: Internal,
}

impl HeapReader {
    /// Create a new HeapReader.
    ///
    /// # Arguments
    /// * `prefix` - The S3 prefix for storing heap data
    /// * `storage` - The storage backend to use
    /// * `heap_scheduler` - The scheduler implementation for checking task status
    ///
    /// # Panics
    /// Panics if `prefix` is empty
    pub fn new(prefix: String, storage: Storage, heap_scheduler: Arc<dyn HeapScheduler>) -> Self {
        assert!(!prefix.is_empty(), "prefix cannot be empty");
        assert!(
            !prefix.contains("//"),
            "prefix cannot contain double slashes"
        );
        Self {
            internal: Internal::new(prefix, storage, heap_scheduler, RetryConfig::default()),
        }
    }

    /// Peek into the heap, filtering by the should_return predicate.
    ///
    /// This method scans through the heap buckets and returns tasks that:
    /// 1. Are not marked as done by the HeapScheduler
    /// 2. Match the provided filter predicate
    ///
    /// # Arguments
    /// * `should_return` - A predicate function to filter which tasks to return
    ///
    /// # Returns
    /// * `Ok(Vec<HeapItem>)` containing the filtered tasks
    /// * `Err` if there was an error reading the heap
    pub async fn peek(
        &self,
        should_return: impl for<'a> Fn(&'a Triggerable) -> bool + Send + Sync,
        limits: Limits,
    ) -> Result<Vec<HeapItem>, Error> {
        let heap_scheduler = self.internal.heap_scheduler();
        let buckets = self.internal.list_approx_first_1k_buckets().await?;
        let mut returns = vec![];
        for bucket in buckets.into_iter().take(
            limits
                .buckets_to_read
                .unwrap_or(Limits::DEFAULT_BUCKETS_TO_READ),
        ) {
            let (entries, _) = match self.internal.load_bucket(bucket).await {
                Ok((entries, e_tag)) => (entries, e_tag),
                Err(Error::Storage(StorageError::NotFound { .. })) => continue,
                Err(err) => {
                    return Err(err);
                }
            };
            for entry in entries {
                if !heap_scheduler.is_done(&entry.trigger, entry.nonce).await?
                    && should_return(&entry.trigger)
                {
                    returns.push(entry);
                }
            }
        }
        Ok(returns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

    #[test]
    #[should_panic(expected = "prefix cannot be empty")]
    fn writer_panics_on_empty_prefix() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapWriter::new(String::new(), storage, scheduler);
    }

    #[test]
    #[should_panic(expected = "prefix cannot be empty")]
    fn pruner_panics_on_empty_prefix() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapPruner::new(String::new(), storage, scheduler);
    }

    #[test]
    #[should_panic(expected = "prefix cannot be empty")]
    fn reader_panics_on_empty_prefix() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapReader::new(String::new(), storage, scheduler);
    }

    #[test]
    fn heap_components_accept_valid_prefix() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);

        // These should not panic
        let _writer = HeapWriter::new(
            "valid-prefix".to_string(),
            storage.clone(),
            scheduler.clone(),
        );
        let _pruner = HeapPruner::new(
            "valid-prefix".to_string(),
            storage.clone(),
            scheduler.clone(),
        );
        let _reader = HeapReader::new("valid-prefix".to_string(), storage, scheduler);
    }

    #[test]
    #[should_panic(expected = "prefix cannot contain double slashes")]
    fn heap_writer_panics_on_double_slash() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapWriter::new("prefix//with//slashes".to_string(), storage, scheduler);
    }

    #[test]
    #[should_panic(expected = "prefix cannot contain double slashes")]
    fn heap_pruner_panics_on_double_slash() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapPruner::new("prefix//with//slashes".to_string(), storage, scheduler);
    }

    #[test]
    #[should_panic(expected = "prefix cannot contain double slashes")]
    fn heap_reader_panics_on_double_slash() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        HeapReader::new("prefix//with//slashes".to_string(), storage, scheduler);
    }
}
