#![deny(missing_docs)]
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
//! - [`HeapWriter`]: Adds new tasks to the heap
//! - [`HeapReader`]: Reads tasks from the heap for processing
//! - [`HeapPruner`]: Removes completed tasks and cleans up empty buckets
//! - [`HeapScheduler`]: User-implemented trait that determines task scheduling and completion
//!
//! ## Data Model
//!
//! Each task in the heap is represented by a [`HeapItem`] containing:
//! - A [`Triggerable`] with a UUID and name identifying the task
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

/// Errors that can occur during heap operations.
///
/// This enum represents all possible errors that can occur when interacting
/// with the s3heap system. Errors can originate from storage operations,
/// data format issues, or concurrency conflicts.
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

/// Limits on range-scan-backed operations.
///
/// This struct allows callers to bound the cost and time of operations that scan
/// through heap buckets. By limiting the number of buckets to read, you can ensure
/// predictable performance even when the heap contains many buckets.
///
/// # Examples
///
/// ```
/// use s3heap::Limits;
///
/// // Use default limits (reads up to 1000 buckets)
/// let limits = Limits::default();
///
/// // Create custom limits
/// let custom_limits = Limits::with_buckets(100);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Limits {
    /// Maximum number of buckets to read during a scan operation.
    /// If None, defaults to 1000 buckets.
    buckets_to_read: Option<usize>,
}

impl Limits {
    const DEFAULT_BUCKETS_TO_READ: usize = 1000;

    /// Create limits with a specific maximum number of buckets to read.
    ///
    /// # Arguments
    /// * `max_buckets` - Maximum number of buckets to scan
    ///
    /// # Examples
    ///
    /// ```
    /// use s3heap::Limits;
    ///
    /// let limits = Limits::with_buckets(50);
    /// assert_eq!(limits.max_buckets(), 50);
    /// ```
    pub fn with_buckets(max_buckets: usize) -> Self {
        Self {
            buckets_to_read: Some(max_buckets),
        }
    }

    /// Get the maximum number of buckets to read.
    ///
    /// Returns the configured limit or the default (1000) if not set.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3heap::Limits;
    ///
    /// let default_limits = Limits::default();
    /// assert_eq!(default_limits.max_buckets(), 1000);
    ///
    /// let custom_limits = Limits::with_buckets(50);
    /// assert_eq!(custom_limits.max_buckets(), 50);
    /// ```
    pub fn max_buckets(&self) -> usize {
        self.buckets_to_read
            .unwrap_or(Self::DEFAULT_BUCKETS_TO_READ)
    }
}

//////////////////////////////////////////// Triggerable ///////////////////////////////////////////

/// Represents a task that can be scheduled and triggered in the heap.
///
/// A `Triggerable` consists of two parts:
/// - A UUID identifying the schedulable unit (e.g., a document, job, or entity)
/// - A name specifying which task to execute on that unit
///
/// This allows multiple different tasks to be scheduled for the same entity,
/// each identified by its name.
///
/// # Examples
///
/// ```
/// use s3heap::Triggerable;
/// use uuid::Uuid;
///
/// let task = Triggerable {
///     uuid: Uuid::new_v4(),
///     name: "index_document".to_string(),
/// };
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Triggerable {
    /// The UUID identifying the schedulable unit
    pub uuid: Uuid,
    /// The name of the specific task to execute
    pub name: String,
}

/////////////////////////////////////////// HeapScheduler //////////////////////////////////////////

/// User-implemented trait that defines the scheduling behavior for heap items.
///
/// The `HeapScheduler` trait connects the heap writer to the heap reader by providing
/// the business logic for task scheduling and completion detection. Users must
/// implement this trait to define how their specific tasks should be scheduled
/// and when they are considered complete.
///
/// # Implementation Notes
///
/// Implementations should be thread-safe (Send + Sync) as they may be called
/// from multiple threads concurrently. Consider using interior mutability
/// patterns if state needs to be shared.
///
/// # Examples
///
/// ```
/// use s3heap::{HeapScheduler, Triggerable, Error};
/// use chrono::{DateTime, Utc};
/// use uuid::Uuid;
/// use std::collections::HashMap;
/// use std::sync::Mutex;
///
/// struct MyScheduler {
///     completed_tasks: Mutex<HashMap<(Uuid, Uuid), bool>>,
/// }
///
/// #[async_trait::async_trait]
/// impl HeapScheduler for MyScheduler {
///     async fn is_done(&self, item: &Triggerable, nonce: Uuid) -> Result<bool, Error> {
///         // Check if task is complete in your system
///         Ok(self.completed_tasks.lock().unwrap()
///             .get(&(item.uuid, nonce))
///             .copied()
///             .unwrap_or(false))
///     }
///
///     async fn next_time_and_nonce(
///         &self,
///         item: &Triggerable,
///     ) -> Result<Option<(DateTime<Utc>, Uuid)>, Error> {
///         // Determine when to schedule this task
///         Ok(Some((Utc::now() + chrono::Duration::minutes(5), Uuid::new_v4())))
///     }
/// }
/// ```
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

/// Writer for adding tasks to the S3-backed heap.
///
/// `HeapWriter` provides the interface for scheduling new tasks in the heap.
/// It batches tasks by their scheduled time (rounded to the nearest minute)
/// and stores them efficiently in parquet files on S3.
///
/// For optimal performance, instantiate one `HeapWriter` per heap prefix
/// and reuse it across multiple operations. The API is designed to be
/// batch-centric - scheduling multiple tasks in a single call is more
/// efficient than making multiple calls with single tasks.
///
/// # Thread Safety
///
/// `HeapWriter` is Send + Sync and can be safely shared across threads.
/// Multiple concurrent push operations are safe due to optimistic
/// concurrency control at the storage layer. If concurrent writes to
/// the same bucket occur, they will be automatically retried.
///
/// # Examples
///
/// ```ignore
/// use s3heap::{HeapWriter, Triggerable};
/// use uuid::Uuid;
///
/// let writer = HeapWriter::new(
///     "my-heap".to_string(),
///     storage,
///     scheduler,
/// );
///
/// // Schedule a batch of tasks
/// let tasks = vec![
///     Triggerable {
///         uuid: Uuid::new_v4(),
///         name: "process_payment".to_string(),
///     },
///     Triggerable {
///         uuid: Uuid::new_v4(),
///         name: "send_notification".to_string(),
///     },
/// ];
///
/// writer.push(&tasks).await?;
/// ```
pub struct HeapWriter {
    internal: Internal,
}

impl HeapWriter {
    /// Create a new HeapWriter.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The S3 prefix for storing heap data. Must not be empty or contain "//".
    /// * `storage` - The storage backend to use for S3 operations
    /// * `heap_scheduler` - The scheduler implementation for determining task schedules
    ///
    /// # Panics
    ///
    /// - Panics if `prefix` is empty
    /// - Panics if `prefix` contains "//" (double slashes)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::HeapWriter;
    /// use std::sync::Arc;
    ///
    /// let writer = HeapWriter::new(
    ///     "production/task-queue".to_string(),
    ///     storage,
    ///     Arc::new(scheduler),
    /// );
    /// ```
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

    /// Schedule a batch of tasks in the heap.
    ///
    /// This method queries the [`HeapScheduler`] for each task to determine when it should
    /// be executed. Tasks scheduled for the same minute are automatically batched together
    /// into a single parquet file for efficient storage. Tasks with no scheduled time
    /// (when the scheduler returns `None`) are silently skipped.
    ///
    /// For best performance, batch multiple tasks into a single call rather than
    /// calling this method repeatedly with individual tasks.
    ///
    /// # Arguments
    ///
    /// * `items` - The tasks to schedule. Empty slices are allowed and will return immediately.
    ///
    /// # Errors
    ///
    /// - [`Error::Internal`] if the scheduler returns an error
    /// - [`Error::Storage`] if there's an S3 operation failure
    /// - [`Error::ETagConflict`] if concurrent modifications exhaust retries
    /// - [`Error::Parquet`] if parquet serialization fails
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::Triggerable;
    /// use uuid::Uuid;
    ///
    /// let tasks = vec![
    ///     Triggerable {
    ///         uuid: Uuid::new_v4(),
    ///         name: "daily_report".to_string(),
    ///     },
    /// ];
    ///
    /// // Schedule tasks - those without a next execution time are skipped
    /// writer.push(&tasks).await?;
    ///
    /// // Empty push is safe and does nothing
    /// writer.push(&[]).await?;
    /// ```
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

/// Manages garbage collection of completed tasks from the heap.
///
/// `HeapPruner` scans through heap buckets and removes tasks that have been
/// marked as complete by the [`HeapScheduler`]. When all tasks in a bucket
/// are complete, the entire bucket file is deleted from S3 to save storage costs.
///
/// Pruning is an important maintenance operation that should be run periodically
/// to prevent the heap from growing unbounded with completed tasks.
///
/// # Thread Safety
///
/// `HeapPruner` is Send + Sync. However, running multiple pruners concurrently
/// on the same prefix may cause unnecessary conflicts and retries. It's recommended
/// to have a single pruner instance per heap prefix, possibly running on a
/// scheduled basis.
///
/// # Examples
///
/// ```ignore
/// use s3heap::{HeapPruner, Limits};
///
/// let pruner = HeapPruner::new(
///     "my-heap".to_string(),
///     storage,
///     scheduler,
/// );
///
/// // Prune with default limits (up to 1000 buckets)
/// pruner.prune(Limits::default()).await?;
///
/// // Prune with custom limits
/// pruner.prune(Limits::with_buckets(100)).await?;
/// ```
pub struct HeapPruner {
    internal: Internal,
}

impl HeapPruner {
    /// Create a new HeapPruner.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The S3 prefix for storing heap data. Must not be empty or contain "//".
    /// * `storage` - The storage backend to use for S3 operations
    /// * `heap_scheduler` - The scheduler implementation for checking task completion
    ///
    /// # Panics
    ///
    /// - Panics if `prefix` is empty
    /// - Panics if `prefix` contains "//" (double slashes)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::HeapPruner;
    /// use std::sync::Arc;
    ///
    /// let pruner = HeapPruner::new(
    ///     "production/task-queue".to_string(),
    ///     storage,
    ///     Arc::new(scheduler),
    /// );
    /// ```
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

    /// Remove completed tasks from the heap.
    ///
    /// This method scans through heap buckets (up to the limit specified),
    /// queries the [`HeapScheduler`] to check which tasks are complete,
    /// removes those tasks, and deletes any buckets that become empty.
    ///
    /// Pruning operations use exponential backoff for retry on conflicts,
    /// ensuring eventual consistency even under concurrent modifications.
    ///
    /// # Arguments
    ///
    /// * `limits` - Controls how many buckets to scan. Use [`Limits::default()`]
    ///   for the default of 1000 buckets, or [`Limits::with_buckets()`] for a custom limit.
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] if S3 operations fail
    /// - [`Error::Internal`] if the scheduler returns an error
    /// - [`Error::Arrow`] if parquet deserialization fails
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::Limits;
    ///
    /// // Prune with default limits
    /// pruner.prune(Limits::default()).await?;
    ///
    /// // Prune only the first 50 buckets for faster operation
    /// pruner.prune(Limits::with_buckets(50)).await?;
    /// ```
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

/// Reader for retrieving tasks from the S3-backed heap.
///
/// `HeapReader` provides read-only access to the heap, allowing you to peek at
/// scheduled tasks without removing them. This is useful for monitoring,
/// debugging, or implementing custom task processing logic.
///
/// The reader scans buckets in chronological order, making it efficient for
/// finding tasks that are due to be executed soon.
///
/// # Thread Safety
///
/// `HeapReader` is Send + Sync and can be safely shared across threads.
/// Multiple concurrent peek operations are safe as they only perform reads
/// and do not modify the heap state.
///
/// # Examples
///
/// ```ignore
/// use s3heap::{HeapReader, Limits};
///
/// let reader = HeapReader::new(
///     "my-heap".to_string(),
///     storage,
///     scheduler,
/// );
///
/// // Get all non-completed tasks
/// let all_tasks = reader.peek(|_| true, Limits::default()).await?;
///
/// // Get only tasks with a specific name
/// let specific_tasks = reader.peek(
///     |task| task.name == "process_payment",
///     Limits::with_buckets(100),
/// ).await?;
/// ```
pub struct HeapReader {
    internal: Internal,
}

impl HeapReader {
    /// Create a new HeapReader.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The S3 prefix for storing heap data. Must not be empty or contain "//".
    /// * `storage` - The storage backend to use for S3 operations
    /// * `heap_scheduler` - The scheduler implementation for checking task status
    ///
    /// # Panics
    ///
    /// - Panics if `prefix` is empty
    /// - Panics if `prefix` contains "//" (double slashes)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::HeapReader;
    /// use std::sync::Arc;
    ///
    /// let reader = HeapReader::new(
    ///     "production/task-queue".to_string(),
    ///     storage,
    ///     Arc::new(scheduler),
    /// );
    /// ```
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

    /// Retrieve tasks from the heap that match the given filter.
    ///
    /// This method scans through heap buckets (up to the specified limit) and
    /// returns tasks that meet two criteria:
    /// 1. The task is not marked as complete by the [`HeapScheduler`]
    /// 2. The task passes the provided filter predicate
    ///
    /// Tasks are returned in the order they appear in the heap buckets,
    /// which corresponds roughly to their scheduled execution time.
    ///
    /// # Arguments
    ///
    /// * `should_return` - A predicate function that returns `true` for tasks
    ///   that should be included in the results. This function is called for
    ///   each non-completed task found.
    /// * `limits` - Controls how many buckets to scan. Use [`Limits::default()`]
    ///   for the default of 1000 buckets, or [`Limits::with_buckets()`] for a custom limit.
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] if S3 operations fail
    /// - [`Error::Internal`] if the scheduler returns an error
    /// - [`Error::Arrow`] if parquet deserialization fails
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use s3heap::Limits;
    ///
    /// // Get all pending tasks
    /// let all_pending = reader.peek(|_| true, Limits::default()).await?;
    ///
    /// // Get only high-priority tasks
    /// let high_priority = reader.peek(
    ///     |task| task.name.starts_with("urgent_"),
    ///     Limits::with_buckets(50),
    /// ).await?;
    ///
    /// // Get tasks for a specific entity
    /// let entity_id = uuid::Uuid::new_v4();
    /// let entity_tasks = reader.peek(
    ///     move |task| task.uuid == entity_id,
    ///     Limits::default(),
    /// ).await?;
    /// ```
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
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

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

    // Tests for RetryConfig
    #[test]
    fn retry_config_default_values() {
        let config = RetryConfig::default();
        assert_eq!(config.min_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert_eq!(config.factor, 2.0);
        assert_eq!(config.max_retries, 10);
    }

    #[test]
    fn retry_config_to_backoff() {
        let config = RetryConfig {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            factor: 1.5,
            max_retries: 5,
        };
        let backoff = config.to_backoff();
        // The backoff builder is properly configured
        // We can't easily test the internal state, but we can verify it builds without panic
        let _ = backoff;
    }

    // Tests for Limits
    #[test]
    fn limits_default_is_none() {
        let limits = Limits::default();
        assert_eq!(limits.buckets_to_read, None);
    }

    #[test]
    fn limits_equality() {
        let limits1 = Limits {
            buckets_to_read: Some(100),
        };
        let limits2 = Limits {
            buckets_to_read: Some(100),
        };
        let limits3 = Limits {
            buckets_to_read: Some(200),
        };
        let limits4 = Limits {
            buckets_to_read: None,
        };

        assert_eq!(limits1, limits2);
        assert_ne!(limits1, limits3);
        assert_ne!(limits1, limits4);
    }

    #[test]
    fn limits_clone() {
        let original = Limits {
            buckets_to_read: Some(500),
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
        let writer = HeapWriter::new("test-prefix".to_string(), storage, scheduler);

        // Pushing empty items should succeed without doing anything
        let result = writer.push(&[]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn writer_push_with_no_scheduled_items() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(ConfigurableScheduler::new());
        let writer = HeapWriter::new("test-no-schedule".to_string(), storage, scheduler);

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

        let writer = HeapWriter::new("test-error".to_string(), storage, scheduler);

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
        let pruner = HeapPruner::new("empty-heap".to_string(), storage, scheduler);

        // Pruning empty heap should succeed
        let result = pruner.prune(Limits::default()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pruner_respects_limits() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        let pruner = HeapPruner::new("limited-prune".to_string(), storage, scheduler);

        let limits = Limits {
            buckets_to_read: Some(5),
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
        let reader = HeapReader::new("empty-reader".to_string(), storage, scheduler);

        let items = reader.peek(|_| true, Limits::default()).await;
        assert!(items.is_ok());
        assert_eq!(items.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reader_peek_with_filter() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        let reader = HeapReader::new("filtered-reader".to_string(), storage, scheduler);

        // Filter that rejects everything
        let items = reader.peek(|_| false, Limits::default()).await;
        assert!(items.is_ok());
        assert_eq!(items.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reader_respects_limits() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(TestScheduler);
        let reader = HeapReader::new("limited-reader".to_string(), storage, scheduler);

        let limits = Limits {
            buckets_to_read: Some(3),
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

        // Should still create a valid backoff
        let _backoff = config.to_backoff();
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
        let _backoff = config.to_backoff();
    }

    #[test]
    fn limits_with_max_value() {
        let limits = Limits {
            buckets_to_read: Some(usize::MAX),
        };
        assert_eq!(limits.buckets_to_read, Some(usize::MAX));
    }
}
