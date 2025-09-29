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
use chroma_storage::Storage;
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
    /// Invalid bucket timestamp (not minute-aligned)
    #[error("invalid bucket: {0}")]
    InvalidBucket(String),
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
/// let custom_limits = Limits::default().with_buckets(100);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Limits {
    /// Maximum number of buckets to read during a scan operation.
    /// If None, defaults to 1000 buckets.
    pub buckets_to_read: Option<usize>,
    /// Maximum number of items to return.
    /// If None, returns all items found within bucket limits.
    pub max_items: Option<usize>,
}

impl Limits {
    const DEFAULT_BUCKETS_TO_READ: usize = 1000;

    /// Set the maximum number of buckets to read.
    ///
    /// # Arguments
    /// * `max_buckets` - Maximum number of buckets to scan
    ///
    /// # Examples
    ///
    /// ```
    /// use s3heap::Limits;
    ///
    /// let limits = Limits::default().with_buckets(50);
    /// assert_eq!(limits.max_buckets(), 50);
    /// ```
    pub fn with_buckets(mut self, max_buckets: usize) -> Self {
        self.buckets_to_read = Some(max_buckets);
        self
    }

    /// Set the maximum number of items to return.
    ///
    /// # Arguments
    /// * `max_items` - Maximum number of items to return
    ///
    /// # Examples
    ///
    /// ```
    /// use s3heap::Limits;
    ///
    /// let limits = Limits::default().with_items(100);
    /// ```
    pub fn with_items(mut self, max_items: usize) -> Self {
        self.max_items = Some(max_items);
        self
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
    /// let custom_limits = Limits::default().with_buckets(50);
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
        validate_prefix(&prefix);
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

        // Execute bucket writes in parallel with concurrency limit
        use futures::stream::{self, StreamExt, TryStreamExt};

        stream::iter(buckets.into_iter())
            .map(|(bucket, entries)| async move {
                self.internal.merge_on_s3(bucket, &entries).await
            })
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }
}

//////////////////////////////////////////// HeapPruner ////////////////////////////////////////////

/// Statistics from a pruning operation.
#[derive(Debug, Clone, Default)]
pub struct PruneStats {
    /// Number of items that were pruned (removed)
    pub items_pruned: usize,
    /// Number of items that were retained
    pub items_retained: usize,
    /// Number of buckets that were deleted entirely
    pub buckets_deleted: usize,
    /// Number of buckets that were updated
    pub buckets_updated: usize,
}

impl PruneStats {
    /// Combine stats from multiple pruning operations.
    pub fn merge(&mut self, other: &PruneStats) {
        self.items_pruned += other.items_pruned;
        self.items_retained += other.items_retained;
        self.buckets_deleted += other.buckets_deleted;
        self.buckets_updated += other.buckets_updated;
    }
}

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
/// pruner.prune(Limits::default().with_buckets(100)).await?;
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
        validate_prefix(&prefix);
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
    ///   for the default of 1000 buckets, or use `.with_buckets()` for a custom limit.
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
    /// pruner.prune(Limits::default().with_buckets(50)).await?;
    /// ```
    pub async fn prune(&self, limits: Limits) -> Result<PruneStats, Error> {
        let buckets = self.internal.list_approx_first_1k_buckets().await?;
        let mut total_stats = PruneStats::default();
        for bucket in buckets.into_iter().take(limits.max_buckets()) {
            let stats = self.prune_bucket(bucket).await?;
            total_stats.merge(&stats);
        }
        Ok(total_stats)
    }

    /// Prune completed tasks from a single bucket.
    ///
    /// This will remove items that are complete. Empty buckets are immediately deleted.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to prune
    ///
    /// # Returns
    /// * `Ok(PruneStats)` with statistics about the pruning operation
    /// * `Err` if there was an error during pruning
    pub async fn prune_bucket(&self, bucket: DateTime<Utc>) -> Result<PruneStats, Error> {
        let backoff = RetryConfig::default().to_backoff();

        (|| async { self.prune_bucket_inner(bucket).await })
            .retry(backoff)
            .await
    }

    /// Internal function that performs a single attempt to prune a bucket.
    async fn prune_bucket_inner(&self, bucket: DateTime<Utc>) -> Result<PruneStats, Error> {
        let heap_scheduler = self.internal.heap_scheduler();
        let (entries, e_tag) = self.internal.load_bucket_or_empty(bucket).await?;
        if entries.is_empty() {
            return Ok(PruneStats::default());
        }

        // Check items in parallel for better performance
        use futures::stream::{self, StreamExt, TryStreamExt};

        let original_count = entries.len();
        let to_retain: Vec<HeapItem> = stream::iter(entries.into_iter())
            .map(|entry| async move {
                let should_retain = !heap_scheduler.is_done(&entry.trigger, entry.nonce).await?;
                Ok::<_, Error>((entry, should_retain))
            })
            .buffer_unordered(10)
            .try_filter_map(|(entry, should_retain)| async move {
                Ok(if should_retain { Some(entry) } else { None })
            })
            .try_collect()
            .await?;

        let (buckets_deleted, buckets_updated) = if to_retain.is_empty() {
            self.internal.clear_bucket(bucket).await?;
            (1, 0)
        } else {
            self.internal
                .store_bucket(bucket, &to_retain, e_tag)
                .await?;
            (0, 1)
        };

        let stats = PruneStats {
            items_retained: to_retain.len(),
            items_pruned: original_count - to_retain.len(),
            buckets_deleted,
            buckets_updated,
        };

        Ok(stats)
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
///     Limits::default().with_buckets(100),
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
        validate_prefix(&prefix);
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
    ///   for the default of 1000 buckets, or use `.with_buckets()` for a custom limit.
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
    ///     Limits::default().with_buckets(50),
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
        let max_items = limits.max_items.unwrap_or(usize::MAX);

        'outer: for bucket in buckets.into_iter().take(limits.max_buckets()) {
            let (entries, _) = self.internal.load_bucket_or_empty(bucket).await?;
            for entry in entries {
                if !heap_scheduler.is_done(&entry.trigger, entry.nonce).await?
                    && should_return(&entry.trigger)
                {
                    returns.push(entry);

                    // Early termination when we've collected enough items
                    if returns.len() >= max_items {
                        break 'outer;
                    }
                }
            }
        }

        Ok(returns)
    }
}

/// Validate that a prefix meets the requirements for heap operations.
///
/// # Panics
///
/// - Panics if `prefix` is empty
/// - Panics if `prefix` contains "//" (double slashes)
fn validate_prefix(prefix: &str) {
    assert!(!prefix.is_empty(), "prefix cannot be empty");
    assert!(
        !prefix.contains("//"),
        "prefix cannot contain double slashes"
    );
}
