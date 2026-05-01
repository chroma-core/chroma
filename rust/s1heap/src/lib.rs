//! s1heap - A single-object persistent heap built on `chroma-storage`.
//!
//! `s1heap` mirrors the public programming model of `s3heap`, but stores the
//! entire heap in one parquet object.
//! The file is updated with optimistic concurrency control using ETags where the
//! underlying storage backend supports conditional writes.
//!
//! # Architecture
//!
//! All heap entries are stored in a single parquet object. Each row contains:
//!
//! - The exact scheduled timestamp
//! - A scheduling UUID
//! - A nonce UUID
//!
//! This keeps the external heap API close to `s3heap`, while reducing the heap
//! to a single storage object and a single Rust source file.

#![deny(missing_docs)]
#![warn(clippy::all)]

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{ETag, GetOptions, PutMode, PutOptions, Storage, StorageError};
use chrono::{DateTime, SecondsFormat, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

const COLUMN_NEXT_SCHEDULEDS: &str = "next_scheduleds";
const COLUMN_SCHEDULING_UUIDS: &str = "scheduling_uuids";
const COLUMN_NONCES: &str = "nonces";

/// Errors that can occur during heap operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// ETag conflict during concurrent modification.
    #[error("e_tag conflict")]
    ETagConflict,
    /// Missing ETag when one was expected.
    #[error("missing e_tag: {0}")]
    MissingETag(String),
    /// Internal implementation error.
    #[error("internal error: {0}")]
    Internal(String),
    /// Partial parquet load failure.
    #[error("partial parquet load failure: {0} errors encountered, first errors: {1:?}")]
    PartialLoadFailure(usize, Vec<String>),
    /// Invalid heap path or prefix.
    #[error("invalid prefix: {0}")]
    InvalidPrefix(String),
    /// Uninitialized heap.
    #[error("uninitialized heap: {0}")]
    UninitializedHeap(String),
    /// Storage backend error.
    #[error("storage error: {0}")]
    Storage(#[from] chroma_storage::StorageError),
    /// UUID parsing error.
    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    /// Parquet format error.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow data processing error.
    #[error("arrow error: {0}")]
    Arrow(String),
    /// Date parsing error.
    #[error("invalid date: {0}")]
    ParseDate(#[from] chrono::ParseError),
}

impl chroma_error::ChromaError for Error {
    fn code(&self) -> chroma_error::ErrorCodes {
        use chroma_error::ErrorCodes;

        match self {
            Error::ETagConflict => ErrorCodes::FailedPrecondition,
            Error::MissingETag(_) => ErrorCodes::FailedPrecondition,
            Error::Internal(_) => ErrorCodes::Internal,
            Error::PartialLoadFailure(..) => ErrorCodes::Internal,
            Error::InvalidPrefix(_) => ErrorCodes::InvalidArgument,
            Error::UninitializedHeap(_) => ErrorCodes::FailedPrecondition,
            Error::Storage(e) => e.code(),
            Error::Uuid(_) => ErrorCodes::InvalidArgument,
            Error::Parquet(_) => ErrorCodes::Internal,
            Error::Arrow(_) => ErrorCodes::Internal,
            Error::ParseDate(_) => ErrorCodes::InvalidArgument,
        }
    }
}

/// Configuration for heap operations.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Configuration {
    /// Retry configuration for storage operations.
    pub backoff: RetryConfig,
}

impl Configuration {
    /// Set the retry configuration.
    pub fn with_backoff(mut self, backoff: RetryConfig) -> Self {
        self.backoff = backoff;
        self
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            backoff: RetryConfig::default(),
        }
    }
}

/// Configuration for retry behavior in heap operations.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RetryConfig {
    /// Base delay for exponential backoff.
    pub min_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Exponential factor for backoff.
    pub factor: f32,
    /// Maximum number of retry attempts.
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
    fn to_backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_factor(self.factor)
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries)
    }
}

/// Limits on range-scan-backed operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    /// Maximum number of items to return or process.
    pub max_items: Option<usize>,
    /// Cut-off time for filtering scheduled items.
    pub time_cut_off: Option<DateTime<Utc>>,
}

impl Limits {
    /// Set the maximum number of items to return.
    pub fn with_items(mut self, max_items: usize) -> Self {
        self.max_items = Some(max_items);
        self
    }

    /// Set the time cut-off for reading items.
    pub fn with_time_cut_off(mut self, time_cut_off: DateTime<Utc>) -> Self {
        self.time_cut_off = Some(time_cut_off);
        self
    }
}

/// UUID type used for scheduling.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct UnitOfSchedulingUuid(Uuid);

impl UnitOfSchedulingUuid {
    /// Create a new scheduling UUID wrapper.
    pub fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for UnitOfSchedulingUuid {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl fmt::Display for UnitOfSchedulingUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A scheduled task with its next execution time and nonce.
#[derive(Clone)]
pub struct Schedule {
    /// The scheduling UUID to be executed.
    pub triggerable: UnitOfSchedulingUuid,
    /// The next scheduled execution time.
    pub next_scheduled: DateTime<Utc>,
    /// The unique identifier for this task invocation.
    pub nonce: Uuid,
}

/// User-implemented trait that supplies completion and schedule information.
#[async_trait::async_trait]
pub trait HeapScheduler: Send + Sync {
    /// Check if a specific task invocation has completed.
    async fn is_done(&self, item: &UnitOfSchedulingUuid, nonce: Uuid) -> Result<bool, Error> {
        let results = self.are_done(&[(*item, nonce)]).await?;
        if results.len() != 1 {
            return Err(Error::Internal(format!(
                "are_done returned {} results for 1 item",
                results.len()
            )));
        }
        Ok(results[0])
    }

    /// Check if multiple task invocations have completed.
    async fn are_done(&self, items: &[(UnitOfSchedulingUuid, Uuid)]) -> Result<Vec<bool>, Error>;

    /// Get the schedule for a specific task by scheduling UUID.
    async fn get_schedule(&self, id: UnitOfSchedulingUuid) -> Result<Option<Schedule>, Error> {
        let mut results = self.get_schedules(&[id]).await?;
        Ok(results.pop())
    }

    /// Get schedules for multiple tasks by scheduling UUID.
    async fn get_schedules(&self, ids: &[UnitOfSchedulingUuid]) -> Result<Vec<Schedule>, Error>;
}

/// A scheduled task instance stored in the heap.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd)]
pub struct HeapItem {
    /// The scheduling UUID for the task.
    pub trigger: UnitOfSchedulingUuid,
    /// The invocation nonce.
    pub nonce: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
struct StoredHeapItem {
    next_scheduled: DateTime<Utc>,
    item: HeapItem,
}

struct Internal {
    storage: Storage,
    path: String,
    heap_scheduler: Arc<dyn HeapScheduler>,
    retry_config: RetryConfig,
}

impl Internal {
    fn new(
        storage: Storage,
        path: String,
        heap_scheduler: Arc<dyn HeapScheduler>,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            storage,
            path,
            heap_scheduler,
            retry_config,
        }
    }

    fn heap_scheduler(&self) -> &dyn HeapScheduler {
        self.heap_scheduler.as_ref()
    }

    async fn load_heap(&self) -> Result<(Vec<StoredHeapItem>, Option<ETag>), Error> {
        let (parquet, e_tag) = self
            .storage
            .get_with_e_tag(&self.path, GetOptions::new(StorageRequestPriority::P0))
            .await?;
        let items = load_parquet(&parquet, &self.path)?;
        Ok((items, e_tag))
    }

    async fn load_heap_or_empty(&self) -> Result<(Vec<StoredHeapItem>, Option<ETag>), Error> {
        match self.load_heap().await {
            Ok(heap) => Ok(heap),
            Err(Error::Storage(StorageError::NotFound { .. })) => Ok((Vec::new(), None)),
            Err(err) => Err(err),
        }
    }

    async fn store_heap(&self, items: &[StoredHeapItem], e_tag: Option<ETag>) -> Result<(), Error> {
        let backoff = self.retry_config.to_backoff();
        let buffer = construct_parquet(items)?;
        let options = PutOptions::default().with_priority(StorageRequestPriority::P0);
        let options = if let Some(e_tag) = e_tag.as_ref() {
            options.with_mode(PutMode::IfMatch(e_tag.clone()))
        } else {
            options.with_mode(PutMode::IfNotExist)
        };

        (|| async {
            self.storage
                .put_bytes(&self.path, buffer.clone(), options.clone())
                .await
                .map_err(Error::from)
        })
        .retry(backoff)
        .await?;

        Ok(())
    }

    async fn merge_on_storage(&self, additions: &[StoredHeapItem]) -> Result<(), Error> {
        let backoff = self.retry_config.to_backoff();
        let additions = additions.to_vec();

        (|| async {
            let (heap, e_tag) = self.load_heap_or_empty().await?;
            let existing_for_scheduler = heap
                .iter()
                .map(|item| (item.item.trigger, item.item.nonce))
                .collect::<Vec<_>>();
            let are_done = self
                .heap_scheduler
                .are_done(&existing_for_scheduler)
                .await?;
            if are_done.len() != existing_for_scheduler.len() {
                return Err(Error::Internal(format!(
                    "scheduler returned {} results for {} items",
                    are_done.len(),
                    existing_for_scheduler.len()
                )));
            }

            let live_existing = heap
                .into_iter()
                .zip(are_done.into_iter())
                .filter_map(|(item, is_done)| (!is_done).then_some(item));
            let heap = dedupe_heap_entries(live_existing.chain(additions.clone()).collect());
            self.store_heap(&heap, e_tag).await
        })
        .retry(backoff)
        .await
    }
}

/// Writer for adding tasks to the single-file heap.
pub struct HeapWriter {
    internal: Internal,
}

impl HeapWriter {
    /// Create a new heap writer.
    pub async fn new(
        storage: Storage,
        prefix: String,
        heap_scheduler: Arc<dyn HeapScheduler>,
    ) -> Result<Self, Error> {
        let config = Configuration::default();
        validate_prefix(&prefix)?;
        initialize_heap_file(&storage, &prefix).await?;

        Ok(Self {
            internal: Internal::new(storage, prefix, heap_scheduler, config.backoff.clone()),
        })
    }

    /// Schedule a batch of tasks in the heap.
    pub async fn push(&self, schedules: &[Schedule]) -> Result<(), Error> {
        if schedules.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            entries.push(StoredHeapItem {
                next_scheduled: schedule.next_scheduled,
                item: HeapItem {
                    trigger: schedule.triggerable,
                    nonce: schedule.nonce,
                },
            });
        }

        self.internal.merge_on_storage(&entries).await
    }
}

/// Statistics from a pruning operation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruneStats {
    /// Number of items pruned.
    pub items_pruned: usize,
    /// Number of items retained from the scanned range.
    pub items_retained: usize,
}

impl PruneStats {
    /// Merge another `PruneStats` into this one.
    pub fn merge(&mut self, other: &PruneStats) -> &mut Self {
        self.items_pruned += other.items_pruned;
        self.items_retained += other.items_retained;
        self
    }
}

impl fmt::Display for PruneStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PruneStats {{ pruned: {}, retained: {} }}",
            self.items_pruned, self.items_retained
        )
    }
}

/// Manages garbage collection of completed tasks from the heap.
pub struct HeapPruner {
    internal: Internal,
    config: Configuration,
}

impl HeapPruner {
    /// Create a new heap pruner.
    pub fn new(
        storage: Storage,
        prefix: String,
        heap_scheduler: Arc<dyn HeapScheduler>,
    ) -> Result<Self, Error> {
        let config = Configuration::default();
        validate_prefix(&prefix)?;

        Ok(Self {
            internal: Internal::new(storage, prefix, heap_scheduler, config.backoff.clone()),
            config,
        })
    }

    /// Remove completed tasks from the heap.
    pub async fn prune(&self, limits: Limits) -> Result<PruneStats, Error> {
        let backoff = self.config.backoff.to_backoff();

        (|| async {
            let (mut heap, e_tag) = self.internal.load_heap_or_empty().await?;
            let max_items = limits.max_items.unwrap_or(usize::MAX);
            let time_cut_off = limits.time_cut_off;
            let prefix_len = heap
                .iter()
                .take_while(|entry| {
                    time_cut_off
                        .map(|cut_off| entry.next_scheduled <= cut_off)
                        .unwrap_or(true)
                })
                .take(max_items)
                .count();

            if prefix_len == 0 {
                return Ok(PruneStats::default());
            }

            let tail = heap.split_off(prefix_len);
            let scheduling_and_nonce = heap
                .iter()
                .map(|entry| (entry.item.trigger, entry.item.nonce))
                .collect::<Vec<_>>();
            let are_done = self
                .internal
                .heap_scheduler()
                .are_done(&scheduling_and_nonce)
                .await?;
            if are_done.len() != scheduling_and_nonce.len() {
                return Err(Error::Internal(format!(
                    "scheduler returned {} results for {} items",
                    are_done.len(),
                    scheduling_and_nonce.len()
                )));
            }

            let mut stats = PruneStats::default();
            let mut retained = Vec::with_capacity(prefix_len + tail.len());
            for (entry, is_done) in heap.into_iter().zip(are_done.into_iter()) {
                if is_done {
                    stats.items_pruned += 1;
                } else {
                    stats.items_retained += 1;
                    retained.push(entry);
                }
            }
            retained.extend(tail);

            if stats.items_pruned > 0 {
                self.internal.store_heap(&retained, e_tag).await?;
            }

            Ok(stats)
        })
        .retry(backoff)
        .await
    }
}

/// Reader for retrieving tasks from the single-file heap.
pub struct HeapReader {
    internal: Internal,
}

impl HeapReader {
    /// Create a new heap reader.
    pub async fn new(
        storage: Storage,
        prefix: String,
        heap_scheduler: Arc<dyn HeapScheduler>,
    ) -> Result<Self, Error> {
        let config = Configuration::default();
        validate_prefix(&prefix)?;

        match storage.get(&prefix, GetOptions::default()).await {
            Ok(_) => {}
            Err(StorageError::NotFound { .. }) => {
                return Err(Error::UninitializedHeap(format!(
                    "heap at prefix '{}' has not been initialized",
                    prefix
                )));
            }
            Err(err) => return Err(Error::Storage(err)),
        }

        Ok(Self {
            internal: Internal::new(storage, prefix, heap_scheduler, config.backoff),
        })
    }

    /// Retrieve tasks from the heap that match the given predicate.
    pub async fn peek(
        &self,
        should_return: impl for<'a> Fn(&'a UnitOfSchedulingUuid, DateTime<Utc>) -> bool + Send + Sync,
        limits: Limits,
    ) -> Result<Vec<(DateTime<Utc>, HeapItem)>, Error> {
        let (heap, _) = self.internal.load_heap_or_empty().await?;
        let max_items = limits.max_items.unwrap_or(usize::MAX);
        let time_cut_off = limits.time_cut_off;
        let candidates = heap
            .into_iter()
            .take_while(|entry| {
                time_cut_off
                    .map(|cut_off| entry.next_scheduled <= cut_off)
                    .unwrap_or(true)
            })
            .filter_map(|entry| {
                should_return(&entry.item.trigger, entry.next_scheduled)
                    .then_some((entry.next_scheduled, entry.item))
            })
            .collect::<Vec<_>>();

        let scheduling_and_nonce = candidates
            .iter()
            .map(|(_, item)| (item.trigger, item.nonce))
            .collect::<Vec<_>>();
        let are_done = self
            .internal
            .heap_scheduler()
            .are_done(&scheduling_and_nonce)
            .await?;
        if are_done.len() != scheduling_and_nonce.len() {
            return Err(Error::Internal(format!(
                "scheduler returned {} results for {} items",
                are_done.len(),
                scheduling_and_nonce.len()
            )));
        }

        let mut returns = Vec::new();
        for ((next_scheduled, item), is_done) in candidates.into_iter().zip(are_done.into_iter()) {
            if !is_done {
                returns.push((next_scheduled, item));
                if returns.len() >= max_items {
                    break;
                }
            }
        }

        Ok(returns)
    }
}

fn validate_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.is_empty() {
        return Err(Error::InvalidPrefix("prefix cannot be empty".to_string()));
    }
    if prefix.contains("//") {
        return Err(Error::InvalidPrefix(
            "prefix cannot contain double slashes".to_string(),
        ));
    }
    Ok(())
}

fn next_scheduled_to_string(next_scheduled: DateTime<Utc>) -> String {
    next_scheduled.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_next_scheduled_string(next_scheduled: &str) -> Result<DateTime<Utc>, Error> {
    Ok(DateTime::parse_from_rfc3339(next_scheduled)?.with_timezone(&Utc))
}

fn get_string_column<'a>(
    batch: &'a RecordBatch,
    column_name: &str,
    path: &str,
) -> Result<&'a StringArray, Error> {
    let column = batch.column_by_name(column_name).ok_or_else(|| {
        Error::Arrow(format!(
            "missing '{}' column in parquet file: {}",
            column_name, path
        ))
    })?;

    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::Arrow(format!(
                "'{}' column is not a StringArray in {}",
                column_name, path
            ))
        })
}

fn dedupe_heap_entries(items: Vec<StoredHeapItem>) -> Vec<StoredHeapItem> {
    let mut deduped = BTreeMap::new();
    for item in items {
        deduped.insert((item.next_scheduled, item.item.trigger), item);
    }
    deduped.into_values().collect()
}

fn construct_parquet(items: &[StoredHeapItem]) -> Result<Vec<u8>, Error> {
    let next_scheduleds = items
        .iter()
        .map(|item| next_scheduled_to_string(item.next_scheduled))
        .collect::<Vec<_>>();
    let scheduling_uuids = items
        .iter()
        .map(|item| item.item.trigger.to_string())
        .collect::<Vec<_>>();
    let nonces = items
        .iter()
        .map(|item| item.item.nonce.to_string())
        .collect::<Vec<_>>();

    let batch = RecordBatch::try_from_iter(vec![
        (
            COLUMN_NEXT_SCHEDULEDS,
            Arc::new(StringArray::from(next_scheduleds)) as ArrayRef,
        ),
        (
            COLUMN_SCHEDULING_UUIDS,
            Arc::new(StringArray::from(scheduling_uuids)) as ArrayRef,
        ),
        (
            COLUMN_NONCES,
            Arc::new(StringArray::from(nonces)) as ArrayRef,
        ),
    ])
    .map_err(|err| Error::Arrow(format!("Failed to create RecordBatch: {}", err)))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(Error::Parquet)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

fn load_parquet(bytes: &Arc<Vec<u8>>, path: &str) -> Result<Vec<StoredHeapItem>, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(bytes.to_vec()))?;
    let reader = builder.build()?;
    let mut items = Vec::new();

    for batch in reader {
        let batch = batch.map_err(|err| Error::Arrow(err.to_string()))?;
        let next_scheduleds = get_string_column(&batch, COLUMN_NEXT_SCHEDULEDS, path)?;
        let scheduling_uuids = get_string_column(&batch, COLUMN_SCHEDULING_UUIDS, path)?;
        let nonces = get_string_column(&batch, COLUMN_NONCES, path)?;
        let mut errors = Vec::new();

        for i in 0..batch.num_rows() {
            if next_scheduleds.is_null(i) || scheduling_uuids.is_null(i) || nonces.is_null(i) {
                errors.push(format!("null value at row {}", i));
                continue;
            }

            let next_scheduled_str = next_scheduleds.value(i);
            let scheduling_uuid_str = scheduling_uuids.value(i);
            let nonce_str = nonces.value(i);

            match (
                parse_next_scheduled_string(next_scheduled_str),
                Uuid::from_str(scheduling_uuid_str),
                Uuid::from_str(nonce_str),
            ) {
                (Ok(next_scheduled), Ok(scheduling_uuid), Ok(nonce)) => {
                    items.push(StoredHeapItem {
                        next_scheduled,
                        item: HeapItem {
                            trigger: scheduling_uuid.into(),
                            nonce,
                        },
                    });
                }
                (Err(e), _, _) => {
                    errors.push(format!("invalid next_scheduled at row {}: {}", i, e));
                }
                (_, Err(e), _) => {
                    errors.push(format!("invalid scheduling UUID at row {}: {}", i, e));
                }
                (_, _, Err(e)) => {
                    errors.push(format!("invalid nonce at row {}: {}", i, e));
                }
            }
        }

        if !errors.is_empty() {
            let first_errors = errors.iter().take(3).cloned().collect::<Vec<_>>();
            return Err(Error::PartialLoadFailure(errors.len(), first_errors));
        }
    }

    items.sort();
    Ok(items)
}

async fn initialize_heap_file(storage: &Storage, path: &str) -> Result<(), Error> {
    match storage.get(path, GetOptions::default()).await {
        Ok(_) => Ok(()),
        Err(StorageError::NotFound { .. }) => {
            let empty = construct_parquet(&[])?;
            match storage
                .put_bytes(
                    path,
                    empty,
                    PutOptions::default().with_mode(PutMode::IfNotExist),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(StorageError::AlreadyExists { .. } | StorageError::Precondition { .. }) => {
                    Ok(())
                }
                Err(err) => Err(Error::Storage(err)),
            }
        }
        Err(err) => Err(Error::Storage(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Timelike};
    use parking_lot::Mutex;
    use std::collections::HashMap;

    struct DummyScheduler;

    #[async_trait::async_trait]
    impl HeapScheduler for DummyScheduler {
        async fn are_done(
            &self,
            items: &[(UnitOfSchedulingUuid, Uuid)],
        ) -> Result<Vec<bool>, Error> {
            Ok(vec![false; items.len()])
        }

        async fn get_schedules(
            &self,
            _ids: &[UnitOfSchedulingUuid],
        ) -> Result<Vec<Schedule>, Error> {
            Ok(Vec::new())
        }
    }

    struct ConfigurableScheduler {
        done_items: Arc<Mutex<HashMap<(Uuid, Uuid), bool>>>,
    }

    impl ConfigurableScheduler {
        fn new() -> Self {
            Self {
                done_items: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn set_done(&self, item: &UnitOfSchedulingUuid, nonce: Uuid, done: bool) {
            self.done_items
                .lock()
                .insert((*item.as_uuid(), nonce), done);
        }
    }

    #[async_trait::async_trait]
    impl HeapScheduler for ConfigurableScheduler {
        async fn are_done(
            &self,
            items: &[(UnitOfSchedulingUuid, Uuid)],
        ) -> Result<Vec<bool>, Error> {
            let done_items = self.done_items.lock();
            Ok(items
                .iter()
                .map(|(item, nonce)| {
                    done_items
                        .get(&(*item.as_uuid(), *nonce))
                        .copied()
                        .unwrap_or(false)
                })
                .collect())
        }

        async fn get_schedules(
            &self,
            _ids: &[UnitOfSchedulingUuid],
        ) -> Result<Vec<Schedule>, Error> {
            Ok(Vec::new())
        }
    }

    fn make_scheduling_uuid(scheduling: u128) -> UnitOfSchedulingUuid {
        Uuid::from_u128(scheduling).into()
    }

    fn make_schedule(
        triggerable: UnitOfSchedulingUuid,
        when: DateTime<Utc>,
        nonce: u128,
    ) -> Schedule {
        Schedule {
            triggerable,
            next_scheduled: when,
            nonce: Uuid::from_u128(nonce),
        }
    }

    #[tokio::test]
    async fn writer_initializes_heap_without_overwriting_existing_data() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let when = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 17).unwrap();
        let schedule = make_schedule(make_scheduling_uuid(10), when, 100);

        let writer = HeapWriter::new(storage.clone(), "heap-file".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer.push(&[schedule.clone()]).await.unwrap();

        let _writer2 = HeapWriter::new(storage.clone(), "heap-file".to_string(), scheduler.clone())
            .await
            .unwrap();

        let reader = HeapReader::new(storage, "heap-file".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1.nonce, schedule.nonce);
    }

    #[tokio::test]
    async fn reader_requires_initialized_heap() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let result = HeapReader::new(storage, "missing-heap".to_string(), scheduler).await;
        assert!(matches!(result, Err(Error::UninitializedHeap(_))));
    }

    #[tokio::test]
    async fn heap_components_reject_invalid_prefixes() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);

        let writer = HeapWriter::new(storage.clone(), "".to_string(), scheduler.clone()).await;
        assert!(matches!(writer, Err(Error::InvalidPrefix(_))));

        let pruner = HeapPruner::new(storage.clone(), "a//b".to_string(), scheduler.clone());
        assert!(matches!(pruner, Err(Error::InvalidPrefix(_))));

        let reader = HeapReader::new(storage, "a//b".to_string(), scheduler).await;
        assert!(matches!(reader, Err(Error::InvalidPrefix(_))));
    }

    #[tokio::test]
    async fn push_and_peek_across_multiple_times() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let schedule1 = make_schedule(
            make_scheduling_uuid(1),
            base + Duration::minutes(5) + Duration::seconds(7),
            1,
        );
        let schedule2 = make_schedule(
            make_scheduling_uuid(2),
            base + Duration::minutes(10) + Duration::seconds(3),
            2,
        );

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer
            .push(&[schedule1.clone(), schedule2.clone()])
            .await
            .unwrap();

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, schedule1.next_scheduled);
        assert_eq!(items[0].1.trigger, schedule1.triggerable);
        assert_eq!(items[1].0, schedule2.next_scheduled);
        assert_eq!(items[1].1.trigger, schedule2.triggerable);
    }

    #[tokio::test]
    async fn same_exact_time_deduplicates_by_scheduling_uuid() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let when = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 17).unwrap();
        let triggerable = make_scheduling_uuid(1);
        let schedule1 = make_schedule(triggerable, when, 1);
        let schedule2 = make_schedule(triggerable, when, 2);

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer.push(&[schedule1]).await.unwrap();
        writer.push(&[schedule2.clone()]).await.unwrap();

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, when);
        assert_eq!(items[0].1.trigger, triggerable);
        assert_eq!(items[0].1.nonce, schedule2.nonce);
    }

    #[tokio::test]
    async fn same_scheduling_uuid_can_exist_multiple_times_within_same_minute() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap();
        let triggerable = make_scheduling_uuid(1);
        let schedule1 = make_schedule(triggerable, base + Duration::seconds(5), 1);
        let schedule2 = make_schedule(triggerable, base + Duration::seconds(35), 2);

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer
            .push(&[schedule1.clone(), schedule2.clone()])
            .await
            .unwrap();

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, schedule1.next_scheduled);
        assert_eq!(items[1].0, schedule2.next_scheduled);
    }

    #[tokio::test]
    async fn peek_respects_filter_and_item_limits() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let schedules = vec![
            make_schedule(make_scheduling_uuid(1), base + Duration::minutes(1), 1),
            make_schedule(
                make_scheduling_uuid(2),
                base + Duration::minutes(1) + Duration::seconds(1),
                2,
            ),
            make_schedule(make_scheduling_uuid(3), base + Duration::minutes(2), 3),
        ];

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer.push(&schedules).await.unwrap();

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader
            .peek(
                |scheduling, _| *scheduling.as_uuid() != Uuid::from_u128(2),
                Limits::default().with_items(1),
            )
            .await
            .unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1.trigger.as_uuid(), &Uuid::from_u128(1));
    }

    #[tokio::test]
    async fn peek_respects_exact_time_cut_off() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let schedule1 = make_schedule(make_scheduling_uuid(1), base + Duration::seconds(10), 1);
        let schedule2 = make_schedule(make_scheduling_uuid(2), base + Duration::seconds(20), 2);
        let schedule3 = make_schedule(make_scheduling_uuid(3), base + Duration::seconds(40), 3);

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer
            .push(&[schedule1.clone(), schedule2.clone(), schedule3.clone()])
            .await
            .unwrap();

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader
            .peek(
                |_, _| true,
                Limits::default().with_time_cut_off(base + Duration::seconds(20)),
            )
            .await
            .unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, schedule1.next_scheduled);
        assert_eq!(items[1].0, schedule2.next_scheduled);
    }

    #[tokio::test]
    async fn prune_removes_completed_items_and_updates_stats() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(ConfigurableScheduler::new());
        let when = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 17).unwrap();
        let schedule1 = make_schedule(make_scheduling_uuid(1), when, 1);
        let schedule2 = make_schedule(make_scheduling_uuid(2), when + Duration::seconds(5), 2);
        scheduler.set_done(&schedule1.triggerable, schedule1.nonce, true);
        scheduler.set_done(&schedule2.triggerable, schedule2.nonce, false);

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer
            .push(&[schedule1.clone(), schedule2.clone()])
            .await
            .unwrap();

        let pruner =
            HeapPruner::new(storage.clone(), "heap".to_string(), scheduler.clone()).unwrap();
        let stats = pruner.prune(Limits::default()).await.unwrap();
        assert_eq!(
            stats,
            PruneStats {
                items_pruned: 1,
                items_retained: 1,
            }
        );

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1.trigger, schedule2.triggerable);
    }

    #[tokio::test]
    async fn prune_respects_time_cut_off() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(ConfigurableScheduler::new());
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let schedule1 = make_schedule(make_scheduling_uuid(1), base + Duration::seconds(10), 1);
        let schedule2 = make_schedule(make_scheduling_uuid(2), base + Duration::seconds(30), 2);
        scheduler.set_done(&schedule1.triggerable, schedule1.nonce, true);
        scheduler.set_done(&schedule2.triggerable, schedule2.nonce, false);

        let writer = HeapWriter::new(storage.clone(), "heap".to_string(), scheduler.clone())
            .await
            .unwrap();
        writer
            .push(&[schedule1.clone(), schedule2.clone()])
            .await
            .unwrap();

        let pruner =
            HeapPruner::new(storage.clone(), "heap".to_string(), scheduler.clone()).unwrap();
        let stats = pruner
            .prune(Limits::default().with_time_cut_off(base + Duration::seconds(20)))
            .await
            .unwrap();
        assert_eq!(
            stats,
            PruneStats {
                items_pruned: 1,
                items_retained: 0,
            }
        );

        let reader = HeapReader::new(storage, "heap".to_string(), scheduler)
            .await
            .unwrap();
        let items = reader.peek(|_, _| true, Limits::default()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1.trigger, schedule2.triggerable);
    }

    #[tokio::test]
    async fn prune_on_uninitialized_heap_is_empty() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let pruner = HeapPruner::new(storage, "heap".to_string(), scheduler).unwrap();
        let stats = pruner.prune(Limits::default()).await.unwrap();
        assert_eq!(stats, PruneStats::default());
    }

    #[test]
    fn construct_parquet_round_trips_entries() {
        let entries = vec![
            StoredHeapItem {
                next_scheduled: Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 17).unwrap(),
                item: HeapItem {
                    trigger: make_scheduling_uuid(2),
                    nonce: Uuid::from_u128(3),
                },
            },
            StoredHeapItem {
                next_scheduled: Utc.with_ymd_and_hms(2024, 1, 1, 12, 6, 42).unwrap(),
                item: HeapItem {
                    trigger: make_scheduling_uuid(5),
                    nonce: Uuid::from_u128(6),
                },
            },
        ];

        let buffer = construct_parquet(&entries).unwrap();
        let loaded = load_parquet(&Arc::new(buffer), "heap").unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn load_parquet_round_trips_fractional_seconds() {
        let entries = vec![StoredHeapItem {
            next_scheduled: Utc
                .with_ymd_and_hms(2024, 1, 1, 12, 5, 17)
                .unwrap()
                .with_nanosecond(123_456_789)
                .unwrap(),
            item: HeapItem {
                trigger: make_scheduling_uuid(2),
                nonce: Uuid::from_u128(3),
            },
        }];

        let buffer = construct_parquet(&entries).unwrap();
        let loaded = load_parquet(&Arc::new(buffer), "heap").unwrap();
        assert_eq!(loaded, entries);
    }
}
