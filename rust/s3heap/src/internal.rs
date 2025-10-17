use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use backon::Retryable;
use bytes::Bytes;
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{DeleteOptions, ETag, GetOptions, PutOptions, Storage, StorageError};
use chrono::round::DurationRound;
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::{Error, HeapScheduler, RetryConfig, Triggerable};

/// Column name constants for the parquet schema
const COLUMN_PARTITIONING_UUIDS: &str = "partitioning_uuids";
const COLUMN_SCHEDULING_UUIDS: &str = "scheduling_uuids";
const COLUMN_NONCES: &str = "nonces";

/// Helper function to extract and validate a string column from a parquet record batch.
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

///////////////////////////////////////////// HeapItem /////////////////////////////////////////////

/// A scheduled task instance in the heap.
///
/// Each `HeapItem` represents a specific invocation of a task at a particular time.
/// The combination of the trigger and nonce uniquely identifies this invocation,
/// allowing the same task to be scheduled multiple times with different nonces.
///
/// # Examples
///
/// ```
/// use s3heap::{HeapItem, Triggerable, UnitOfPartitioningUuid, UnitOfSchedulingUuid};
/// use uuid::Uuid;
///
/// let item = HeapItem {
///     trigger: Triggerable {
///         partitioning: UnitOfPartitioningUuid::new(Uuid::new_v4()),
///         scheduling: UnitOfSchedulingUuid::new(Uuid::new_v4()),
///     },
///     nonce: Uuid::new_v4(),
/// };
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd)]
pub struct HeapItem {
    /// The triggerable task to be executed
    pub trigger: Triggerable,
    /// Unique identifier for this invocation of the task
    pub nonce: Uuid,
}

///////////////////////////////////////////// Internal /////////////////////////////////////////////

/// Internal implementation for S3-backed heap operations.
///
/// This struct encapsulates the low-level operations of storing and retrieving
/// heap items from S3 using parquet files organized into time-based buckets.
/// It handles the complexity of bucket management, parquet serialization,
/// and concurrent access control via ETags.
///
/// # Design Decisions
///
/// - **Bucket Granularity**: One minute was chosen as the bucket size to balance
///   between file count (too many small files) and update contention (too few large files)
/// - **Parquet Format**: Provides efficient compression and columnar storage for UUID/string data
/// - **ETag-based Concurrency**: Uses S3's conditional PUT/GET for optimistic concurrency control
pub struct Internal {
    storage: Storage,
    prefix: String,
    heap_scheduler: Arc<dyn HeapScheduler>,
    retry_config: RetryConfig,
}

impl Internal {
    /// Create a new Internal instance.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The S3 prefix for storing heap data
    /// * `storage` - The storage backend to use
    /// * `heap_scheduler` - The scheduler implementation
    /// * `retry_config` - Configuration for retry behavior on conflicts
    pub fn new(
        storage: Storage,
        prefix: String,
        heap_scheduler: Arc<dyn HeapScheduler>,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            prefix,
            storage,
            heap_scheduler,
            retry_config,
        }
    }

    /// Atomically append entries to a bucket on S3.
    ///
    /// This method implements an optimistic concurrency control pattern:
    /// 1. Load the current bucket contents (if it exists)
    /// 2. Append new entries to the existing data
    /// 3. Write back with ETag validation
    /// 4. Retry with exponential backoff on conflicts
    ///
    /// # Concurrency Safety
    ///
    /// Multiple writers can safely call this method concurrently for the same bucket.
    /// The ETag mechanism ensures that only one writer succeeds at a time, with
    /// others automatically retrying. This provides eventual consistency without
    /// requiring distributed locks.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp to merge into
    /// * `entries` - The new entries to add
    ///
    /// # Errors
    ///
    /// - [`Error::ETagConflict`] if retries were exhausted due to concurrent modifications
    /// - [`Error::Storage`] if S3 operations fail
    /// - [`Error::Parquet`] if serialization fails
    pub async fn merge_on_s3(
        &self,
        bucket: DateTime<Utc>,
        entries: &[HeapItem],
    ) -> Result<(), Error> {
        let backoff = self.retry_config.to_backoff();

        let entries = entries.to_vec();
        (|| async {
            let (on_s3, e_tag) = self.load_bucket_or_empty(bucket).await?;
            let triggerables = on_s3
                .iter()
                .map(|x| (x.trigger, x.nonce))
                .collect::<Vec<_>>();
            let schedules = self.heap_scheduler.are_done(&triggerables).await?;
            let mut results = Vec::with_capacity(on_s3.len().saturating_add(entries.len()));
            for (item, is_done) in on_s3.into_iter().zip(schedules) {
                if !is_done {
                    results.push(item)
                }
            }
            results.extend(entries.clone());
            results.sort();
            results.reverse();
            results.dedup_by_key(|x| x.trigger);
            results.reverse();
            self.store_bucket(bucket, &results, e_tag).await
        })
        .retry(backoff)
        .await
    }

    /// List approximately the first 1000 buckets in chronological order.
    ///
    /// S3's list operation returns keys in lexicographic order, which for our
    /// timestamp-based keys corresponds to chronological order. The ~1000 limit
    /// is imposed by S3's ListObjectsV2 API.
    ///
    /// # Implementation Note
    ///
    /// The actual number returned may be less than 1000 if fewer buckets exist,
    /// or slightly different due to S3's pagination boundaries.
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] if the S3 list operation fails
    /// - [`Error::Internal`] if a returned path has an unexpected format
    /// - [`Error::ParseDate`] if a bucket timestamp cannot be parsed
    pub async fn list_approx_first_1k_buckets(&self) -> Result<Vec<DateTime<Utc>>, Error> {
        let first_1k = self
            .storage
            .list_prefix(&self.prefix, GetOptions::default())
            .await?;
        first_1k
            .into_iter()
            .filter(|x| !x.ends_with("INIT"))
            .map(|p| -> Result<_, Error> {
                let Some(dt) = p
                    .strip_prefix(&self.prefix)
                    .map(|s| s.trim_start_matches('/'))
                else {
                    return Err(Error::Internal(format!(
                        "invalid prefix returned from list: expected path to start with '{}', got '{}'",
                        self.prefix, p
                    )));
                };
                // Validate the timestamp format
                if dt.is_empty() {
                    return Err(Error::Internal(format!(
                        "empty timestamp in path: {}",
                        p
                    )));
                }
                Ok(DateTime::parse_from_rfc3339(dt)?.with_timezone(&Utc))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Load all entries from a bucket.
    ///
    /// Reads the parquet file for a given bucket and deserializes all HeapItems
    /// contained within. Also returns the ETag for use in subsequent conditional updates.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp to load
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The vector of HeapItems in the bucket
    /// - The ETag of the file (if the storage backend provides one)
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] if the file cannot be read from S3
    /// - [`Error::Parquet`] if the parquet file is corrupted
    /// - [`Error::Arrow`] if required columns are missing or have wrong types
    /// - [`Error::Uuid`] if UUID parsing fails
    /// - [`Error::InvalidBucket`] if bucket is not minute-aligned
    pub async fn load_bucket(
        &self,
        bucket: DateTime<Utc>,
    ) -> Result<(Vec<HeapItem>, Option<ETag>), Error> {
        // Validate that bucket is minute-aligned
        if bucket.second() != 0 || bucket.nanosecond() != 0 {
            return Err(Error::InvalidBucket(format!(
                "Bucket {} is not minute-aligned",
                bucket
            )));
        }
        let path = self.path_for_bucket(bucket);
        let (parquet, e_tag) = self
            .storage
            .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
            .await?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(parquet.to_vec()))?;
        let reader = builder.build()?;
        let mut items = vec![];
        for batch in reader {
            let batch = batch.map_err(|err| Error::Arrow(err.to_string()))?;
            let partitioning_uuids = get_string_column(&batch, COLUMN_PARTITIONING_UUIDS, &path)?;
            let scheduling_uuids = get_string_column(&batch, COLUMN_SCHEDULING_UUIDS, &path)?;
            let nonces = get_string_column(&batch, COLUMN_NONCES, &path)?;
            let mut errors = Vec::new();
            for i in 0..batch.num_rows() {
                if partitioning_uuids.is_null(i) || scheduling_uuids.is_null(i) || nonces.is_null(i)
                {
                    errors.push(format!("null value at row {}", i));
                    continue;
                }
                let partitioning_uuid_str = partitioning_uuids.value(i);
                let scheduling_uuid_str = scheduling_uuids.value(i);
                let nonce_str = nonces.value(i);

                match (
                    Uuid::from_str(partitioning_uuid_str),
                    Uuid::from_str(scheduling_uuid_str),
                    Uuid::from_str(nonce_str),
                ) {
                    (Ok(partitioning_uuid), Ok(scheduling_uuid), Ok(nonce)) => {
                        items.push(HeapItem {
                            trigger: Triggerable {
                                partitioning: partitioning_uuid.into(),
                                scheduling: scheduling_uuid.into(),
                            },
                            nonce,
                        });
                    }
                    (Err(e), _, _) => {
                        errors.push(format!("invalid partitioning UUID at row {}: {}", i, e));
                    }
                    (_, Err(e), _) => {
                        errors.push(format!("invalid scheduling UUID at row {}: {}", i, e));
                    }
                    (_, _, Err(e)) => {
                        errors.push(format!("invalid nonce at row {}: {}", i, e));
                    }
                }
            }

            // Return error if there were any loading failures
            if !errors.is_empty() {
                let first_errors: Vec<_> = errors.iter().take(3).cloned().collect();
                return Err(Error::PartialLoadFailure(errors.len(), first_errors));
            }
        }
        Ok((items, e_tag))
    }

    /// Atomically store entries in a bucket.
    ///
    /// Serializes HeapItems to parquet format and writes to S3 using conditional
    /// PUT operations to ensure atomicity. This is the core primitive for safe
    /// concurrent updates to the heap.
    ///
    /// # Concurrency Control
    ///
    /// This method implements conditional writes:
    /// - If `e_tag` is `Some`, the PUT only succeeds if the current ETag matches (update case)
    /// - If `e_tag` is `None`, the PUT only succeeds if the file doesn't exist (create case)
    ///
    /// This ensures that concurrent modifications are detected and can be retried,
    /// preventing lost updates without requiring distributed locks.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp to store
    /// * `items` - The items to store (will replace existing contents)
    /// * `e_tag` - The expected ETag for conditional updates (`None` for new files)
    ///
    /// # Errors
    ///
    /// - [`Error::ETagConflict`] if the ETag doesn't match (concurrent modification detected)
    /// - [`Error::Storage`] if S3 operations fail
    /// - [`Error::Parquet`] if serialization fails
    pub async fn store_bucket(
        &self,
        bucket: DateTime<Utc>,
        items: &[HeapItem],
        e_tag: Option<ETag>,
    ) -> Result<(), Error> {
        // Validate that bucket is minute-aligned
        if bucket.second() != 0 || bucket.nanosecond() != 0 {
            return Err(Error::InvalidBucket(format!(
                "Bucket {} is not minute-aligned",
                bucket
            )));
        }
        let backoff = self.retry_config.to_backoff();

        let path = self.path_for_bucket(bucket);
        let buffer = construct_parquet(items)?;
        let options = if let Some(e_tag) = e_tag.as_ref() {
            PutOptions::if_matches(e_tag, StorageRequestPriority::P0)
        } else {
            PutOptions::if_not_exists(StorageRequestPriority::P0)
        };

        (|| async {
            self.storage
                .put_bytes(&path, buffer.clone(), options.clone())
                .await
                .map_err(Error::from)
        })
        .retry(backoff)
        .await?;

        Ok(())
    }

    /// Delete a bucket file from S3.
    ///
    /// Removes the entire parquet file for a bucket, typically called when
    /// all tasks in the bucket have been completed.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp to clear
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] if the S3 delete operation fails
    ///
    /// # Note
    ///
    /// Deleting a non-existent bucket is typically not an error in S3.
    pub async fn clear_bucket(&self, bucket: DateTime<Utc>) -> Result<(), Error> {
        self.storage
            .delete(&self.path_for_bucket(bucket), DeleteOptions::default())
            .await?;
        Ok(())
    }

    /// Compute the bucket for a given timestamp.
    ///
    /// Buckets are aligned to minute boundaries, meaning all tasks scheduled
    /// within the same minute are stored in the same bucket file. This provides
    /// a good balance between file size and update contention.
    ///
    /// # Arguments
    ///
    /// * `when` - The timestamp to compute a bucket for
    ///
    /// # Returns
    ///
    /// The timestamp truncated to the start of its minute
    ///
    /// # Errors
    ///
    /// - [`Error::RoundError`] if the timestamp cannot be truncated (extremely rare)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // 12:34:56 -> 12:34:00
    /// let bucket = internal.compute_bucket(timestamp)?;
    /// ```
    pub fn compute_bucket(&self, when: DateTime<Utc>) -> Result<DateTime<Utc>, Error> {
        Ok(when.duration_trunc(TimeDelta::minutes(1))?)
    }

    /// Get a reference to the heap scheduler.
    ///
    /// Provides access to the scheduler for checking task completion
    /// and determining scheduling times.
    pub fn heap_scheduler(&self) -> &dyn HeapScheduler {
        self.heap_scheduler.as_ref()
    }

    /// Generate the S3 path for a given bucket.
    ///
    /// Converts a bucket timestamp into the S3 object key where its
    /// parquet file is stored.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp (must be minute-aligned)
    ///
    /// # Returns
    ///
    /// The S3 path in the format: `{prefix}/{timestamp}`
    /// where timestamp is formatted as `YYYY-MM-DDTHH:MM:SSZ`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // For prefix "my-heap" and bucket 2024-03-15T14:30:00Z
    /// // Returns: "my-heap/2024-03-15T14:30:00Z"
    /// let path = internal.path_for_bucket(bucket);
    /// ```
    pub fn path_for_bucket(&self, bucket: DateTime<Utc>) -> String {
        format!(
            "{}/{}",
            self.prefix,
            bucket.naive_utc().format("%Y-%m-%dT%H:%M:%SZ")
        )
    }

    /// Load a bucket or return empty if not found.
    ///
    /// This is a common pattern where we want to treat a missing bucket
    /// as an empty bucket rather than an error.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket timestamp to load
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The vector of HeapItems in the bucket (empty if not found)
    /// - The ETag of the file (None if not found)
    pub async fn load_bucket_or_empty(
        &self,
        bucket: DateTime<Utc>,
    ) -> Result<(Vec<HeapItem>, Option<ETag>), Error> {
        match self.load_bucket(bucket).await {
            Ok((entries, e_tag)) => Ok((entries, e_tag)),
            Err(Error::Storage(StorageError::NotFound { .. })) => Ok((vec![], None)),
            Err(err) => Err(err),
        }
    }
}

/// Serialize HeapItems into a parquet file.
///
/// Creates a parquet file with three string columns:
/// - `uuids`: String representation of the trigger UUIDs
/// - `names`: The trigger task names
/// - `nonces`: String representation of the invocation nonces
///
/// The parquet format provides efficient compression (using Snappy) and
/// columnar storage, which is ideal for our UUID and string data.
///
/// # Arguments
///
/// * `items` - The heap items to serialize
///
/// # Returns
///
/// The serialized parquet file as a byte vector
///
/// # Errors
///
/// - [`Error::Arrow`] if the record batch cannot be created
/// - [`Error::Parquet`] if parquet serialization fails
///
/// # Implementation Notes
///
/// - Uses Snappy compression for good compression/speed tradeoff
/// - All columns are non-nullable strings
/// - Empty input produces a valid parquet file with no rows
fn construct_parquet(items: &[HeapItem]) -> Result<Vec<u8>, Error> {
    let partitioning_uuids = items
        .iter()
        .map(|x| x.trigger.partitioning.to_string())
        .collect::<Vec<_>>();
    let scheduling_uuids = items
        .iter()
        .map(|x| x.trigger.scheduling.to_string())
        .collect::<Vec<_>>();
    let nonces = items
        .iter()
        .map(|x| x.nonce.to_string())
        .collect::<Vec<_>>();
    // Create an Arrow record batch
    let partitioning_uuids = StringArray::from(partitioning_uuids);
    let scheduling_uuids = StringArray::from(scheduling_uuids);
    let nonces = StringArray::from(nonces);
    let batch = RecordBatch::try_from_iter(vec![
        (
            COLUMN_PARTITIONING_UUIDS,
            Arc::new(partitioning_uuids) as ArrayRef,
        ),
        (
            COLUMN_SCHEDULING_UUIDS,
            Arc::new(scheduling_uuids) as ArrayRef,
        ),
        (COLUMN_NONCES, Arc::new(nonces) as ArrayRef),
    ])
    .map_err(|err| Error::Arrow(format!("Failed to create RecordBatch: {}", err)))?;

    // Write to parquet.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buffer = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(Error::Parquet)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schedule;
    use chrono::TimeZone;
    use std::time::Duration;

    /// A dummy scheduler implementation for testing purposes.
    ///
    /// This scheduler always reports that items are not done and have no scheduled times.
    pub struct DummyScheduler;

    #[async_trait::async_trait]
    impl HeapScheduler for DummyScheduler {
        async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
            Ok(vec![false; items.len()])
        }

        async fn get_schedules(&self, _ids: &[Uuid]) -> Result<Vec<Schedule>, Error> {
            Ok(vec![])
        }
    }

    // HeapItem tests
    #[test]
    fn heap_item_creation_and_equality() {
        let trigger = crate::Triggerable {
            partitioning: Uuid::new_v4().into(),
            scheduling: Uuid::new_v4().into(),
        };
        let nonce = Uuid::new_v4();

        let item1 = HeapItem { trigger, nonce };

        let item2 = HeapItem { trigger, nonce };

        assert_eq!(item1, item2);
        assert_eq!(item1.trigger, trigger);
        assert_eq!(item1.nonce, nonce);
    }

    #[test]
    fn heap_item_clone() {
        let item = HeapItem {
            trigger: crate::Triggerable {
                partitioning: Uuid::new_v4().into(),
                scheduling: Uuid::new_v4().into(),
            },
            nonce: Uuid::new_v4(),
        };

        let cloned = item.clone();
        assert_eq!(item, cloned);
        assert_eq!(item.trigger, cloned.trigger);
        assert_eq!(item.nonce, cloned.nonce);
    }

    #[test]
    fn heap_item_default() {
        let item = HeapItem::default();
        assert_eq!(item.trigger.partitioning.as_uuid(), &Uuid::nil());
        assert_eq!(item.trigger.scheduling.as_uuid(), &Uuid::nil());
        assert_eq!(item.nonce, Uuid::nil());
    }

    // Internal struct tests
    #[test]
    fn internal_new() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let retry_config = RetryConfig {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            factor: 1.5,
            max_retries: 3,
        };

        let internal = Internal::new(
            storage,
            "test-prefix".to_string(),
            scheduler.clone(),
            retry_config.clone(),
        );

        assert_eq!(internal.prefix, "test-prefix");
        assert_eq!(internal.retry_config.min_delay, retry_config.min_delay);
        assert_eq!(internal.retry_config.max_delay, retry_config.max_delay);
        assert_eq!(internal.retry_config.factor, retry_config.factor);
        assert_eq!(internal.retry_config.max_retries, retry_config.max_retries);
    }

    #[test]
    fn bucket_computation_truncates_to_minute() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "test".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        // Test that times within the same minute go to the same bucket
        let time1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 34, 5).unwrap();
        let time2 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 34, 30).unwrap();
        let time3 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 34, 59).unwrap();

        let bucket1 = internal.compute_bucket(time1).unwrap();
        let bucket2 = internal.compute_bucket(time2).unwrap();
        let bucket3 = internal.compute_bucket(time3).unwrap();

        let expected = Utc.with_ymd_and_hms(2024, 1, 1, 12, 34, 0).unwrap();
        assert_eq!(bucket1, expected);
        assert_eq!(bucket2, expected);
        assert_eq!(bucket3, expected);

        // Test that times already at minute boundary stay the same
        let exact_minute = Utc.with_ymd_and_hms(2024, 1, 1, 12, 34, 0).unwrap();
        let bucket = internal.compute_bucket(exact_minute).unwrap();
        assert_eq!(bucket, exact_minute);
    }

    #[test]
    fn bucket_computation_edge_cases() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "test".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        // Test edge of year transition
        let new_year = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();
        let bucket = internal.compute_bucket(new_year).unwrap();
        assert_eq!(
            bucket,
            Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 0).unwrap()
        );

        // Test first minute of new year
        let first_minute = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 30).unwrap();
        let bucket = internal.compute_bucket(first_minute).unwrap();
        assert_eq!(bucket, Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());

        // Test leap year boundary
        let leap_day = Utc.with_ymd_and_hms(2024, 2, 29, 12, 34, 56).unwrap();
        let bucket = internal.compute_bucket(leap_day).unwrap();
        assert_eq!(
            bucket,
            Utc.with_ymd_and_hms(2024, 2, 29, 12, 34, 0).unwrap()
        );
    }

    #[test]
    fn path_for_bucket_format() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "test-prefix".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        let bucket = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();
        let path = internal.path_for_bucket(bucket);
        assert_eq!(path, "test-prefix/2024-03-15T14:30:00Z");

        // Test with different prefix
        let internal2 = Internal {
            prefix: "another/nested/prefix".to_string(),
            storage: chroma_storage::test_storage().1,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };
        let path2 = internal2.path_for_bucket(bucket);
        assert_eq!(path2, "another/nested/prefix/2024-03-15T14:30:00Z");
    }

    #[test]
    fn path_for_bucket_edge_cases() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "edge".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        // Test midnight
        let midnight = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let path = internal.path_for_bucket(midnight);
        assert_eq!(path, "edge/2024-01-01T00:00:00Z");

        // Test last minute of day
        let last_minute = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 0).unwrap();
        let path = internal.path_for_bucket(last_minute);
        assert_eq!(path, "edge/2024-12-31T23:59:00Z");

        // Test single digit month and day
        let single_digits = Utc.with_ymd_and_hms(2024, 3, 5, 7, 9, 0).unwrap();
        let path = internal.path_for_bucket(single_digits);
        assert_eq!(path, "edge/2024-03-05T07:09:00Z");
    }

    #[test]
    fn heap_scheduler_reference() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let scheduler = Arc::new(DummyScheduler);
        let internal = Internal {
            prefix: "test".to_string(),
            storage,
            heap_scheduler: scheduler.clone(),
            retry_config: RetryConfig::default(),
        };

        // Verify we can get the scheduler reference
        let scheduler_ref = internal.heap_scheduler();
        // Just verify it's accessible - can't really test the pointer itself
        let _ = scheduler_ref;
    }

    #[test]
    fn construct_parquet_empty_items() {
        // Test that construct_parquet handles empty list correctly
        let items = vec![];
        let result = construct_parquet(&items);
        assert!(result.is_ok());
        let buffer = result.unwrap();
        assert!(
            !buffer.is_empty(),
            "Parquet file should have headers even when empty"
        );

        // Verify the parquet file is valid even when empty
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(buffer),
        )
        .unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn construct_parquet_with_items() {
        use crate::Triggerable;

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let nonce1 = Uuid::new_v4();
        let nonce2 = Uuid::new_v4();

        let items = vec![
            HeapItem {
                trigger: Triggerable {
                    partitioning: uuid1.into(),
                    scheduling: uuid2.into(),
                },
                nonce: nonce1,
            },
            HeapItem {
                trigger: Triggerable {
                    partitioning: uuid2.into(),
                    scheduling: uuid1.into(),
                },
                nonce: nonce2,
            },
        ];

        let result = construct_parquet(&items);
        assert!(result.is_ok());
        let buffer = result.unwrap();
        assert!(!buffer.is_empty());

        // Verify we can read it back and the data matches
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(buffer),
        )
        .unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        let mut found_items = vec![];

        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();

            // Verify columns exist
            let partitioning_uuids = batch.column_by_name(COLUMN_PARTITIONING_UUIDS).unwrap();
            let scheduling_uuids = batch.column_by_name(COLUMN_SCHEDULING_UUIDS).unwrap();
            let nonces = batch.column_by_name(COLUMN_NONCES).unwrap();

            // Extract and verify data
            let partitioning_uuids = partitioning_uuids
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let scheduling_uuids = scheduling_uuids
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let nonces = nonces.as_any().downcast_ref::<StringArray>().unwrap();

            for i in 0..batch.num_rows() {
                found_items.push((
                    partitioning_uuids.value(i).to_string(),
                    scheduling_uuids.value(i).to_string(),
                    nonces.value(i).to_string(),
                ));
            }
        }

        assert_eq!(total_rows, 2);
        assert_eq!(found_items.len(), 2);

        // Verify the data matches what we put in
        assert!(found_items.iter().any(|(u, n, nc)| u == &uuid1.to_string()
            && n == &uuid2.to_string()
            && nc == &nonce1.to_string()));
        assert!(found_items.iter().any(|(u, n, nc)| u == &uuid2.to_string()
            && n == &uuid1.to_string()
            && nc == &nonce2.to_string()));
    }

    #[test]
    fn construct_parquet_single_item() {
        use crate::Triggerable;

        let scheduling_id = Uuid::new_v4();
        let item = HeapItem {
            trigger: Triggerable {
                partitioning: Uuid::new_v4().into(),
                scheduling: scheduling_id.into(),
            },
            nonce: Uuid::new_v4(),
        };

        let result = construct_parquet(&[item.clone()]);
        assert!(result.is_ok());
        let buffer = result.unwrap();

        // Read it back
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(buffer),
        )
        .unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();

            let scheduling_uuids = batch
                .column_by_name(COLUMN_SCHEDULING_UUIDS)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(scheduling_uuids.value(0), scheduling_id.to_string());
        }
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn construct_parquet_large_batch() {
        use crate::Triggerable;

        // Create a large batch of items
        let items: Vec<HeapItem> = (0..1000)
            .map(|_| HeapItem {
                trigger: Triggerable {
                    partitioning: Uuid::new_v4().into(),
                    scheduling: Uuid::new_v4().into(),
                },
                nonce: Uuid::new_v4(),
            })
            .collect();

        let result = construct_parquet(&items);
        assert!(result.is_ok());
        let buffer = result.unwrap();

        // Read it back and verify count
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(buffer),
        )
        .unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 1000);
    }

    // Async tests for Internal methods
    #[tokio::test]
    async fn list_approx_first_1k_buckets_empty() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "empty-list".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        let buckets = internal.list_approx_first_1k_buckets().await.unwrap();
        assert_eq!(buckets.len(), 0);
    }

    #[tokio::test]
    async fn clear_bucket_nonexistent() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "clear-test".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler),
            retry_config: RetryConfig::default(),
        };

        let bucket = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        // Clearing a non-existent bucket might return an error depending on storage implementation
        // In production S3, it would succeed, but test storage might behave differently
        let _ = internal.clear_bucket(bucket).await;
    }
}
