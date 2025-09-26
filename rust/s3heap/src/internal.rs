use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use backon::Retryable;
use bytes::Bytes;
use chrono::round::DurationRound;
use chrono::{DateTime, TimeDelta, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{DeleteOptions, ETag, GetOptions, PutOptions, Storage, StorageError};

use crate::{Error, HeapScheduler, RetryConfig, Triggerable};

///////////////////////////////////////////// HeapItem /////////////////////////////////////////////

/// A HeapItem represents a scheduled task in the heap.
///
/// Each item contains:
/// - `trigger`: The triggerable task with UUID and name
/// - `nonce`: A unique identifier for this specific invocation
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeapItem {
    /// The triggerable task to be executed
    pub trigger: Triggerable,
    /// Unique identifier for this invocation of the task
    pub nonce: Uuid,
}

///////////////////////////////////////////// Internal /////////////////////////////////////////////

/// Internal implementation details for the S3-backed heap.
///
/// This struct handles the low-level operations of storing and retrieving
/// heap items from S3 using parquet files organized into time-based buckets.
pub struct Internal {
    prefix: String,
    storage: Storage,
    heap_scheduler: Arc<dyn HeapScheduler>,
    retry_config: RetryConfig,
}

impl Internal {
    /// Create a new Internal instance.
    ///
    /// # Arguments
    /// * `prefix` - The S3 prefix for storing heap data
    /// * `storage` - The storage backend to use
    /// * `heap_scheduler` - The scheduler implementation
    pub fn new(
        prefix: String,
        storage: Storage,
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

    /// Merge entries into the file on S3.
    ///
    /// This function loads the existing bucket data (if any), appends the new entries,
    /// and writes everything back. It uses exponential backoff for retries.
    ///
    /// # Concurrency Safety
    /// This method uses optimistic concurrency control via ETags. If multiple writers
    /// attempt to modify the same bucket simultaneously, the ETag check will cause
    /// conflicts to be detected and retried automatically.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to merge into
    /// * `entries` - The new entries to add
    ///
    /// # Returns
    /// * `Ok(())` if the merge succeeded
    /// * `Err(Error::ETagConflict)` if retries were exhausted due to conflicts
    /// * `Err` if there was another error during merging
    pub async fn merge_on_s3(
        &self,
        bucket: DateTime<Utc>,
        entries: &[HeapItem],
    ) -> Result<(), Error> {
        let backoff = self.retry_config.to_backoff();

        let entries = entries.to_vec();
        (|| async {
            let (mut on_s3, e_tag) = match self.load_bucket(bucket).await {
                Ok((on_s3, e_tag)) => (on_s3, e_tag),
                Err(Error::Storage(StorageError::NotFound { .. })) => (vec![], None),
                Err(err) => {
                    return Err(err);
                }
            };
            on_s3.extend(entries.iter().cloned());
            self.store_bucket(bucket, &on_s3, e_tag).await
        })
        .retry(backoff)
        .await
    }

    /// List the first 1k buckets in lexicographic order.
    ///
    /// This provides an approximation of the earliest buckets in the heap.
    /// The limit of 1000 is imposed by S3's list operation.
    ///
    /// # Returns
    /// * `Ok(Vec<DateTime<Utc>>)` containing bucket timestamps
    /// * `Err` if there was an error listing buckets
    pub async fn list_approx_first_1k_buckets(&self) -> Result<Vec<DateTime<Utc>>, Error> {
        let first_1k = self
            .storage
            .list_prefix(&self.prefix, GetOptions::default())
            .await?;
        first_1k
            .into_iter()
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

    /// Load the entries from a bucket.
    ///
    /// Reads the parquet file for a given bucket and deserializes the HeapItems.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to load
    ///
    /// # Returns
    /// * `Ok((items, e_tag))` containing the items and the file's ETag
    /// * `Err` if there was an error loading the bucket
    pub async fn load_bucket(
        &self,
        bucket: DateTime<Utc>,
    ) -> Result<(Vec<HeapItem>, Option<ETag>), Error> {
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
            let uuid = batch.column_by_name("uuids").ok_or_else(|| {
                Error::Arrow(format!("missing 'uuids' column in parquet file: {}", path))
            })?;
            let name = batch.column_by_name("names").ok_or_else(|| {
                Error::Arrow(format!("missing 'names' column in parquet file: {}", path))
            })?;
            let nonce = batch.column_by_name("nonces").ok_or_else(|| {
                Error::Arrow(format!("missing 'nonces' column in parquet file: {}", path))
            })?;
            let uuid = uuid
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    Error::Arrow(format!("'uuids' column is not a StringArray in {}", path))
                })?;
            let name = name
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    Error::Arrow(format!("'names' column is not a StringArray in {}", path))
                })?;
            let nonce = nonce
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    Error::Arrow(format!("'nonces' column is not a StringArray in {}", path))
                })?;
            for i in 0..batch.num_rows() {
                if uuid.is_null(i) || name.is_null(i) || nonce.is_null(i) {
                    return Err(Error::Arrow(format!(
                        "null value at row {} in parquet file: {}",
                        i, path
                    )));
                }
                let uuid_str = uuid.value(i);
                let name_str = name.value(i);
                let nonce_str = nonce.value(i);

                let uuid = Uuid::from_str(uuid_str)?;
                let name = name_str.to_string();
                let nonce = Uuid::from_str(nonce_str)?;

                items.push(HeapItem {
                    trigger: Triggerable { uuid, name },
                    nonce,
                });
            }
        }
        Ok((items, e_tag))
    }

    /// Store entries in a bucket.
    ///
    /// Serializes HeapItems to parquet and writes to S3 with conditional updates
    /// based on the ETag to prevent concurrent modification issues.
    ///
    /// # Concurrency Safety
    /// This method uses conditional PUT operations:
    /// - If `e_tag` is Some, the PUT only succeeds if the current ETag matches
    /// - If `e_tag` is None, the PUT only succeeds if the file doesn't exist
    ///   This ensures atomic updates and prevents lost updates in concurrent scenarios.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to store
    /// * `items` - The items to store
    /// * `e_tag` - The expected ETag for conditional updates (None for new files)
    ///
    /// # Returns
    /// * `Ok(())` if the store succeeded
    /// * `Err(Error::ETagConflict)` if the ETag didn't match (concurrent modification)
    /// * `Err` if there was another error
    pub async fn store_bucket(
        &self,
        bucket: DateTime<Utc>,
        items: &[HeapItem],
        e_tag: Option<ETag>,
    ) -> Result<(), Error> {
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

    /// Remove the file corresponding to a bucket.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp to clear
    ///
    /// # Returns
    /// * `Ok(())` if the bucket was deleted or didn't exist
    /// * `Err` if there was an error deleting the bucket
    pub async fn clear_bucket(&self, bucket: DateTime<Utc>) -> Result<(), Error> {
        self.storage
            .delete(&self.path_for_bucket(bucket), DeleteOptions::default())
            .await?;
        Ok(())
    }

    /// Turn a timestamp into a bucket.
    ///
    /// Buckets are aligned to minute boundaries for efficient grouping of tasks.
    ///
    /// # Arguments
    /// * `when` - The timestamp to compute a bucket for
    ///
    /// # Returns
    /// * `Ok(DateTime<Utc>)` truncated to the minute
    /// * `Err` if the timestamp cannot be truncated
    pub fn compute_bucket(&self, when: DateTime<Utc>) -> Result<DateTime<Utc>, Error> {
        Ok(when.duration_trunc(TimeDelta::minutes(1))?)
    }

    /// Get a reference to the heap scheduler.
    ///
    /// # Returns
    /// A reference to the underlying HeapScheduler implementation
    pub fn heap_scheduler(&self) -> &dyn HeapScheduler {
        self.heap_scheduler.as_ref()
    }

    /// Generate the S3 path for a given bucket.
    ///
    /// # Arguments
    /// * `bucket` - The bucket timestamp
    ///
    /// # Returns
    /// The S3 path as a string in the format: `{prefix}/{timestamp}`
    pub fn path_for_bucket(&self, bucket: DateTime<Utc>) -> String {
        format!(
            "{}/{}",
            self.prefix,
            bucket.naive_utc().format("%Y-%m-%dT%H:%M:%SZ")
        )
    }
}

/// Construct a parquet file from a list of HeapItems.
///
/// The parquet file contains three columns:
/// - uuids: String representation of the trigger UUIDs
/// - names: The trigger names
/// - nonces: String representation of the nonce UUIDs
///
/// # Arguments
/// * `items` - The heap items to serialize
///
/// # Returns
/// * `Ok(Vec<u8>)` containing the parquet file bytes
/// * `Err` if serialization fails
fn construct_parquet(items: &[HeapItem]) -> Result<Vec<u8>, Error> {
    let uuids = items
        .iter()
        .map(|x| x.trigger.uuid.to_string())
        .collect::<Vec<_>>();
    let names = items
        .iter()
        .map(|x| x.trigger.name.clone())
        .collect::<Vec<_>>();
    let nonces = items
        .iter()
        .map(|x| x.nonce.to_string())
        .collect::<Vec<_>>();
    // Create an Arrow record batch
    let uuids = StringArray::from(uuids);
    let names = StringArray::from(names);
    let nonces = StringArray::from(nonces);
    let batch = RecordBatch::try_from_iter(vec![
        ("uuids", Arc::new(uuids) as ArrayRef),
        ("names", Arc::new(names) as ArrayRef),
        ("nonces", Arc::new(nonces) as ArrayRef),
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
    use chrono::TimeZone;

    #[test]
    fn bucket_computation_truncates_to_minute() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "test".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler {}),
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
    fn path_for_bucket_format() {
        let (_temp_dir, storage) = chroma_storage::test_storage();
        let internal = Internal {
            prefix: "test-prefix".to_string(),
            storage,
            heap_scheduler: Arc::new(DummyScheduler {}),
            retry_config: RetryConfig::default(),
        };

        let bucket = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();
        let path = internal.path_for_bucket(bucket);
        assert_eq!(path, "test-prefix/2024-03-15T14:30:00Z");

        // Test with different prefix
        let internal2 = Internal {
            prefix: "another/nested/prefix".to_string(),
            storage: chroma_storage::test_storage().1,
            heap_scheduler: Arc::new(DummyScheduler {}),
            retry_config: RetryConfig::default(),
        };
        let path2 = internal2.path_for_bucket(bucket);
        assert_eq!(path2, "another/nested/prefix/2024-03-15T14:30:00Z");
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
    }

    #[test]
    fn construct_parquet_with_items() {
        use crate::Triggerable;

        let items = vec![
            HeapItem {
                trigger: Triggerable {
                    uuid: Uuid::new_v4(),
                    name: "test-task-1".to_string(),
                },
                nonce: Uuid::new_v4(),
            },
            HeapItem {
                trigger: Triggerable {
                    uuid: Uuid::new_v4(),
                    name: "test-task-2".to_string(),
                },
                nonce: Uuid::new_v4(),
            },
        ];

        let result = construct_parquet(&items);
        assert!(result.is_ok());
        let buffer = result.unwrap();
        assert!(!buffer.is_empty());

        // Verify we can read it back
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(buffer),
        )
        .unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
            assert!(batch.column_by_name("uuids").is_some());
            assert!(batch.column_by_name("names").is_some());
            assert!(batch.column_by_name("nonces").is_some());
        }
        assert_eq!(total_rows, 2);
    }

    struct DummyScheduler;

    #[async_trait::async_trait]
    impl crate::HeapScheduler for DummyScheduler {
        async fn is_done(
            &self,
            _item: &crate::Triggerable,
            _nonce: Uuid,
        ) -> Result<bool, crate::Error> {
            Ok(false)
        }

        async fn next_time_and_nonce(
            &self,
            _item: &crate::Triggerable,
        ) -> Result<Option<(DateTime<Utc>, Uuid)>, crate::Error> {
            Ok(None)
        }
    }
}
