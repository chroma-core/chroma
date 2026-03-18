use std::num::TryFromIntError;
use std::sync::Arc;

use chroma_cache::{Cache, CacheConfig, CacheError, Weighted};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{GetOptions, Storage, StorageError};
use chroma_types::chroma_proto;
use chroma_types::{LogRecord, OperationRecord, RecordConversionError};
use futures::stream::StreamExt;
use prost::Message;
use thiserror::Error;
use wal3::LogPosition;

/// A fragment pointer returned by the ScoutLogFragments RPC.
///
/// Each pointer describes an immutable parquet file in object storage that
/// contains a contiguous range of log records.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FragmentPointer {
    /// The relative path of the fragment in object storage.
    pub path: String,
    /// The first log offset contained in this fragment.
    pub start_offset: u64,
    /// The first log offset NOT contained in this fragment (exclusive upper bound).
    pub limit_offset: u64,
    /// The size of the fragment file in bytes.
    pub num_bytes: u64,
    /// The storage prefix to prepend when reading from object storage.
    pub storage_prefix: String,
    /// When true the parquet file stores absolute offsets and should be parsed
    /// without a starting log position. When false the file stores relative
    /// offsets and `start_offset` must be supplied as the base.
    pub absolute_offsets: bool,
}

impl From<chroma_proto::LogFragmentPointer> for FragmentPointer {
    fn from(proto: chroma_proto::LogFragmentPointer) -> Self {
        Self {
            path: proto.path,
            start_offset: proto.start_offset,
            limit_offset: proto.limit_offset,
            num_bytes: proto.num_bytes,
            storage_prefix: proto.storage_prefix,
            absolute_offsets: proto.absolute_offsets,
        }
    }
}

/// Cache key for a fragment in the dedicated fragment cache.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FragmentCacheKey {
    storage_prefix: String,
    path: String,
}

/// Cached fragment bytes.
#[derive(Clone, Debug)]
struct CachedFragmentBytes {
    bytes: Arc<Vec<u8>>,
}

impl Weighted for CachedFragmentBytes {
    fn weight(&self) -> usize {
        self.bytes.len()
    }
}

/// Errors that can occur when fetching fragment data from object storage.
#[derive(Error, Debug)]
pub enum FragmentFetchError {
    #[error("Storage error reading fragment {path}: {source}")]
    Storage {
        path: String,
        source: Arc<StorageError>,
    },
    #[error("Fragment parquet parse error: {0}")]
    ParseError(#[from] wal3::Error),
    #[error(
        "Proto decode error for record at offset {offset} in fragment {fragment_path}: {source}"
    )]
    ProtoDecode {
        offset: u64,
        fragment_path: String,
        source: prost::DecodeError,
    },
    #[error("Record conversion error: {0}")]
    RecordConversion(#[from] RecordConversionError),
    #[error("Cache error: {0}")]
    CacheError(#[from] CacheError),
    #[error("Hole detected in log records: expected offset {expected} but found {found}")]
    HoleInLog {
        /// The offset that was expected next in the contiguous sequence.
        expected: i64,
        /// The offset that was actually found.
        found: i64,
    },
    #[error("Integer conversion error: {0}")]
    IntegerConversion(#[from] TryFromIntError),
    #[error("Fragment fetcher not configured")]
    NotConfigured,
}

impl ChromaError for FragmentFetchError {
    fn code(&self) -> ErrorCodes {
        match self {
            FragmentFetchError::Storage { .. } => ErrorCodes::Internal,
            FragmentFetchError::ParseError(_) => ErrorCodes::Internal,
            FragmentFetchError::ProtoDecode { .. } => ErrorCodes::DataLoss,
            FragmentFetchError::RecordConversion(_) => ErrorCodes::Internal,
            FragmentFetchError::CacheError(_) => ErrorCodes::Internal,
            FragmentFetchError::HoleInLog { .. } => ErrorCodes::DataLoss,
            FragmentFetchError::IntegerConversion(_) => ErrorCodes::Internal,
            FragmentFetchError::NotConfigured => ErrorCodes::Internal,
        }
    }
}

/// A library embedded in query/compactor nodes that dereferences fragment
/// pointers returned by ScoutLogFragments and reads fragment data directly
/// from object storage.
///
/// Per the ADR, this owns a dedicated cache instance (Option B) to provide
/// strong isolation from block/sparse/hnsw caches.
#[derive(Debug)]
pub struct FragmentFetcher {
    storage: Storage,
    cache: Box<dyn Cache<FragmentCacheKey, CachedFragmentBytes>>,
}

impl FragmentFetcher {
    /// Create a new fragment fetcher with the given storage and cache config.
    pub async fn new(
        storage: Storage,
        cache_config: &CacheConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let cache =
            chroma_cache::from_config::<FragmentCacheKey, CachedFragmentBytes>(cache_config)
                .await?;
        Ok(Self { storage, cache })
    }

    /// Create a fragment fetcher for testing with an unbounded cache.
    #[cfg(test)]
    pub fn new_for_test(storage: Storage) -> Self {
        let cache = chroma_cache::new_non_persistent_cache_for_test::<
            FragmentCacheKey,
            CachedFragmentBytes,
        >();
        Self { storage, cache }
    }

    /// Fetch and decode log records from a set of fragment pointers.
    ///
    /// Records are filtered to the half-open range [start_offset, limit_offset)
    /// and returned sorted by log_offset. At most `max_concurrency` fragment
    /// fetches are in flight at any given time.  Futures are constructed lazily
    /// so that no more than `max_concurrency` are alive at once.
    #[tracing::instrument(skip(self, pointers), fields(num_fragments = pointers.len()))]
    pub async fn fetch_records(
        self: &Arc<Self>,
        pointers: &[FragmentPointer],
        start_offset: u64,
        limit_offset: u64,
        max_concurrency: usize,
    ) -> Result<Vec<LogRecord>, FragmentFetchError> {
        if pointers.is_empty() {
            if start_offset < limit_offset {
                return Err(FragmentFetchError::HoleInLog {
                    expected: start_offset as i64,
                    found: limit_offset as i64,
                });
            }
            return Ok(Vec::new());
        }
        // NOTE(rescrv): The way this works, it will construct at most max_concurrency futures at
        // once.
        let max_concurrency = max_concurrency.max(1);
        let mut stream = futures::stream::iter(pointers.iter().cloned())
            .map(|pointer| {
                let this = Arc::clone(self);
                async move {
                    this.fetch_fragment(&pointer, start_offset, limit_offset)
                        .await
                }
            })
            .buffer_unordered(max_concurrency);
        let mut all_records: Vec<LogRecord> = Vec::new();
        while let Some(result) = stream.next().await {
            all_records.extend(result?);
        }
        all_records.sort_by_key(|r| r.log_offset);
        if all_records.is_empty() && start_offset < limit_offset {
            return Err(FragmentFetchError::HoleInLog {
                expected: start_offset as i64,
                found: limit_offset as i64,
            });
        }
        check_contiguous(&all_records, start_offset)?;
        Ok(all_records)
    }

    /// Fetch a single fragment from storage, using the cache if available.
    #[tracing::instrument(skip(self))]
    async fn fetch_fragment(
        &self,
        pointer: &FragmentPointer,
        start_offset: u64,
        limit_offset: u64,
    ) -> Result<Vec<LogRecord>, FragmentFetchError> {
        let cache_key = FragmentCacheKey {
            storage_prefix: pointer.storage_prefix.clone(),
            path: pointer.path.clone(),
        };

        let bytes = if let Some(cached) = self.cache.get(&cache_key).await? {
            cached.bytes
        } else {
            let full_path = wal3::fragment_path(&pointer.storage_prefix, &pointer.path);
            let bytes = self
                .storage
                .get(&full_path, GetOptions::new(StorageRequestPriority::P0))
                .await
                .map_err(|e| FragmentFetchError::Storage {
                    path: full_path,
                    source: Arc::new(e),
                })?;
            self.cache
                .insert(
                    cache_key,
                    CachedFragmentBytes {
                        bytes: Arc::clone(&bytes),
                    },
                )
                .await;
            bytes
        };

        let starting_position = if pointer.absolute_offsets {
            None
        } else {
            Some(LogPosition::from_offset(pointer.start_offset))
        };
        let (parsed_records, _num_bytes, _now_us) =
            wal3::interfaces::s3::parse_parquet_fast(&bytes, starting_position).await?;
        drop(bytes);

        let fragment_capacity = pointer
            .limit_offset
            .saturating_sub(pointer.start_offset)
            .min(limit_offset.saturating_sub(start_offset));
        let mut records = Vec::with_capacity(fragment_capacity.try_into()?);
        for (log_position, record_bytes) in parsed_records {
            let offset = log_position.offset();
            if offset < start_offset || offset >= limit_offset {
                continue;
            }
            let proto_op_record = chroma_proto::OperationRecord::decode(record_bytes.as_slice())
                .map_err(|e| FragmentFetchError::ProtoDecode {
                    offset,
                    fragment_path: pointer.path.clone(),
                    source: e,
                })?;
            let record: OperationRecord = proto_op_record.try_into()?;
            records.push(LogRecord {
                log_offset: offset as i64,
                record,
            });
        }
        Ok(records)
    }
}

/// Verify that a sorted slice of log records starts at `start_offset` and has
/// no gaps in offsets.
fn check_contiguous(records: &[LogRecord], start_offset: u64) -> Result<(), FragmentFetchError> {
    if let Some(first) = records.first() {
        if first.log_offset != start_offset as i64 {
            return Err(FragmentFetchError::HoleInLog {
                expected: start_offset as i64,
                found: first.log_offset,
            });
        }
    }
    for window in records.windows(2) {
        if window[1].log_offset != window[0].log_offset + 1 {
            return Err(FragmentFetchError::HoleInLog {
                expected: window[0].log_offset + 1,
                found: window[1].log_offset,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{check_contiguous, FragmentFetchError, FragmentFetcher, FragmentPointer};
    use chroma_types::{LogRecord, Operation, OperationRecord};
    use prost::Message;
    use wal3::{upload_parquet, FragmentIdentifier, FragmentSeqNo, LogPosition, LogWriterOptions};

    fn make_record(log_offset: i64) -> LogRecord {
        LogRecord {
            log_offset,
            record: OperationRecord {
                id: format!("id-{}", log_offset),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        }
    }

    fn make_proto_record(log_offset: i64) -> chroma_types::chroma_proto::OperationRecord {
        let record = make_record(log_offset).record;
        record
            .try_into()
            .expect("OperationRecord should convert to proto")
    }

    async fn write_fragment(
        storage: &chroma_storage::Storage,
        storage_prefix: &str,
        seq_no: u64,
        start_offset: u64,
        offsets: &[i64],
    ) -> String {
        let messages = offsets
            .iter()
            .map(|offset| {
                let proto = make_proto_record(*offset);
                proto.encode_to_vec()
            })
            .collect::<Vec<_>>();
        let (path, _, _) = upload_parquet(
            &LogWriterOptions::default(),
            storage,
            storage_prefix,
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(seq_no)),
            Some(LogPosition::from_offset(start_offset)),
            messages,
            None,
            0,
        )
            .await
            .expect("fragment write should succeed");
        path
    }

    #[test]
    fn fragment_pointer_from_proto_absolute() {
        use chroma_types::chroma_proto::LogFragmentPointer;
        let proto = LogFragmentPointer {
            path: "log/Bucket=0/FragmentSeqNo=0000000000000001.parquet".to_string(),
            start_offset: 0,
            limit_offset: 10,
            num_bytes: 1024,
            storage_prefix: "tenant/database/collection".to_string(),
            absolute_offsets: true,
        };
        let pointer: FragmentPointer = proto.into();
        assert_eq!(
            pointer.path,
            "log/Bucket=0/FragmentSeqNo=0000000000000001.parquet"
        );
        assert_eq!(pointer.start_offset, 0);
        assert_eq!(pointer.limit_offset, 10);
        assert_eq!(pointer.num_bytes, 1024);
        assert_eq!(pointer.storage_prefix, "tenant/database/collection");
        assert!(
            pointer.absolute_offsets,
            "absolute_offsets should be true for S3 fragments"
        );
    }

    #[test]
    fn fragment_pointer_from_proto_relative() {
        use chroma_types::chroma_proto::LogFragmentPointer;
        let proto = LogFragmentPointer {
            path: "log/Bucket=0/FragmentSeqNo=0000000000000001.parquet".to_string(),
            start_offset: 42,
            limit_offset: 52,
            num_bytes: 2048,
            storage_prefix: "tenant/database/collection".to_string(),
            absolute_offsets: false,
        };
        let pointer: FragmentPointer = proto.into();
        assert_eq!(pointer.start_offset, 42);
        assert_eq!(pointer.limit_offset, 52);
        assert!(
            !pointer.absolute_offsets,
            "absolute_offsets should be false for replicated fragments"
        );
    }

    #[test]
    fn check_contiguous_empty() {
        check_contiguous(&[], 0).expect("empty records should have no holes");
    }

    #[test]
    fn check_contiguous_single_record() {
        let records = vec![make_record(5)];
        check_contiguous(&records, 5).expect("single record at start_offset should pass");
    }

    #[test]
    fn check_contiguous_records() {
        let records = vec![make_record(1), make_record(2), make_record(3)];
        check_contiguous(&records, 1).expect("contiguous records from start_offset should pass");
    }

    #[test]
    fn check_contiguous_detects_interior_gap() {
        let records = vec![make_record(1), make_record(2), make_record(5)];
        let err = check_contiguous(&records, 1).expect_err("should detect hole between 2 and 5");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 3, "expected offset 3 after offset 2");
                assert_eq!(found, 5, "found offset 5 instead of 3");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[test]
    fn check_contiguous_detects_first_interior_gap() {
        let records = vec![make_record(1), make_record(3), make_record(7)];
        let err =
            check_contiguous(&records, 1).expect_err("should detect first hole between 1 and 3");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 2, "expected offset 2 after offset 1");
                assert_eq!(found, 3, "found offset 3 instead of 2");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[test]
    fn check_contiguous_detects_leading_gap() {
        let records = vec![make_record(5), make_record(6), make_record(7)];
        let err = check_contiguous(&records, 3).expect_err("should detect missing leading records");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 3, "expected start_offset 3");
                assert_eq!(found, 5, "found offset 5 instead of 3");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[test]
    fn check_contiguous_single_record_leading_gap() {
        let records = vec![make_record(10)];
        let err = check_contiguous(&records, 7)
            .expect_err("single record not at start_offset should fail");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 7, "expected start_offset 7");
                assert_eq!(found, 10, "found offset 10 instead of 7");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn fetch_records_empty_pointers_nonempty_range() {
        let (_tmp, storage) = chroma_storage::test_storage();
        let fetcher = Arc::new(FragmentFetcher::new_for_test(storage));
        let err = fetcher
            .fetch_records(&[], 5, 10, 10)
            .await
            .expect_err("empty pointers with start < limit should be a hole");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 5, "expected start_offset 5");
                assert_eq!(found, 10, "found limit_offset 10");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn fetch_records_empty_pointers_empty_range() {
        let (_tmp, storage) = chroma_storage::test_storage();
        let fetcher = Arc::new(FragmentFetcher::new_for_test(storage));
        let records = fetcher
            .fetch_records(&[], 5, 5, 10)
            .await
            .expect("empty pointers with start == limit should succeed");
        assert!(records.is_empty(), "should return no records");
    }

    #[tokio::test]
    async fn fetch_records_rejects_non_contiguous_fragments() {
        let (_tmp, storage) = chroma_storage::test_storage();
        let storage_prefix = "tenant/database/collection";
        let path1 = write_fragment(&storage, storage_prefix, 1, 5, &[5, 6]).await;
        let path2 = write_fragment(&storage, storage_prefix, 2, 8, &[8, 9]).await;
        let fetcher = Arc::new(FragmentFetcher::new_for_test(storage));
        let pointers = vec![
            FragmentPointer {
                path: path1,
                start_offset: 5,
                limit_offset: 7,
                num_bytes: 0,
                storage_prefix: storage_prefix.to_string(),
                absolute_offsets: true,
            },
            FragmentPointer {
                path: path2,
                start_offset: 8,
                limit_offset: 10,
                num_bytes: 0,
                storage_prefix: storage_prefix.to_string(),
                absolute_offsets: true,
            },
        ];

        let err = fetcher
            .fetch_records(&pointers, 5, 10, 2)
            .await
            .expect_err("fragment fetch should reject stitched records with gaps");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 7, "expected the first missing offset");
                assert_eq!(found, 8, "found the next available offset after the gap");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn fetch_records_accepts_contiguous_fragments_out_of_order() {
        let (_tmp, storage) = chroma_storage::test_storage();
        let storage_prefix = "tenant/database/collection";
        let path1 = write_fragment(&storage, storage_prefix, 1, 5, &[5, 6]).await;
        let path2 = write_fragment(&storage, storage_prefix, 2, 7, &[7, 8, 9]).await;
        let fetcher = Arc::new(FragmentFetcher::new_for_test(storage));
        let pointers = vec![
            FragmentPointer {
                path: path2,
                start_offset: 7,
                limit_offset: 10,
                num_bytes: 0,
                storage_prefix: storage_prefix.to_string(),
                absolute_offsets: true,
            },
            FragmentPointer {
                path: path1,
                start_offset: 5,
                limit_offset: 7,
                num_bytes: 0,
                storage_prefix: storage_prefix.to_string(),
                absolute_offsets: true,
            },
        ];

        let records = fetcher
            .fetch_records(&pointers, 5, 10, 2)
            .await
            .expect("contiguous fragments should fetch successfully");

        assert_eq!(
            records.iter().map(|record| record.log_offset).collect::<Vec<_>>(),
            vec![5, 6, 7, 8, 9],
            "records should be sorted and contiguous across fragment boundaries"
        );
    }
}
