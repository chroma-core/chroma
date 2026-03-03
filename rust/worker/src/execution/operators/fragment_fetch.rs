use std::sync::Arc;

use chroma_cache::{Cache, CacheConfig, CacheError, Weighted};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{GetOptions, Storage, StorageError};
use chroma_types::chroma_proto;
use chroma_types::{LogRecord, OperationRecord, RecordConversionError};
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
}

impl From<chroma_proto::LogFragmentPointer> for FragmentPointer {
    fn from(proto: chroma_proto::LogFragmentPointer) -> Self {
        Self {
            path: proto.path,
            start_offset: proto.start_offset,
            limit_offset: proto.limit_offset,
            num_bytes: proto.num_bytes,
            storage_prefix: proto.storage_prefix,
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
    #[error("Proto decode error for record at offset {offset}: {source}")]
    ProtoDecode {
        offset: u64,
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
    /// and returned sorted by log_offset.
    pub async fn fetch_records(
        &self,
        pointers: &[FragmentPointer],
        start_offset: u64,
        limit_offset: u64,
    ) -> Result<Vec<LogRecord>, FragmentFetchError> {
        let mut all_records = Vec::new();
        for pointer in pointers {
            let records = self
                .fetch_fragment(pointer, start_offset, limit_offset)
                .await?;
            all_records.extend(records);
        }
        all_records.sort_by_key(|r| r.log_offset);
        check_for_holes(&all_records)?;
        Ok(all_records)
    }

    /// Fetch a single fragment from storage, using the cache if available.
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

        let starting_position = LogPosition::from_offset(pointer.start_offset);
        let (parsed_records, _num_bytes, _now_us) =
            wal3::interfaces::s3::parse_parquet_fast(&bytes, Some(starting_position)).await?;

        let mut records = Vec::new();
        for (log_position, record_bytes) in parsed_records {
            let offset = log_position.offset();
            if offset < start_offset || offset >= limit_offset {
                continue;
            }
            let proto_op_record = chroma_proto::OperationRecord::decode(record_bytes.as_slice())
                .map_err(|e| FragmentFetchError::ProtoDecode { offset, source: e })?;
            let record: OperationRecord = proto_op_record.try_into()?;
            records.push(LogRecord {
                log_offset: offset as i64,
                record,
            });
        }
        Ok(records)
    }
}

/// Verify that a sorted slice of log records has no gaps in offsets.
fn check_for_holes(records: &[LogRecord]) -> Result<(), FragmentFetchError> {
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
    use super::{check_for_holes, FragmentFetchError, FragmentPointer};
    use chroma_types::{LogRecord, Operation, OperationRecord};

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

    #[test]
    fn fragment_pointer_from_proto() {
        use chroma_types::chroma_proto::LogFragmentPointer;
        let proto = LogFragmentPointer {
            path: "log/Bucket=0/FragmentSeqNo=0000000000000001.parquet".to_string(),
            start_offset: 0,
            limit_offset: 10,
            num_bytes: 1024,
            storage_prefix: "tenant/database/collection".to_string(),
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
    }

    #[test]
    fn check_for_holes_empty() {
        check_for_holes(&[]).expect("empty records should have no holes");
    }

    #[test]
    fn check_for_holes_single_record() {
        let records = vec![make_record(5)];
        check_for_holes(&records).expect("single record should have no holes");
    }

    #[test]
    fn check_for_holes_contiguous() {
        let records = vec![make_record(1), make_record(2), make_record(3)];
        check_for_holes(&records).expect("contiguous records should have no holes");
    }

    #[test]
    fn check_for_holes_detects_gap() {
        let records = vec![make_record(1), make_record(2), make_record(5)];
        let err = check_for_holes(&records).expect_err("should detect hole between 2 and 5");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 3, "expected offset 3 after offset 2");
                assert_eq!(found, 5, "found offset 5 instead of 3");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }

    #[test]
    fn check_for_holes_detects_first_gap() {
        let records = vec![make_record(1), make_record(3), make_record(7)];
        let err = check_for_holes(&records).expect_err("should detect first hole between 1 and 3");
        match err {
            FragmentFetchError::HoleInLog { expected, found } => {
                assert_eq!(expected, 2, "expected offset 2 after offset 1");
                assert_eq!(found, 3, "found offset 3 instead of 2");
            }
            other => panic!("expected HoleInLog error, got: {:?}", other),
        }
    }
}
