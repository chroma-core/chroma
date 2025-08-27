#![doc = include_str!("../README.md")]

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use setsum::Setsum;

mod backoff;
mod batch_manager;
mod copy;
mod cursors;
mod destroy;
mod gc;
mod manifest;
mod manifest_manager;
mod reader;
mod snapshot_cache;
mod writer;

pub use backoff::ExponentialBackoff;
pub use batch_manager::BatchManager;
pub use copy::copy;
pub use cursors::{Cursor, CursorName, CursorStore, Witness};
pub use destroy::destroy;
pub use gc::{Garbage, GarbageCollector};
pub use manifest::{
    unprefixed_snapshot_path, Manifest, ManifestAndETag, Snapshot, SnapshotPointer,
};
pub use manifest_manager::ManifestManager;
pub use reader::{Limits, LogReader};
pub use snapshot_cache::SnapshotCache;
pub use writer::{upload_parquet, LogWriter, MarkDirty};

/////////////////////////////////////////////// Error //////////////////////////////////////////////

/// Error captures the different error conditions that can occur when interacting with the log.
#[derive(Clone, Debug, Default, thiserror::Error)]
pub enum Error {
    #[default]
    #[error("success")]
    Success,
    #[error("uninitialized log")]
    UninitializedLog,
    #[error("already initialized log")]
    AlreadyInitialized,
    #[error("scanned region is garbage collected")]
    GarbageCollected,
    // NOTE(rescrv):  Durable means the contention occurs only on the manifest.  A manifest-scoped
    // op may return this case and assume higher ups translate it to failure.  This is hypothetical
    // and a note to the future because in the code as of this comment the data is always durable
    // before any manifest contention can happen.
    //
    // There are three cases:
    // - The operation does not need to be retried because it is durable, but we need to internally
    //   propagate state to correct for the log contention.
    // - The operation needs to be retried because there was explicit contention writing the
    //   fragement.  We need to retry, but can return this error to the user.
    // - The operation is in an ambiguous state and we cannot advise the user either way.  Fail the
    //   write and let a higher level protocol handle it.
    //
    // By observation, manifest contention on the write path is always LogContentionDurable.  If
    // you change the write path you need to audit where it gets returned.
    #[error("log contention, but your data is durable")]
    LogContentionDurable,
    #[error("log contention, and your operation may be retried")]
    LogContentionRetry,
    #[error("log contention, and your data may or may not be durable")]
    LogContentionFailure,
    #[error("the log is full")]
    LogFull,
    #[error("the log is closed")]
    LogClosed,
    #[error("an empty batch was passed to append")]
    EmptyBatch,
    #[error("perform exponential backoff and retry")]
    Backoff,
    #[error("an internal, otherwise unclassifiable error")]
    Internal,
    #[error("could not find FSN in path: {0}")]
    MissingFragmentSequenceNumber(String),
    #[error("corrupt manifest: {0}")]
    CorruptManifest(String),
    #[error("corrupt fragment: {0}")]
    CorruptFragment(String),
    #[error("corrupt cursor: {0}")]
    CorruptCursor(String),
    #[error("corrupt garbage: {0}")]
    CorruptGarbage(String),
    #[error("missing cursor: {0}")]
    NoSuchCursor(String),
    #[error("garbage collection: {0}")]
    GarbageCollection(String),
    #[error("garbage collection precondition failed: manifest missing this: {0}")]
    GarbageCollectionPrecondition(SnapshotPointerOrFragmentSeqNo),
    #[error("scrub error: {0}")]
    ScrubError(#[from] Box<ScrubError>),
    #[error("parquet error: {0}")]
    ParquetError(#[from] Arc<parquet::errors::ParquetError>),
    #[error("storage error: {0}")]
    StorageError(#[from] Arc<chroma_storage::StorageError>),
}

impl chroma_error::ChromaError for Error {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            Self::Success => chroma_error::ErrorCodes::Success,
            Self::UninitializedLog => chroma_error::ErrorCodes::FailedPrecondition,
            Self::AlreadyInitialized => chroma_error::ErrorCodes::AlreadyExists,
            Self::GarbageCollected => chroma_error::ErrorCodes::NotFound,
            Self::LogContentionDurable => chroma_error::ErrorCodes::Aborted,
            Self::LogContentionRetry => chroma_error::ErrorCodes::Aborted,
            Self::LogContentionFailure => chroma_error::ErrorCodes::Aborted,
            Self::LogFull => chroma_error::ErrorCodes::Aborted,
            Self::LogClosed => chroma_error::ErrorCodes::FailedPrecondition,
            Self::EmptyBatch => chroma_error::ErrorCodes::InvalidArgument,
            Self::Backoff => chroma_error::ErrorCodes::Unavailable,
            Self::Internal => chroma_error::ErrorCodes::Internal,
            Self::MissingFragmentSequenceNumber(_) => chroma_error::ErrorCodes::Internal,
            Self::CorruptManifest(_) => chroma_error::ErrorCodes::DataLoss,
            Self::CorruptFragment(_) => chroma_error::ErrorCodes::DataLoss,
            Self::CorruptCursor(_) => chroma_error::ErrorCodes::DataLoss,
            Self::CorruptGarbage(_) => chroma_error::ErrorCodes::DataLoss,
            Self::NoSuchCursor(_) => chroma_error::ErrorCodes::Unknown,
            Self::GarbageCollection(_) => chroma_error::ErrorCodes::Unknown,
            Self::GarbageCollectionPrecondition(_) => chroma_error::ErrorCodes::FailedPrecondition,
            Self::ScrubError(_) => chroma_error::ErrorCodes::DataLoss,
            Self::ParquetError(_) => chroma_error::ErrorCodes::Unknown,
            Self::StorageError(storage) => storage.code(),
        }
    }
}

///////////////////////////////////// SnapshotPointerOrFragment ////////////////////////////////////

#[derive(Clone, Debug)]
pub enum SnapshotPointerOrFragmentSeqNo {
    SnapshotPointer(SnapshotPointer),
    FragmentSeqNo(u64),
    Stringy(String),
}

impl From<SnapshotPointer> for SnapshotPointerOrFragmentSeqNo {
    fn from(inner: SnapshotPointer) -> Self {
        Self::SnapshotPointer(inner)
    }
}

impl From<u64> for SnapshotPointerOrFragmentSeqNo {
    fn from(inner: u64) -> Self {
        Self::FragmentSeqNo(inner)
    }
}

impl std::fmt::Display for SnapshotPointerOrFragmentSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::SnapshotPointer(ptr) => write!(f, "Snapshot({:?})", ptr.path_to_snapshot),
            Self::FragmentSeqNo(seq) => write!(f, "Fragment({})", *seq),
            Self::Stringy(s) => write!(f, "Stringy({s})"),
        }
    }
}

/////////////////////////////////////////// ScrubSuccess ///////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScrubSuccess {
    pub calculated_setsum: Setsum,
    pub bytes_read: u64,
    pub short_read: bool,
}

//////////////////////////////////////////// ScrubError ////////////////////////////////////////////

#[derive(Clone, Debug, thiserror::Error)]
pub enum ScrubError {
    #[error("CorruptManifest: {what}")]
    CorruptManifest { manifest: String, what: String },
    #[error("CorruptFragment: {seq_no} {what}")]
    CorruptFragment {
        manifest: String,
        seq_no: FragmentSeqNo,
        what: String,
    },
    #[error("MismatchedPath: {reference:?} expected {:?} got {:?}", reference.path, empirical.path)]
    MismatchedPath {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MismatchedSeqNo: {reference:?} expected {:?} got {:?}", reference.seq_no, empirical.seq_no)]
    MismatchedSeqNo {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MismatchedNumBytes: {reference:?} expected {:?} got {:?}", reference.num_bytes, empirical.num_bytes)]
    MismatchedNumBytes {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MismatchedStart: {reference:?} expected {:?} got {:?}", reference.start, empirical.start)]
    MismatchedStart {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MismatchedLimit: {reference:?} expected {:?} got {:?}", reference.limit, empirical.limit)]
    MismatchedLimit {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MismatchedSnapshotSetsum: {reference:?} expected {} got {}", reference.setsum.hexdigest(), empirical.setsum.hexdigest())]
    MismatchedSnapshotSetsum {
        reference: SnapshotPointer,
        empirical: Snapshot,
    },
    #[error("MismatchedFragmentSetsum: {reference:?} expected {} got {}", reference.setsum.hexdigest(), empirical.setsum.hexdigest())]
    MismatchedFragmentSetsum {
        reference: Fragment,
        empirical: Fragment,
    },
    #[error("MissingFragment: {reference:?}")]
    MissingFragment { reference: Fragment },
    #[error("MissingFragmentBySetsumPath: {setsum:?} {path:?}")]
    MissingFragmentBySetsumPath { setsum: Setsum, path: String },
    #[error("MissingSnapshotBySetsumPath: {setsum:?} {path:?}")]
    MissingSnapshotBySetsumPath { setsum: Setsum, path: String },
    #[error("MissingSnapshot: {reference:?}")]
    MissingSnapshot { reference: SnapshotPointer },
    #[error("Garbage: expected: {0}")]
    CorruptGarbage(String),
    #[error("Corrupt snapshot replace")]
    CorruptSnapshotDrop { lhs: Setsum, rhs: Setsum },
    #[error("Corrupt snapshot replace")]
    CorruptSnapshotReplace {
        old_snapshot_setsum: Setsum,
        new_snapshot_setsum: Setsum,
        dropped: Setsum,
    },
    #[error("Internal error within scrubbing: {0}")]
    Internal(String),
    #[error("OverallMismatch: {manifest:?} {observed:?}")]
    OverallMismatch {
        manifest: ScrubSuccess,
        observed: ScrubSuccess,
    },
    #[error("The given snapshot rolls up to nothing with garbage")]
    ReplaceDroppedEverything { snapshot: SnapshotPointer },
}

//////////////////////////////////////////// LogPosition ///////////////////////////////////////////

/// A log position is a pair of an offset and a timestamp.  Every record has a unique log position.
/// A LogPosition only implements equality, which checks both offset and timestamp_us.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct LogPosition {
    /// The offset field of a LogPosition is a strictly increasing timestamp.  It has no gaps and
    /// spans [0, u64::MAX).
    offset: u64,
}

impl LogPosition {
    pub const MAX: LogPosition = LogPosition { offset: u64::MAX };
    pub const MIN: LogPosition = LogPosition { offset: u64::MIN };

    /// Create a new log position from offset and current time.
    pub fn from_offset(offset: u64) -> Self {
        LogPosition { offset }
    }

    /// The offset of the LogPosition.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// True iff this contains offset.
    pub fn contains_offset(start: LogPosition, end: LogPosition, offset: u64) -> bool {
        start.offset <= offset && offset < end.offset
    }
}

impl std::ops::Add<u64> for LogPosition {
    type Output = LogPosition;

    fn add(self, rhs: u64) -> Self::Output {
        LogPosition {
            offset: self.offset.wrapping_add(rhs),
        }
    }
}

impl std::ops::Add<usize> for LogPosition {
    type Output = LogPosition;

    fn add(self, rhs: usize) -> Self::Output {
        LogPosition {
            offset: self.offset.wrapping_add(rhs as u64),
        }
    }
}

impl std::ops::Sub<u64> for LogPosition {
    type Output = LogPosition;

    fn sub(self, rhs: u64) -> Self::Output {
        LogPosition {
            offset: self.offset.wrapping_sub(rhs),
        }
    }
}

impl std::ops::Sub<LogPosition> for LogPosition {
    type Output = u64;

    fn sub(self, rhs: LogPosition) -> Self::Output {
        self.offset - rhs.offset
    }
}

impl std::ops::AddAssign<usize> for LogPosition {
    fn add_assign(&mut self, rhs: usize) {
        *self = *self + rhs;
    }
}

////////////////////////////////////////// ThrottleOptions /////////////////////////////////////////

/// ThrottleOptions control admission to S3 and batch size/interval.
///
/// These are per logical grouping in S3 (which maps to a prefix), so they can be set differently
/// for different prefixes.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ThrottleOptions {
    /// The maximum number of bytes to batch.  Defaults to 64MB (2 * GRPC max payload size).
    #[serde(default = "ThrottleOptions::default_batch_size_bytes")]
    pub batch_size_bytes: usize,
    /// The maximum number of microseconds to batch.  Defaults to 100ms or 100_000us.
    #[serde(default = "ThrottleOptions::default_batch_interval_us")]
    pub batch_interval_us: usize,
    /// The maximum number of operations per second to allow.  Defaults to 2_000.
    #[serde(default = "ThrottleOptions::default_throughput")]
    pub throughput: usize,
    /// The number of operations per second to reserve for backoff/retry.  Defaults to 1_500.
    #[serde(default = "ThrottleOptions::default_headroom")]
    pub headroom: usize,
}

impl ThrottleOptions {
    fn default_batch_size_bytes() -> usize {
        64_000_000
    }

    fn default_batch_interval_us() -> usize {
        100_000
    }

    fn default_throughput() -> usize {
        2_000
    }

    fn default_headroom() -> usize {
        1_500
    }
}

impl Default for ThrottleOptions {
    fn default() -> Self {
        ThrottleOptions {
            // Batch for at least 20ms.
            batch_interval_us: Self::default_batch_interval_us(),
            // Set a batch size of 8MB.
            batch_size_bytes: Self::default_batch_size_bytes(),
            // Set a throughput that's approximately 5/7th the throughput of the throughput S3
            // allows.  If we hit throttle errors at this throughput we have a case for support.
            throughput: Self::default_throughput(),
            // How much headroom we have for retries.
            headroom: Self::default_headroom(),
        }
    }
}

impl From<ThrottleOptions> for ExponentialBackoff {
    fn from(options: ThrottleOptions) -> Self {
        ExponentialBackoff::new(options.throughput as f64, options.headroom as f64)
    }
}

////////////////////////////////////////// SnapshotOptions /////////////////////////////////////////

/// SnapshotOptions control the size of snapshots and manifests.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotOptions {
    /// The maximum number of outbound snapshot pointers to embed in a snapshot or manifest.
    #[serde(default = "SnapshotOptions::default_snapshot_rollover_threshold")]
    pub snapshot_rollover_threshold: usize,
    /// The maximum number of fragment pointers to embed in a snapshot or manifest.
    #[serde(default = "SnapshotOptions::default_fragment_rollover_threshold")]
    pub fragment_rollover_threshold: usize,
}

impl SnapshotOptions {
    /// Corresponds to [SnapshotOptions::snapshot_rollover_threshold], or the number of snapshot
    /// pointers to embed in a snapshot or manifest.
    fn default_snapshot_rollover_threshold() -> usize {
        (1 << 18) / SnapshotPointer::JSON_SIZE_ESTIMATE
    }

    /// Corresponds to [SnapshotOptions::fragment_rollover_threshold], or the number of fragment
    /// pointers to embed in a snapshot or manifest.
    fn default_fragment_rollover_threshold() -> usize {
        (1 << 19) / Fragment::JSON_SIZE_ESTIMATE
    }
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        SnapshotOptions {
            snapshot_rollover_threshold: Self::default_snapshot_rollover_threshold(),
            fragment_rollover_threshold: Self::default_fragment_rollover_threshold(),
        }
    }
}

///////////////////////////////////////// LogWriterOptions /////////////////////////////////////////

/// LogWriterOptions control the behavior of the log writer.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogWriterOptions {
    /// Default throttling options for fragments.
    #[serde(default)]
    pub throttle_fragment: ThrottleOptions,
    /// Default throttling options for manifest.
    #[serde(default)]
    pub throttle_manifest: ThrottleOptions,
    /// Default snapshot options for manifest.
    #[serde(default)]
    pub snapshot_manifest: SnapshotOptions,
}

///////////////////////////////////////// LogReaderOptions /////////////////////////////////////////

/// LogReaderOptions control the behavior of the log writer.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogReaderOptions {
    /// Default throttling options for manifest.
    #[serde(default)]
    pub throttle: ThrottleOptions,
}

//////////////////////////////////////// CursorStoreOptions ////////////////////////////////////////

/// CursorStoreOptions control the behavior of the cursor store.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CursorStoreOptions {
    /// Number of concurrent cursor operations per cursor store.
    #[serde(default = "CursorStoreOptions::default_concurrency")]
    pub concurrency: usize,
}

impl CursorStoreOptions {
    /// Default concurrency for cursor store operations.
    fn default_concurrency() -> usize {
        10
    }
}

impl Default for CursorStoreOptions {
    fn default() -> Self {
        Self {
            concurrency: Self::default_concurrency(),
        }
    }
}

///////////////////////////////////// GarbageCollectionOptions /////////////////////////////////////

/// GarbageCollectionOptions control the behavior of garbage collection.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GarbageCollectionOptions {
    /// Default throttling options for deletes.
    #[serde(default)]
    pub throttle: ThrottleOptions,
}

/////////////////////////////////////////// FragmentSeqNo //////////////////////////////////////////

/// A FragmentSeqNo is an identifier that corresponds to the the number of fragments that have been
/// issued prior to the segment with this FragmentSeqNo.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct FragmentSeqNo(pub u64);

impl FragmentSeqNo {
    const BEGIN: FragmentSeqNo = FragmentSeqNo(1);

    /// Returns the successor of this FragmentSeqNo, or None if this FragmentSeqNo is the maximum
    pub fn successor(&self) -> Option<Self> {
        if self.0 == u64::MAX {
            None
        } else {
            Some(FragmentSeqNo(self.0 + 1))
        }
    }

    // Round down to the nearest multiple of 5k.
    pub fn bucket(&self) -> u64 {
        (self.0 / 4_096) * 4_096
    }
}

impl std::fmt::Display for FragmentSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Add<FragmentSeqNo> for u64 {
    type Output = FragmentSeqNo;

    fn add(self, rhs: FragmentSeqNo) -> Self::Output {
        FragmentSeqNo(self.wrapping_add(rhs.0))
    }
}

impl std::ops::Add<u64> for FragmentSeqNo {
    type Output = FragmentSeqNo;

    fn add(self, rhs: u64) -> Self::Output {
        FragmentSeqNo(self.0.wrapping_add(rhs))
    }
}

impl std::ops::Sub<FragmentSeqNo> for FragmentSeqNo {
    type Output = u64;

    fn sub(self, rhs: FragmentSeqNo) -> Self::Output {
        self.0.wrapping_sub(rhs.0)
    }
}

impl std::ops::AddAssign<u64> for FragmentSeqNo {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self.0.wrapping_add(rhs);
    }
}

///////////////////////////////////////////// Fragment /////////////////////////////////////////////

/// A Fragment is an immutable piece of the log containing adjacent writes.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Fragment {
    pub path: String,
    pub seq_no: FragmentSeqNo,
    pub start: LogPosition,
    pub limit: LogPosition,
    pub num_bytes: u64,
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub setsum: Setsum,
}

impl Fragment {
    /// An estimate on the number of bytes required to serialize this object as JSON.
    pub const JSON_SIZE_ESTIMATE: usize = 256;

    pub fn possibly_contains_position(&self, position: LogPosition) -> bool {
        LogPosition::contains_offset(self.start, self.limit, position.offset)
    }
}

/////////////////////////////////////////////// util ///////////////////////////////////////////////

fn deserialize_setsum<'de, D>(deserializer: D) -> Result<Setsum, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Setsum::from_hexdigest(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("invalid setsum: {}", s)))
}

fn serialize_setsum<S>(setsum: &Setsum, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let s = setsum.hexdigest();
    s.serialize(serializer)
}

////////////////////////////////////////// Fragment Paths //////////////////////////////////////////

pub fn fragment_prefix() -> String {
    "log/".to_string()
}

pub fn prefixed_fragment_path(prefix: &str, fragment_seq_no: FragmentSeqNo) -> String {
    format!(
        "{}/{}Bucket={:016x}/FragmentSeqNo={:016x}.parquet",
        prefix,
        fragment_prefix(),
        fragment_seq_no.bucket(),
        fragment_seq_no.0,
    )
}

pub fn unprefixed_fragment_path(fragment_seq_no: FragmentSeqNo) -> String {
    format!(
        "log/Bucket={:016x}/FragmentSeqNo={:016x}.parquet",
        fragment_seq_no.bucket(),
        fragment_seq_no.0,
    )
}

pub fn parse_fragment_path(path: &str) -> Option<FragmentSeqNo> {
    // FragmentSeqNo is always in the basename.
    let (_, basename) = path.rsplit_once('/')?;
    let fsn_equals_number = basename.strip_suffix(".parquet")?;
    let number = fsn_equals_number.strip_prefix("FragmentSeqNo=")?;
    u64::from_str_radix(number, 16).ok().map(FragmentSeqNo)
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!(
            "THIS_IS_THE_COLLECTION/log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet",
            prefixed_fragment_path("THIS_IS_THE_COLLECTION", FragmentSeqNo(1))
        );
    }
}
