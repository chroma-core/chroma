#![doc = include_str!("../README.md")]

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-export Setsum publicly for downstream crates
pub use setsum::Setsum;

mod backoff;
mod copy;
mod cursors;
mod destroy;
mod gc;
pub mod interfaces;
mod manifest;
mod quorum_writer;
mod reader;
mod snapshot_cache;
mod writer;

pub use backoff::ExponentialBackoff;
pub use copy::copy;
pub use cursors::{Cursor, CursorName, CursorStore, CursorWitness};
pub use destroy::destroy;
pub use gc::{Garbage, GarbageCollector};
pub use interfaces::repl::{
    create_repl_factories, ReplicatedFragmentManagerFactory, ReplicatedFragmentOptions,
    ReplicatedManifestManagerFactory, StorageWrapper,
};
pub use interfaces::s3::{
    create_s3_factories, upload_parquet, ManifestManager, ManifestReader, S3FragmentManagerFactory,
    S3FragmentPuller, S3FragmentUploader, S3ManifestManagerFactory,
};
pub use interfaces::{
    BatchManager, FragmentConsumer, FragmentManagerFactory, FragmentPointer, FragmentPublisher,
    FragmentUploader, ManifestConsumer, ManifestManagerFactory, ManifestPublisher, ManifestWitness,
    PositionWitness,
};
pub use manifest::{
    unprefixed_snapshot_path, Manifest, ManifestAndWitness, Snapshot, SnapshotPointer,
};
pub use quorum_writer::write_quorum;
pub use reader::{scan_from_manifest, Limits, LogReader};
pub use snapshot_cache::SnapshotCache;
pub use writer::{now_micros, LogWriter, MarkDirty};

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
    #[error("an internal, otherwise unclassifiable error ({file}:{line})")]
    Internal { file: String, line: u32 },
    #[error("could not find FSN in path: {0}")]
    MissingFragmentSequenceNumber(String),
    #[error("manifest missing for log {log_id}")]
    ManifestMissing { log_id: Uuid },
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
    GarbageCollectionPrecondition(SnapshotPointerOrFragmentIdentifier),
    #[error("scrub error: {0}")]
    ScrubError(#[from] Box<ScrubError>),
    #[error("parquet error: {0}")]
    ParquetError(#[from] Arc<parquet::errors::ParquetError>),
    #[error("storage error: {0}")]
    StorageError(#[from] Arc<chroma_storage::StorageError>),
    #[error("overflow error: {0}")]
    Overflow(String),
    #[error("replication error:  failed to replicate")]
    ReplicationError,
    #[error("replication consistency error: {0}")]
    ReplicationConsistencyError(String),
    #[error("replication batch too large: {messages_len} messages exceeds limit {limit}")]
    ReplicationBatchTooLarge { messages_len: u64, limit: u64 },
    #[error("spanner-related error: {0}")]
    SpannerError(#[from] Arc<google_cloud_spanner::client::Error>),
    #[error("spanner-row-related error: {0}")]
    SpannerRowError(#[from] Arc<google_cloud_spanner::row::Error>),
    #[error("spanner-session-related error: {0}")]
    SpannerSessionError(#[from] Arc<google_cloud_spanner::session::SessionError>),
    #[error("tonic-related error: {0}")]
    TonicError(#[from] tonic::Status),
}

impl Error {
    pub fn internal(file: impl Into<String>, line: impl Into<u32>) -> Self {
        let file = file.into();
        let line = line.into();
        Self::Internal { file, line }
    }
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
            Self::Internal { .. } => chroma_error::ErrorCodes::Internal,
            Self::MissingFragmentSequenceNumber(_) => chroma_error::ErrorCodes::Internal,
            Self::ManifestMissing { .. } => chroma_error::ErrorCodes::NotFound,
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
            Self::Overflow(_) => chroma_error::ErrorCodes::InvalidArgument,
            Self::ReplicationError => chroma_error::ErrorCodes::Unavailable,
            Self::ReplicationConsistencyError(_) => chroma_error::ErrorCodes::DataLoss,
            Self::ReplicationBatchTooLarge { .. } => chroma_error::ErrorCodes::InvalidArgument,
            Self::SpannerError(_) => chroma_error::ErrorCodes::Internal,
            Self::SpannerRowError(_) => chroma_error::ErrorCodes::Internal,
            Self::SpannerSessionError(_) => chroma_error::ErrorCodes::Internal,
            Self::TonicError(t) => t.code().into(),
        }
    }
}

impl From<google_cloud_spanner::client::Error> for Error {
    fn from(err: google_cloud_spanner::client::Error) -> Self {
        Self::SpannerError(Arc::new(err))
    }
}

impl From<google_cloud_spanner::row::Error> for Error {
    fn from(err: google_cloud_spanner::row::Error) -> Self {
        Self::SpannerRowError(Arc::new(err))
    }
}

impl From<google_cloud_spanner::session::SessionError> for Error {
    fn from(err: google_cloud_spanner::session::SessionError) -> Self {
        Self::SpannerSessionError(Arc::new(err))
    }
}

impl google_cloud_gax::retry::TryAs<tonic::Status> for Error {
    fn try_as(&self) -> Option<&tonic::Status> {
        if let Self::TonicError(t) = self {
            Some(t)
        } else {
            None
        }
    }
}

///////////////////////////////////// SnapshotPointerOrFragment ////////////////////////////////////

#[derive(Clone, Debug)]
pub enum SnapshotPointerOrFragmentIdentifier {
    SnapshotPointer(SnapshotPointer),
    FragmentIdentifier(FragmentIdentifier),
    Stringy(String),
}

impl From<SnapshotPointer> for SnapshotPointerOrFragmentIdentifier {
    fn from(inner: SnapshotPointer) -> Self {
        Self::SnapshotPointer(inner)
    }
}

impl From<FragmentIdentifier> for SnapshotPointerOrFragmentIdentifier {
    fn from(inner: FragmentIdentifier) -> Self {
        Self::FragmentIdentifier(inner)
    }
}

impl std::fmt::Display for SnapshotPointerOrFragmentIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::SnapshotPointer(ptr) => write!(f, "Snapshot({:?})", ptr.path_to_snapshot),
            Self::FragmentIdentifier(ident) => write!(f, "Fragment({ident})"),
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
        seq_no: FragmentIdentifier,
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

/// A FragmentSeqNo is a form of strongly-typed identifier for sequence numbers that identify
/// fragments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct FragmentSeqNo(u64);

impl PartialOrd for FragmentSeqNo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FragmentSeqNo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl FragmentSeqNo {
    /// The first fragment sequence number.
    pub const BEGIN: FragmentSeqNo = FragmentSeqNo(1);

    /// The zero fragment sequence number (before any fragments).
    pub const ZERO: FragmentSeqNo = FragmentSeqNo(0);

    /// Creates a new FragmentSeqNo from a u64.
    pub fn from_u64(value: u64) -> Self {
        FragmentSeqNo(value)
    }

    /// Returns the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Returns the successor of this FragmentSeqNo, or None if this is the maximum.
    pub fn successor(&self) -> Option<Self> {
        if self.0 == u64::MAX {
            None
        } else {
            Some(FragmentSeqNo(self.0 + 1))
        }
    }

    /// Round down to the nearest multiple of 4k.
    pub fn bucket(&self) -> u64 {
        (self.0 / 4_096) * 4_096
    }
}

impl std::fmt::Display for FragmentSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/////////////////////////////////////////// FragmentUuid ///////////////////////////////////////////

/// A FragmentUuid is a form of strongly-typed identifier for uuids that identify fragments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct FragmentUuid(Uuid);

impl FragmentUuid {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }
}

impl PartialOrd for FragmentUuid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FragmentUuid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl FragmentUuid {
    /// Creates a new FragmentUuid from a Uuid.
    pub fn from_uuid(uuid: Uuid) -> Self {
        FragmentUuid(uuid)
    }

    /// Returns the underlying UUID as a little-endian u128.
    pub fn as_u128_le(&self) -> u128 {
        self.0.to_u128_le()
    }
}

impl std::fmt::Display for FragmentUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

//////////////////////////////////////// FragmentIdentifier ////////////////////////////////////////

/// A FragmentIdentifier uniquely identifies a fragment within a log.
///
/// There are two variants:
/// - `SeqNo(u64)`: A sequential number assigned in order of fragment creation. This variant
///   supports operations like `successor()` and range-based garbage collection.
/// - `Uuid(Uuid)`: A universally unique identifier. UUIDs are ordered by their byte
///   representation, which for UUID v7 corresponds to temporal ordering.
///
/// A manifest must contain fragments of only one variant type (enforced by scrubbing).
///
/// Ordering: `SeqNo` variants compare by their inner u64. `Uuid` variants compare by their byte
/// representation. Cross-variant comparison orders all `SeqNo` values before all `Uuid` values,
/// but this should not occur in practice since manifests enforce uniformity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(untagged)]
pub enum FragmentIdentifier {
    /// Sequential fragment identifier. Supports successor() and range-based operations.
    SeqNo(FragmentSeqNo),
    /// UUID-based fragment identifier. Ordered by byte representation.
    Uuid(FragmentUuid),
}

impl FragmentIdentifier {
    /// The first fragment identifier.
    pub const BEGIN: FragmentIdentifier = FragmentIdentifier::SeqNo(FragmentSeqNo::BEGIN);

    /// Returns the inner u64 for SeqNo variants, or None for Uuid variants.
    pub fn as_seq_no(&self) -> Option<FragmentSeqNo> {
        match self {
            FragmentIdentifier::SeqNo(x) => Some(*x),
            FragmentIdentifier::Uuid(_) => None,
        }
    }

    /// Returns the inner Uuid for Uuid variants, or None for SeqNo variants.
    pub fn as_uuid(&self) -> Option<FragmentUuid> {
        match self {
            FragmentIdentifier::SeqNo(_) => None,
            FragmentIdentifier::Uuid(u) => Some(*u),
        }
    }

    /// Returns the successor of this FragmentIdentifier, or None if this is the maximum or is a
    /// Uuid.
    pub fn successor(&self) -> Option<Self> {
        match self {
            FragmentIdentifier::SeqNo(x) => x.successor().map(FragmentIdentifier::SeqNo),
            FragmentIdentifier::Uuid(_) => None,
        }
    }
}

impl std::fmt::Display for FragmentIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FragmentIdentifier::SeqNo(x) => write!(f, "{x}"),
            FragmentIdentifier::Uuid(u) => write!(f, "{u}"),
        }
    }
}

impl From<FragmentSeqNo> for FragmentIdentifier {
    fn from(seq_no: FragmentSeqNo) -> Self {
        Self::SeqNo(seq_no)
    }
}

impl From<FragmentUuid> for FragmentIdentifier {
    fn from(uuid: FragmentUuid) -> Self {
        Self::Uuid(uuid)
    }
}

///////////////////////////////////////////// Fragment /////////////////////////////////////////////

/// A Fragment is an immutable piece of the log containing adjacent writes.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Fragment {
    pub path: String,
    pub seq_no: FragmentIdentifier,
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

const FRAGMENT_PREFIX_WITH_TRAILING_SLASH: &str = "log/";

pub fn prefixed_fragment_path(prefix: &str, fragment_id: FragmentIdentifier) -> String {
    format!("{prefix}/{}", unprefixed_fragment_path(fragment_id))
}

pub fn unprefixed_fragment_path(fragment_id: FragmentIdentifier) -> String {
    // NOTE(rescrv):  The SeqNo case uses a bucket prefix to FragmentSeqNo.  This was done to aid
    // in spreading reads out.  Research since then has shown this to be unnecessary since 2018
    // when Amazon started splitting at more than just '/' characters.
    //
    // For that reason we don't prefix the UUID.
    match fragment_id {
        FragmentIdentifier::SeqNo(seq_no) => {
            let bucket = seq_no.bucket();
            format!(
                "{}Bucket={:016x}/FragmentSeqNo={:016x}.parquet",
                FRAGMENT_PREFIX_WITH_TRAILING_SLASH,
                bucket,
                seq_no.as_u64(),
            )
        }
        FragmentIdentifier::Uuid(uuid) => {
            format!(
                "{}Uuid={}.parquet",
                FRAGMENT_PREFIX_WITH_TRAILING_SLASH, uuid,
            )
        }
    }
}

pub fn parse_fragment_path(path: &str) -> Option<FragmentIdentifier> {
    // FragmentIdentifier is always in the basename.
    let (_, basename) = path.rsplit_once('/')?;
    let name = basename.strip_suffix(".parquet")?;
    if let Some(number) = name.strip_prefix("FragmentSeqNo=") {
        u64::from_str_radix(number, 16)
            .ok()
            .map(|n| FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(n)))
    } else if let Some(uuid_str) = name.strip_prefix("Uuid=") {
        uuid_str
            .parse()
            .ok()
            .map(|u| FragmentIdentifier::Uuid(FragmentUuid::from_uuid(u)))
    } else {
        None
    }
}

pub fn fragment_path(prefix: &str, path: &str) -> String {
    format!("{prefix}/{path}")
}

////////////////////////////////////////// LogWriterTrait //////////////////////////////////////////

/// Trait that provides a type-erased interface to LogWriter.
#[async_trait::async_trait]
pub trait LogWriterTrait: std::fmt::Debug + Send + Sync + 'static {
    /// Append a single message to the log.
    async fn append(&self, message: Vec<u8>) -> Result<LogPosition, Error>;

    /// Append multiple messages to the log atomically.
    async fn append_many(&self, messages: Vec<Vec<u8>>) -> Result<LogPosition, Error>;

    /// Returns a possibly-stale copy of the manifest with a witness for verification.
    async fn manifest_and_witness(&self) -> Result<ManifestAndWitness, Error>;

    /// Perform phase 1 of garbage collection: compute garbage.
    async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error>;

    /// Perform phase 2 of garbage collection: update the manifest.
    async fn garbage_collect_phase2_update_manifest(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error>;

    /// Perform phase 3 of garbage collection: delete garbage files.
    async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error>;

    /// Perform all three phases of garbage collection in sequence.
    async fn garbage_collect(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl<
        P: interfaces::FragmentPointer,
        FP: interfaces::FragmentManagerFactory<FragmentPointer = P> + Send + Sync + 'static,
        MP: interfaces::ManifestManagerFactory<FragmentPointer = P> + Send + Sync + 'static,
    > LogWriterTrait for LogWriter<P, FP, MP>
{
    async fn append(&self, message: Vec<u8>) -> Result<LogPosition, Error> {
        LogWriter::append(self, message).await
    }

    async fn append_many(&self, messages: Vec<Vec<u8>>) -> Result<LogPosition, Error> {
        LogWriter::append_many(self, messages).await
    }

    async fn manifest_and_witness(&self) -> Result<ManifestAndWitness, Error> {
        LogWriter::manifest_and_witness(self).await
    }

    async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error> {
        LogWriter::garbage_collect_phase1_compute_garbage(self, options, keep_at_least).await
    }

    async fn garbage_collect_phase2_update_manifest(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        LogWriter::garbage_collect_phase2_update_manifest(self, options).await
    }

    async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        LogWriter::garbage_collect_phase3_delete_garbage(self, options).await
    }

    async fn garbage_collect(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<(), Error> {
        LogWriter::garbage_collect(self, options, keep_at_least).await
    }
}

////////////////////////////////////////// LogReaderTrait //////////////////////////////////////////

/// Trait that provides a type-erased interface to LogReader.
#[async_trait::async_trait]
pub trait LogReaderTrait: std::fmt::Debug + Send + Sync + 'static {
    /// Verify that the reader would read the same manifest as the one provided in
    /// manifest_and_witness, but do it in a way that doesn't load the whole manifest.
    async fn verify(&self, manifest_and_witness: &ManifestAndWitness) -> Result<bool, Error>;

    /// Load and return the manifest.
    async fn manifest(&self) -> Result<Option<Manifest>, Error>;

    /// Load and return the manifest with its witness.
    async fn manifest_and_witness(&self) -> Result<Option<ManifestAndWitness>, Error>;

    /// Return the oldest timestamp in the log.
    async fn oldest_timestamp(&self) -> Result<LogPosition, Error>;

    /// Return the next write timestamp (the position where the next write will be placed).
    async fn next_write_timestamp(&self) -> Result<LogPosition, Error>;

    /// Scan fragments from a given position with limits.
    async fn scan(&self, from: LogPosition, limits: reader::Limits)
        -> Result<Vec<Fragment>, Error>;

    /// Scan fragments using a cached manifest.
    ///
    /// This differs from scan in that it takes a loaded manifest.
    async fn scan_with_cache(
        &self,
        manifest: &Manifest,
        from: LogPosition,
        limits: reader::Limits,
        short_read: &mut bool,
    ) -> Result<Vec<Fragment>, Error>;

    /// Read a parquet fragment and return its contents.
    async fn read_parquet(
        &self,
        fragment: &Fragment,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error>;

    /// Scrub the log to verify its integrity.
    async fn scrub(&self, limits: reader::Limits) -> Result<ScrubSuccess, Vec<Error>>;
}

#[async_trait::async_trait]
impl<
        P: interfaces::FragmentPointer,
        FC: interfaces::FragmentConsumer<FragmentPointer = P>,
        MC: interfaces::ManifestConsumer<P>,
    > LogReaderTrait for LogReader<P, FC, MC>
{
    async fn verify(&self, manifest_and_witness: &ManifestAndWitness) -> Result<bool, Error> {
        LogReader::verify(self, manifest_and_witness).await
    }

    async fn manifest(&self) -> Result<Option<Manifest>, Error> {
        LogReader::manifest(self).await
    }

    async fn manifest_and_witness(&self) -> Result<Option<ManifestAndWitness>, Error> {
        LogReader::manifest_and_witness(self).await
    }

    async fn oldest_timestamp(&self) -> Result<LogPosition, Error> {
        LogReader::oldest_timestamp(self).await
    }

    async fn next_write_timestamp(&self) -> Result<LogPosition, Error> {
        LogReader::next_write_timestamp(self).await
    }

    async fn scan(
        &self,
        from: LogPosition,
        limits: reader::Limits,
    ) -> Result<Vec<Fragment>, Error> {
        LogReader::scan(self, from, limits).await
    }

    async fn scan_with_cache(
        &self,
        manifest: &Manifest,
        from: LogPosition,
        limits: reader::Limits,
        short_read: &mut bool,
    ) -> Result<Vec<Fragment>, Error> {
        LogReader::scan_with_cache(self, manifest, from, limits, short_read).await
    }

    async fn read_parquet(
        &self,
        fragment: &Fragment,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        LogReader::read_parquet(self, fragment).await
    }

    async fn scrub(&self, limits: reader::Limits) -> Result<ScrubSuccess, Vec<Error>> {
        LogReader::scrub(self, limits).await
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!(
            "THIS_IS_THE_COLLECTION/log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet",
            prefixed_fragment_path("THIS_IS_THE_COLLECTION", FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)))
        );
    }

    #[test]
    fn paths_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let path = prefixed_fragment_path(
            "THIS_IS_THE_COLLECTION",
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid)),
        );
        println!("prefixed_fragment_path(Uuid(...)): {path}");
        assert_eq!(
            "THIS_IS_THE_COLLECTION/log/Uuid=550e8400-e29b-41d4-a716-446655440000.parquet",
            path
        );
    }

    #[test]
    fn prefixed_fragment_path_seq_no_round_trip() {
        let original = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(4097));
        let path = prefixed_fragment_path("test_prefix", original);
        println!("prefixed_fragment_path(SeqNo(4097)): {path}");
        let parsed = parse_fragment_path(&path);
        assert_eq!(parsed, Some(original));
    }

    #[test]
    fn prefixed_fragment_path_uuid_round_trip() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let original = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid));
        let path = prefixed_fragment_path("test_prefix", original);
        println!("prefixed_fragment_path(Uuid(...)): {path}");
        let parsed = parse_fragment_path(&path);
        assert_eq!(parsed, Some(original));
    }

    #[test]
    fn fragment_identifier_serde_round_trip() {
        let original = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42));
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42)): {serialized}");
        let deserialized: FragmentIdentifier =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn fragment_identifier_uuid_serde_round_trip() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let original = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid));
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized FragmentIdentifier::Uuid(...): {serialized}");
        let deserialized: FragmentIdentifier =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn parse_fragment_path_seq_no() {
        let path = "prefix/log/Bucket=0000000000001000/FragmentSeqNo=0000000000001234.parquet";
        let result = parse_fragment_path(path);
        assert_eq!(
            result,
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(0x1234)))
        );
    }

    #[test]
    fn parse_fragment_path_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let path = "prefix/log/Uuid=550e8400-e29b-41d4-a716-446655440000.parquet";
        let result = parse_fragment_path(path);
        assert_eq!(
            result,
            Some(FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid)))
        );
    }

    #[test]
    fn parse_fragment_path_invalid() {
        let path = "prefix/log/Unknown=something.parquet";
        let result = parse_fragment_path(path);
        assert_eq!(result, None);
    }

    #[test]
    fn unprefixed_fragment_path_seq_no_round_trip() {
        let original = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(4097));
        let path = unprefixed_fragment_path(original);
        println!("unprefixed_fragment_path(SeqNo(4097)): {path}");
        let parsed = parse_fragment_path(&path);
        assert_eq!(parsed, Some(original));
    }

    #[test]
    fn unprefixed_fragment_path_uuid_round_trip() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let original = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid));
        let path = unprefixed_fragment_path(original);
        println!("unprefixed_fragment_path(Uuid(...)): {path}");
        let parsed = parse_fragment_path(&path);
        assert_eq!(parsed, Some(original));
    }

    #[test]
    fn fragment_identifier_seq_no_ordering() {
        let a = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1));
        let b = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2));
        let c = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(100));

        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
        assert!(b > a);
        assert!(c > b);
        assert!(c > a);

        let same1 = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42));
        let same2 = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42));
        assert_eq!(same1.cmp(&same2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn fragment_identifier_uuid_ordering() {
        // UUIDs are ordered by their byte representation.
        // These UUIDs differ in their last byte for easy comparison.
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uuid2 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
        let uuid3 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440003").unwrap();

        let a = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1));
        let b = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2));
        let c = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid3));

        println!("uuid1: {uuid1}");
        println!("uuid2: {uuid2}");
        println!("uuid3: {uuid3}");

        assert!(a < b, "uuid1 should be less than uuid2");
        assert!(b < c, "uuid2 should be less than uuid3");
        assert!(a < c, "uuid1 should be less than uuid3");
        assert!(b > a, "uuid2 should be greater than uuid1");
        assert!(c > b, "uuid3 should be greater than uuid2");
        assert!(c > a, "uuid3 should be greater than uuid1");

        let same1 = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2));
        let same2 = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2));
        assert_eq!(same1.cmp(&same2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn fragment_identifier_cross_variant_ordering() {
        // Cross-variant comparison: SeqNo < Uuid
        let seq_no = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(u64::MAX));
        let uuid = FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid::Uuid::nil()));

        assert!(
            seq_no < uuid,
            "SeqNo should always be less than Uuid in cross-variant comparison"
        );
        assert!(
            uuid > seq_no,
            "Uuid should always be greater than SeqNo in cross-variant comparison"
        );
    }

    #[test]
    fn fragment_identifier_sorting() {
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uuid2 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();

        let mut seq_nos = vec![
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3)),
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
        ];
        seq_nos.sort();
        assert_eq!(
            seq_nos,
            vec![
                FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
                FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
                FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3)),
            ]
        );

        let mut uuids = vec![
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
        ];
        uuids.sort();
        assert_eq!(
            uuids,
            vec![
                FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
                FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            ]
        );
    }

    #[test]
    fn fragment_identifier_max_finds_correct_value() {
        // Test that .max() works correctly for SeqNo
        let fragments_seq = [
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(5)),
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(8)),
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
        ];
        let max_seq = fragments_seq.iter().max();
        assert_eq!(
            max_seq,
            Some(&FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(8)))
        );

        // Test that .max() works correctly for Uuid
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uuid2 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
        let uuid3 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440003").unwrap();
        let fragments_uuid = [
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
            FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid3)),
        ];
        let max_uuid = fragments_uuid.iter().max();
        assert_eq!(
            max_uuid,
            Some(&FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid3)))
        );
    }
}
