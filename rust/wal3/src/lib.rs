#![doc = include_str!("../README.md")]

use serde::{Deserialize, Serialize};

mod backoff;

use backoff::ExponentialBackoff;

/////////////////////////////////////////////// Error //////////////////////////////////////////////

/// Error captures the different error conditions that can occur when interacting with the log.
#[derive(Clone, Debug, Default)]
pub enum Error {
    #[default]
    Success,
    UninitializedLog,
    AlreadyInitialized,
    GarbageCollected,
    LogContention,
    LogFull,
    EndOfLog,
    Internal,
    CorruptManifest(String),
    CorruptCursor(String),
    NoSuchCursor(String),
}

//////////////////////////////////////////// LogPosition ///////////////////////////////////////////

/// A log position is a pair of an offset and a timestamp.  Every record has a unique log position.
/// A LogPosition only implements equality, which checks both offset and timestamp_us.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogPosition {
    /// The offset field of a LogPosition is a strictly increasing timestamp.  It has no gaps and
    /// spans [0, u64::MAX).
    offset: u64,
    /// The timestamp_us field of a LogPosition is a strictly increasing timestamp.  It has gaps
    /// and corresponds to wallclock time.
    timestamp_us: u64,
}

impl LogPosition {
    pub fn new(offset: u64, timestamp_us: u64) -> Self {
        LogPosition {
            offset,
            timestamp_us,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn timestamp_us(&self) -> u64 {
        self.timestamp_us
    }

    pub fn contains_offset(start: LogPosition, end: LogPosition, offset: u64) -> bool {
        start.offset <= offset && offset < end.offset
    }

    pub fn contains_timestamp(start: LogPosition, end: LogPosition, timestamp: u64) -> bool {
        start.offset <= timestamp && timestamp < end.offset
    }
}

////////////////////////////////////////// ThrottleOptions /////////////////////////////////////////

/// ThrottleOptions control admission to S3 and batch size/interval.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ThrottleOptions {
    /// The maximum number of bytes to batch.  Defaults to 8MB.
    pub batch_size: usize,
    /// The maximum number of microseconds to batch.  Defaults to 20ms.
    pub batch_interval: usize,
    /// The maximum number of operations per second to allow.  Defaults to 2_000.
    pub throughput: usize,
    /// The number of operations per second to reserve for backoff/retry.  Defaults to 1_500.
    pub headroom: usize,
    /// The maximum number of outstanding requests to allow.  Defaults to 100.
    pub outstanding: usize,
}

impl Default for ThrottleOptions {
    fn default() -> Self {
        ThrottleOptions {
            // Batch for at least 20ms.
            batch_interval: 20_000,
            // Set a batch size of 8MB.
            batch_size: 8 * 1_000_000,
            // Set a throughput that's approximately 5/7th the throughput of the throughput S3
            // allows.  If we hit throttle errors at this throughput we have a case for support.
            throughput: 2_000,
            // How much headroom we have for retries.
            headroom: 1_500,
            // Allow up to 100 requests to be outstanding.
            outstanding: 100,
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
    pub snapshot_rollover_threshold: usize,
    /// The maximum number of fragment pointers to embed in a snapshot or manifest.
    pub fragment_rollover_threshold: usize,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        SnapshotOptions {
            // TODO(rescrv):  Commented out values are better.
            snapshot_rollover_threshold: 2048, // (1 << 18) / SnapPointer::JSON_SIZE_ESTIMATE,
            fragment_rollover_threshold: 1536, // (1 << 19) / ShardFragment::JSON_SIZE_ESTIMATE,
        }
    }
}

/////////////////////////////////////////// FragmentSeqNo //////////////////////////////////////////

/// A FragmentSeqNo is the offset of the log position of the first record in a fragment.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct FragmentSeqNo(pub usize);

impl FragmentSeqNo {
    pub fn successor(&self) -> Option<Self> {
        if self.0 == usize::MAX {
            None
        } else {
            Some(FragmentSeqNo(self.0 + 1))
        }
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
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub setsum: setsum::Setsum,
}

impl Fragment {
    pub const JSON_SIZE_ESTIMATE: usize = 256;
}

/////////////////////////////////////////////// util ///////////////////////////////////////////////

fn deserialize_setsum<'de, D>(deserializer: D) -> Result<setsum::Setsum, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    setsum::Setsum::from_hexdigest(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("invalid setsum: {}", s)))
}

fn serialize_setsum<S>(setsum: &setsum::Setsum, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let s = setsum.hexdigest();
    s.serialize(serializer)
}
