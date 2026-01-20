use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use setsum::Setsum;
use tracing::Span;

use chroma_storage::ETag;
use chroma_types::Cmek;

use crate::{
    Error, Fragment, FragmentIdentifier, FragmentSeqNo, FragmentUuid, Garbage,
    GarbageCollectionOptions, LogPosition, Manifest, ManifestAndWitness, Snapshot, SnapshotPointer,
};

pub mod batch_manager;
pub mod repl;
pub mod s3;

pub use batch_manager::BatchManager;

////////////////////////////////////////// FragmentPointer /////////////////////////////////////////

pub trait FragmentPointer: Clone + Send + Sync + 'static {
    fn try_create(ident: FragmentIdentifier, pos: LogPosition) -> Option<Self>;
    fn identifier(&self) -> FragmentIdentifier;
    fn bootstrap(position: LogPosition) -> Self
    where
        Self: Sized;
}

impl FragmentPointer for (FragmentSeqNo, LogPosition) {
    fn try_create(ident: FragmentIdentifier, pos: LogPosition) -> Option<Self> {
        if let FragmentIdentifier::SeqNo(seq_no) = ident {
            Some((seq_no, pos))
        } else {
            None
        }
    }

    fn identifier(&self) -> FragmentIdentifier {
        FragmentIdentifier::SeqNo(self.0)
    }

    fn bootstrap(position: LogPosition) -> Self {
        (FragmentSeqNo::BEGIN, position)
    }
}

impl FragmentPointer for FragmentUuid {
    fn try_create(ident: FragmentIdentifier, _: LogPosition) -> Option<Self> {
        if let FragmentIdentifier::Uuid(uuid) = ident {
            Some(uuid)
        } else {
            None
        }
    }

    fn identifier(&self) -> FragmentIdentifier {
        FragmentIdentifier::Uuid(*self)
    }

    fn bootstrap(_: LogPosition) -> Self {
        FragmentUuid::generate()
    }
}

////////////////////////////////////// FragmentManagerFactory //////////////////////////////////////

#[async_trait::async_trait]
pub trait FragmentManagerFactory {
    type FragmentPointer: FragmentPointer;
    type Publisher: FragmentPublisher<FragmentPointer = Self::FragmentPointer>;
    type Consumer: FragmentConsumer<FragmentPointer = Self::FragmentPointer>;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error>;
    async fn make_consumer(&self) -> Result<Self::Consumer, Error>;
}

///////////////////////////////////////// FragmentUploader /////////////////////////////////////////

#[async_trait::async_trait]
pub trait FragmentUploader<FP: FragmentPointer>: Send + Sync + 'static {
    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &FP,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<(String, Setsum, usize), Error>;
}

///////////////////////////////////////// FragmentPublisher ////////////////////////////////////////

#[async_trait::async_trait]
pub trait FragmentPublisher: Send + Sync + 'static {
    type FragmentPointer: FragmentPointer;

    /// Enqueue work to be published.
    async fn push_work(
        &self,
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        span: Span,
    );
    /// Take enqueued work to be published.
    async fn take_work(
        &self,
        manifest_manager: &(dyn ManifestPublisher<Self::FragmentPointer> + Sync),
    ) -> Result<
        Option<(
            Self::FragmentPointer,
            Vec<(
                Vec<Vec<u8>>,
                tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
                Span,
            )>,
        )>,
        Error,
    >;
    /// Finish the previous call to take_work.
    async fn finish_write(&self);

    /// Wait until take_work might have work.
    async fn wait_for_writable(&self);
    /// How long to sleep until take work might have work.
    fn until_next_time(&self) -> Duration;

    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &Self::FragmentPointer,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<(String, Setsum, usize), Error>;

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self);
    /// Finish shutting down.
    fn shutdown_finish(&self);
}

///////////////////////////////////////// FragmentConsumer /////////////////////////////////////////

#[async_trait::async_trait]
pub trait FragmentConsumer: Send + Sync + 'static {
    type FragmentPointer: FragmentPointer;

    async fn read_raw_bytes(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<Arc<Vec<u8>>, Error>;

    async fn read_parquet(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error>;

    async fn read_fragment(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<Option<Fragment>, Error>;
}

////////////////////////////////////// ManifestManagerFactory //////////////////////////////////////

#[async_trait::async_trait]
pub trait ManifestManagerFactory {
    type FragmentPointer: FragmentPointer;
    type Publisher: ManifestPublisher<Self::FragmentPointer>;
    type Consumer: ManifestConsumer<Self::FragmentPointer>;

    async fn init_manifest(&self, manifest: &Manifest) -> Result<(), Error>;
    async fn open_publisher(&self) -> Result<Self::Publisher, Error>;
    async fn make_consumer(&self) -> Result<Self::Consumer, Error>;
}

////////////////////////////////////////// ManifestWitness /////////////////////////////////////////

/// Position-based witness data for Spanner backend.
///
/// Contains `enumeration_offset` and `collected_setsum` where:
/// - `enumeration_offset` changes on `publish_fragment` (appends).
/// - `collected_setsum` changes on `apply_garbage` (GC).
///
/// Both must match for the cached manifest to be considered valid.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PositionWitness {
    /// The enumeration offset, which changes on appends.
    pub position: LogPosition,
    /// The collected setsum as hexdigest, which changes on GC.
    pub collected: String,
}

impl PositionWitness {
    /// Creates a new PositionWitness from a LogPosition and Setsum.
    pub fn new(position: LogPosition, collected: setsum::Setsum) -> Self {
        Self {
            position,
            collected: collected.hexdigest(),
        }
    }

    /// Returns the LogPosition component.
    pub fn position(&self) -> LogPosition {
        self.position
    }

    /// Returns the collected setsum, or None if the stored hexdigest is invalid.
    pub fn collected(&self) -> Option<setsum::Setsum> {
        setsum::Setsum::from_hexdigest(&self.collected)
    }
}

/// A witness to the state of a manifest used for cache invalidation.
///
/// The witness is compared against the current state to determine if a cached manifest is still
/// valid. Different backends use different witness types:
/// - S3: Uses ETag from the object store.
/// - Spanner: Uses Position with enumeration_offset and collected setsum.
///
/// The `collected` field in the Position variant is critical for correctness: it ensures that
/// garbage collection (which modifies `collected` and deletes fragments) invalidates cached
/// manifests. Without it, readers could use stale cached manifests containing references to
/// deleted fragments.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ManifestWitness {
    ETag(ETag),
    /// Position-based witness for Spanner backend.
    Position(PositionWitness),
}

///////////////////////////////////////// ManifestPublisher ////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManifestPublisher<FP: FragmentPointer>: Send + Sync + 'static {
    /// Recover the manifest so that it can do work.
    async fn recover(&mut self) -> Result<(), Error>;
    /// Return a possibly-stale version of the manifest.
    async fn manifest_and_witness(&self) -> Result<ManifestAndWitness, Error>;
    /// Assign a timestamp for the next fragment that's going to be published on this manifest.
    fn assign_timestamp(&self, record_count: usize) -> Option<FP>;
    /// Publish a fragment previously assigned a timestamp using assign_timestamp.
    async fn publish_fragment(
        &self,
        pointer: &FP,
        regions: &[&str],
        path: &str,
        messages_len: u64,
        num_bytes: u64,
        setsum: Setsum,
    ) -> Result<LogPosition, Error>;
    /// Check if the garbge will apply "cleanly", that is without violating invariants.
    async fn garbage_applies_cleanly(&self, garbage: &Garbage) -> Result<bool, Error>;
    /// Apply a garbage file to the manifest.
    async fn apply_garbage(&self, garbage: Garbage) -> Result<(), Error>;
    /// Compute the garbage assuming at least log position will be kept.
    async fn compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, Error>;

    /// Snapshot storers and accessors
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
    async fn snapshot_install(&self, snapshot: &Snapshot) -> Result<SnapshotPointer, Error>;
    /// Manifest storers and accessors
    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error>;
    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error>;

    /// Shutdown the manifest manager.  Must be called between prepare and finish of
    /// FragmentPublisher shutdown.
    fn shutdown(&self);
}

///////////////////////////////////////// ManifestConsumer /////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManifestConsumer<FP: FragmentPointer>: Send + Sync + 'static {
    /// Snapshot storers and accessors
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
    /// Manifest storers and accessors
    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error>;
    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error>;
}

/////////////////////////////////////////////// utils //////////////////////////////////////////////

/// Computes the setsum and extracts records from parquet bytes.
///
/// The `starting_log_position` is used to convert relative offsets to absolute positions for the
/// returned records. The setsum is always computed using the raw offsets from the file (relative
/// or absolute) to match how the writer computed it.
///
/// Returns `(setsum, records, uses_relative_offsets)` where `uses_relative_offsets` indicates
/// whether the parquet file uses relative offsets (true) or absolute offsets (false).
///
/// The returned LogPositions are absolute.
///
/// Thus, you must provide a starting_log_position for relative logs and omit it for absolute ones.
#[allow(clippy::type_complexity)]
pub fn checksum_parquet(
    parquet: &[u8],
    starting_log_position: Option<LogPosition>,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, bool, u64), Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet))
        .map_err(|e| {
            Error::CorruptFragment(format!("failed to create parquet reader builder: {}", e))
        })?;
    let reader = builder
        .build()
        .map_err(|e| Error::CorruptFragment(format!("failed to build parquet reader: {}", e)))?;
    let mut setsum = Setsum::default();
    let mut records = vec![];
    let mut uses_relative_offsets = false;
    let mut epoch_micros_singleton = None;
    for batch in reader {
        let batch = batch
            .map_err(|e| Error::CorruptFragment(format!("failed to read parquet batch: {}", e)))?;
        // Determine if we have absolute offsets or relative offsets.
        // - For absolute offsets: offset_base is 0, use offset directly for both setsum and position
        // - For relative offsets: offset_base is starting_log_position (or 0 if None), use raw
        //   offset for setsum (to match writer) and add offset_base for returned positions
        let (offset_column, offset_base) = if let Some(offset) = batch.column_by_name("offset") {
            if starting_log_position.is_some() {
                return Err(Error::internal(file!(), line!()));
            }
            (offset.clone(), 0u64)
        } else if let Some(relative_offset) = batch.column_by_name("relative_offset") {
            // For relative offsets, use the starting position if provided, otherwise 0.
            // When starting_log_position is None, the returned positions will be relative
            // (0, 1, 2...) which is appropriate for read_fragment which derives start/limit.
            uses_relative_offsets = true;
            let Some(base) = starting_log_position.map(|p| p.offset()) else {
                return Err(Error::internal(file!(), line!()));
            };
            (relative_offset.clone(), base)
        } else {
            return Err(Error::CorruptFragment(
                "missing offset or relative_offset column".to_string(),
            ));
        };
        let epoch_micros = batch
            .column_by_name("timestamp_us")
            .ok_or_else(|| Error::CorruptFragment("missing timestamp_us column".to_string()))?;
        let body = batch
            .column_by_name("body")
            .ok_or_else(|| Error::CorruptFragment("missing body column".to_string()))?;
        let offset_array = offset_column
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                Error::CorruptFragment("offset column is not UInt64Array".to_string())
            })?;
        let epoch_micros = epoch_micros
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                Error::CorruptFragment("timestamp_us column is not UInt64Array".to_string())
            })?;
        let body = body
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| Error::CorruptFragment("body column is not BinaryArray".to_string()))?;
        for i in 0..batch.num_rows() {
            // The raw offset from the file (relative or absolute depending on column type)
            let raw_offset = offset_array.value(i);
            // The absolute offset for returning positions to callers
            let absolute_offset = raw_offset.checked_add(offset_base).ok_or_else(|| {
                Error::CorruptFragment(format!("offset overflow: {} + {}", raw_offset, offset_base))
            })?;
            let epoch_micros = epoch_micros.value(i);
            if let Some(em) = epoch_micros_singleton {
                if em != epoch_micros {
                    return Err(Error::CorruptFragment(format!(
                        "inconsistent epoch_micros: expected {} but found {}",
                        em, epoch_micros
                    )));
                }
            }
            epoch_micros_singleton = Some(epoch_micros);
            let body = body.value(i);
            // Use raw_offset for setsum to match how the writer computed it.
            // The writer uses the offset value that gets stored in the file (relative or absolute).
            setsum.insert_vectored(&[&raw_offset.to_be_bytes(), &epoch_micros.to_be_bytes(), body]);
            // Use absolute_offset for returned positions so callers get correct log positions.
            records.push((LogPosition::from_offset(absolute_offset), body.to_vec()));
        }
    }
    if let Some(epoch_micros) = epoch_micros_singleton {
        Ok((setsum, records, uses_relative_offsets, epoch_micros))
    } else {
        Ok((setsum, records, uses_relative_offsets, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_EPOCH_MICROS: u64 = 1234567890123456;

    /// Verifies checksum_parquet returns relative positions (0, 1, 2...) when called with None
    /// starting_log_position on a relative-offset parquet file.
    #[test]
    fn checksum_parquet_with_none_starting_position_returns_relative_positions() {
        use crate::writer::construct_parquet;

        let messages = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];

        // Create a relative-offset parquet file
        let (buffer, _setsum) = construct_parquet(None, &messages, TEST_EPOCH_MICROS)
            .expect("construct_parquet should succeed");

        // Read with None starting_log_position
        let (setsum, records, uses_relative_offsets, _) =
            checksum_parquet(&buffer, Some(LogPosition::from_offset(42)))
                .expect("checksum_parquet should succeed");

        println!(
            "checksum_parquet_with_none_starting_position_returns_relative_positions: \
             uses_relative_offsets={}, positions={:?}, setsum={}",
            uses_relative_offsets,
            records.iter().map(|(p, _)| p.offset()).collect::<Vec<_>>(),
            setsum.hexdigest()
        );

        assert!(uses_relative_offsets, "should detect relative offsets");
        assert_eq!(records.len(), 3, "should have 3 records");
        // Positions should be 0, 1, 2 (relative)
        assert_eq!(records[0].0.offset(), 42, "first position should be 42");
        assert_eq!(records[1].0.offset(), 43, "second position should be 43");
        assert_eq!(records[2].0.offset(), 44, "third position should be 44");
    }

    /// Verifies checksum_parquet translates relative positions to absolute when given a
    /// starting_log_position.
    #[test]
    fn checksum_parquet_with_starting_position_translates_relative_to_absolute() {
        use crate::writer::construct_parquet;

        let messages = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
        let starting_position = LogPosition::from_offset(100);

        // Create a relative-offset parquet file
        let (buffer, setsum_from_writer) = construct_parquet(None, &messages, TEST_EPOCH_MICROS)
            .expect("construct_parquet should succeed");

        // Read with a starting_log_position - positions should be translated
        let (setsum_from_reader, records, uses_relative_offsets, _) =
            checksum_parquet(&buffer, Some(starting_position))
                .expect("checksum_parquet should succeed");

        println!(
            "checksum_parquet_with_starting_position_translates_relative_to_absolute: \
             uses_relative_offsets={}, positions={:?}, setsum_writer={}, setsum_reader={}",
            uses_relative_offsets,
            records.iter().map(|(p, _)| p.offset()).collect::<Vec<_>>(),
            setsum_from_writer.hexdigest(),
            setsum_from_reader.hexdigest()
        );

        assert!(uses_relative_offsets, "should detect relative offsets");
        assert_eq!(records.len(), 3, "should have 3 records");
        // Positions should be translated to absolute (100, 101, 102)
        assert_eq!(
            records[0].0.offset(),
            100,
            "first position should be 100 (translated)"
        );
        assert_eq!(
            records[1].0.offset(),
            101,
            "second position should be 101 (translated)"
        );
        assert_eq!(
            records[2].0.offset(),
            102,
            "third position should be 102 (translated)"
        );

        // Setsum should still match because it uses raw offsets (0, 1, 2) not translated ones
        assert_eq!(
            setsum_from_writer, setsum_from_reader,
            "setsums should match regardless of starting_log_position translation"
        );
    }

    /// Verifies that for absolute-offset files, the starting_log_position parameter is ignored
    /// for position calculation (since positions are already absolute).
    #[test]
    fn checksum_parquet_ignores_starting_position_for_absolute_offset_files() {
        use crate::writer::construct_parquet;

        let messages = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let write_position = LogPosition::from_offset(50);

        // Create an absolute-offset parquet file starting at offset 50
        let (buffer, setsum_from_writer) =
            construct_parquet(Some(write_position), &messages, TEST_EPOCH_MICROS)
                .expect("construct_parquet should succeed");

        // Read with a different starting_log_position - should be ignored for absolute files
        let (setsum_from_reader, records, uses_relative_offsets, _) =
            checksum_parquet(&buffer, None).expect("checksum_parquet should succeed");

        println!(
            "checksum_parquet_ignores_starting_position_for_absolute_offset_files: \
             uses_relative_offsets={}, positions={:?}",
            uses_relative_offsets,
            records.iter().map(|(p, _)| p.offset()).collect::<Vec<_>>()
        );

        assert!(
            !uses_relative_offsets,
            "should detect absolute offsets in file"
        );
        assert_eq!(records.len(), 2, "should have 2 records");
        // Positions should be the original absolute values (50, 51), not affected by
        // the different_position parameter
        assert_eq!(
            records[0].0.offset(),
            50,
            "first position should be 50 (original absolute)"
        );
        assert_eq!(
            records[1].0.offset(),
            51,
            "second position should be 51 (original absolute)"
        );

        // Setsums should match
        assert_eq!(
            setsum_from_writer, setsum_from_reader,
            "setsums should match for absolute-offset files"
        );
    }
}
