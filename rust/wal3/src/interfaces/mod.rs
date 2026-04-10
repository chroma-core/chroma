use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use setsum::Setsum;
use tracing::Span;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, Storage, StorageError,
};
use chroma_types::Cmek;

use crate::{
    reader::Limits, CursorWitness, Error, Fragment, FragmentIdentifier, FragmentSeqNo,
    FragmentUuid, Garbage, GarbageCollectionOptions, LogPosition, LogWriterOptions, Manifest,
    ManifestAndWitness, ManifestBounds, ManifestBoundsAndWitness, Snapshot, SnapshotPointer,
    StorageWrapper, ThrottleOptions,
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
    type Consumer: FragmentConsumer;
    type Uploader: FragmentUploader<Self::FragmentPointer>;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error>;
    async fn make_consumer(&self) -> Result<Self::Consumer, Error>;
    async fn make_fragment_uploader(&self) -> Result<Self::Uploader, Error>;
    async fn preferred_storage(&self) -> Storage;
    fn write_options(&self) -> LogWriterOptions;
}

//////////////////////////////////// Fragment Upload Fault Injection ///////////////////////////////

/// The label used by downstream services to target wal3 fragment uploads with fault injection.
pub const FRAGMENT_UPLOAD_FAULT_LABEL: &str = "wal3.fragment_upload";
/// The hard-coded fault labels used to target replicated wal3 fragment uploads by replica index.
pub const FRAGMENT_UPLOAD_REPLICA_FAULT_LABELS: [&str; 3] = [
    "wal3.fragment_upload.0",
    "wal3.fragment_upload.1",
    "wal3.fragment_upload.2",
];

/// Returns the fault label for a specific replicated wal3 fragment upload replica index.
pub fn fragment_upload_replica_fault_label(replica_idx: usize) -> Option<&'static str> {
    FRAGMENT_UPLOAD_REPLICA_FAULT_LABELS
        .get(replica_idx)
        .copied()
}

/// Faults that can be injected immediately before a fragment upload begins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentUploadFault {
    Unavailable,
    Delay(Duration),
}

/// Supplies optional upload faults to wal3 without coupling the crate to a specific registry.
pub trait FragmentUploadFaultInjector: Send + Sync + 'static {
    fn fault_for_upload(&self) -> Option<FragmentUploadFault>;

    fn fault_for_replica_upload(&self, _replica_idx: usize) -> Option<FragmentUploadFault> {
        None
    }
}

impl FragmentUploadFaultInjector for () {
    fn fault_for_upload(&self) -> Option<FragmentUploadFault> {
        None
    }
}

/// Wraps a fragment manager factory so every publisher it creates applies upload fault injection.
pub struct FaultInjectingFragmentManagerFactory<F> {
    inner: F,
    fault_injector: Option<Arc<dyn FragmentUploadFaultInjector>>,
}

impl<F> FaultInjectingFragmentManagerFactory<F> {
    pub fn new(inner: F, fault_injector: Option<Arc<dyn FragmentUploadFaultInjector>>) -> Self {
        Self {
            inner,
            fault_injector,
        }
    }

    pub fn inner(&self) -> &F {
        &self.inner
    }

    pub fn into_inner(self) -> F {
        self.inner
    }
}

impl<F: Clone> Clone for FaultInjectingFragmentManagerFactory<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault_injector: self.fault_injector.as_ref().map(Arc::clone),
        }
    }
}

/// A fragment uploader wrapper that applies fault injection before delegating to
/// the wrapped uploader.
///
/// This type is public because it is observable through
/// `FaultInjectingFragmentManagerFactory`'s associated publisher type.
pub struct FaultInjectingFragmentUploader<FP: FragmentPointer, U: FragmentUploader<FP>> {
    inner: U,
    fault_injector: Option<Arc<dyn FragmentUploadFaultInjector>>,
    _phantom: std::marker::PhantomData<FP>,
}

impl<FP: FragmentPointer, U: FragmentUploader<FP>> FaultInjectingFragmentUploader<FP, U> {
    fn new(inner: U, fault_injector: Option<Arc<dyn FragmentUploadFaultInjector>>) -> Self {
        Self {
            inner,
            fault_injector,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<FP: FragmentPointer, U: FragmentUploader<FP>> FragmentUploader<FP>
    for FaultInjectingFragmentUploader<FP, U>
{
    async fn upload_parquet(
        &self,
        pointer: &FP,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<UploadResult, Error> {
        let fragment_identifier = pointer.identifier();
        match self
            .fault_injector
            .as_ref()
            .and_then(|fault_injector| fault_injector.fault_for_upload())
        {
            Some(FragmentUploadFault::Delay(delay)) => {
                tracing::warn!(
                    fault_label = FRAGMENT_UPLOAD_FAULT_LABEL,
                    fragment_identifier = %fragment_identifier,
                    delay_seconds = delay.as_secs_f64(),
                    "injecting wal3 upload delay fault"
                );
                tokio::time::sleep(delay).await;
            }
            Some(FragmentUploadFault::Unavailable) => {
                tracing::warn!(
                    fault_label = FRAGMENT_UPLOAD_FAULT_LABEL,
                    fragment_identifier = %fragment_identifier,
                    "injecting wal3 upload unavailable fault"
                );
                return Err(Error::TonicError(tonic::Status::unavailable(format!(
                    "fault injected for {}",
                    FRAGMENT_UPLOAD_FAULT_LABEL
                ))));
            }
            None => {}
        }
        self.inner
            .upload_parquet(pointer, messages, cmek, epoch_micros)
            .await
    }

    async fn preferred_storage(&self) -> Storage {
        self.inner.preferred_storage().await
    }

    async fn preferred_prefix(&self) -> String {
        self.inner.preferred_prefix().await
    }

    async fn preferred_storage_wrapper(&self) -> &StorageWrapper {
        self.inner.preferred_storage_wrapper().await
    }

    async fn storages(&self) -> &[StorageWrapper] {
        self.inner.storages().await
    }
}

/// A fragment publisher that is either the original factory publisher or a
/// fault-injecting `BatchManager` publisher built around a wrapped uploader.
///
/// This type is public because it is the associated publisher type returned by
/// `FaultInjectingFragmentManagerFactory`.
pub enum MaybeFaultInjectingFragmentPublisher<
    FP: FragmentPointer,
    P: FragmentPublisher<FragmentPointer = FP>,
    U: FragmentUploader<FP>,
> {
    Plain(P),
    FaultInjecting(BatchManager<FP, FaultInjectingFragmentUploader<FP, U>>),
}

#[async_trait::async_trait]
impl<FP, P, U> FragmentPublisher for MaybeFaultInjectingFragmentPublisher<FP, P, U>
where
    FP: FragmentPointer,
    P: FragmentPublisher<FragmentPointer = FP>,
    U: FragmentUploader<FP>,
{
    type FragmentPointer = FP;

    async fn push_work(
        &self,
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        span: Span,
    ) {
        match self {
            Self::Plain(publisher) => publisher.push_work(messages, tx, span).await,
            Self::FaultInjecting(publisher) => publisher.push_work(messages, tx, span).await,
        }
    }

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
    > {
        match self {
            Self::Plain(publisher) => publisher.take_work(manifest_manager).await,
            Self::FaultInjecting(publisher) => publisher.take_work(manifest_manager).await,
        }
    }

    async fn finish_write(&self) {
        match self {
            Self::Plain(publisher) => publisher.finish_write().await,
            Self::FaultInjecting(publisher) => publisher.finish_write().await,
        }
    }

    async fn wait_for_writable(&self) {
        match self {
            Self::Plain(publisher) => publisher.wait_for_writable().await,
            Self::FaultInjecting(publisher) => publisher.wait_for_writable().await,
        }
    }

    fn until_next_time(&self) -> Duration {
        match self {
            Self::Plain(publisher) => publisher.until_next_time(),
            Self::FaultInjecting(publisher) => publisher.until_next_time(),
        }
    }

    async fn upload_parquet(
        &self,
        pointer: &Self::FragmentPointer,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<UploadResult, Error> {
        match self {
            Self::Plain(publisher) => {
                publisher
                    .upload_parquet(pointer, messages, cmek, epoch_micros)
                    .await
            }
            Self::FaultInjecting(publisher) => {
                publisher
                    .upload_parquet(pointer, messages, cmek, epoch_micros)
                    .await
            }
        }
    }

    async fn read_json_file(&self, path: &str) -> Result<(Arc<Vec<u8>>, Option<ETag>), Error> {
        match self {
            Self::Plain(publisher) => publisher.read_json_file(path).await,
            Self::FaultInjecting(publisher) => publisher.read_json_file(path).await,
        }
    }

    async fn preferred_storage(&self) -> Storage {
        match self {
            Self::Plain(publisher) => publisher.preferred_storage().await,
            Self::FaultInjecting(publisher) => publisher.preferred_storage().await,
        }
    }

    async fn preferred_prefix(&self) -> String {
        match self {
            Self::Plain(publisher) => publisher.preferred_prefix().await,
            Self::FaultInjecting(publisher) => publisher.preferred_prefix().await,
        }
    }

    async fn storages(&self) -> Vec<repl::StorageWrapper> {
        match self {
            Self::Plain(publisher) => publisher.storages().await,
            Self::FaultInjecting(publisher) => publisher.storages().await,
        }
    }

    fn shutdown_prepare(&self) {
        match self {
            Self::Plain(publisher) => publisher.shutdown_prepare(),
            Self::FaultInjecting(publisher) => publisher.shutdown_prepare(),
        }
    }

    fn shutdown_finish(&self) {
        match self {
            Self::Plain(publisher) => publisher.shutdown_finish(),
            Self::FaultInjecting(publisher) => publisher.shutdown_finish(),
        }
    }

    async fn write_garbage(
        &self,
        options: &ThrottleOptions,
        existing: Option<&ETag>,
        garbage: &Garbage,
    ) -> Result<Option<ETag>, Error> {
        match self {
            Self::Plain(publisher) => publisher.write_garbage(options, existing, garbage).await,
            Self::FaultInjecting(publisher) => {
                publisher.write_garbage(options, existing, garbage).await
            }
        }
    }

    async fn reset_garbage(&self, options: &ThrottleOptions, e_tag: &ETag) -> Result<(), Error> {
        match self {
            Self::Plain(publisher) => publisher.reset_garbage(options, e_tag).await,
            Self::FaultInjecting(publisher) => publisher.reset_garbage(options, e_tag).await,
        }
    }
}

#[async_trait::async_trait]
impl<F> FragmentManagerFactory for FaultInjectingFragmentManagerFactory<F>
where
    F: FragmentManagerFactory + Send + Sync,
{
    type FragmentPointer = <F as FragmentManagerFactory>::FragmentPointer;
    type Publisher = MaybeFaultInjectingFragmentPublisher<
        Self::FragmentPointer,
        <F as FragmentManagerFactory>::Publisher,
        <F as FragmentManagerFactory>::Uploader,
    >;
    type Consumer = <F as FragmentManagerFactory>::Consumer;
    type Uploader = FaultInjectingFragmentUploader<
        Self::FragmentPointer,
        <F as FragmentManagerFactory>::Uploader,
    >;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
        if let Some(fault_injector) = self.fault_injector.as_ref() {
            let fragment_uploader = self.inner.make_fragment_uploader().await?;
            let fragment_uploader = FaultInjectingFragmentUploader::new(
                fragment_uploader,
                Some(Arc::clone(fault_injector)),
            );
            let publisher = BatchManager::new(self.inner.write_options(), fragment_uploader)
                .ok_or_else(|| Error::internal(file!(), line!()))?;
            Ok(MaybeFaultInjectingFragmentPublisher::FaultInjecting(
                publisher,
            ))
        } else {
            Ok(MaybeFaultInjectingFragmentPublisher::Plain(
                FragmentManagerFactory::make_publisher(&self.inner).await?,
            ))
        }
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        FragmentManagerFactory::make_consumer(&self.inner).await
    }

    async fn make_fragment_uploader(&self) -> Result<Self::Uploader, Error> {
        let uploader = self.inner.make_fragment_uploader().await?;
        Ok(FaultInjectingFragmentUploader::new(
            uploader,
            self.fault_injector.as_ref().map(Arc::clone),
        ))
    }

    async fn preferred_storage(&self) -> Storage {
        FragmentManagerFactory::preferred_storage(&self.inner).await
    }

    fn write_options(&self) -> LogWriterOptions {
        self.inner.write_options()
    }
}

///////////////////////////////////////// FragmentUploader /////////////////////////////////////////

/// The result of a successful parquet upload.
///
/// Contains:
/// - `path`: The path where the fragment was stored.
/// - `setsum`: The setsum of the fragment contents.
/// - `num_bytes`: The size of the fragment in bytes.
/// - `successful_regions`: The regions that successfully received the fragment.
///   For single-region deployments, this is empty (all regions are implied).
///   For multi-region deployments, this contains only the regions that actually
///   stored the fragment successfully.
#[derive(Clone, Debug)]
pub struct UploadResult {
    /// The path where the fragment was stored.
    pub path: String,
    /// The setsum of the fragment contents.
    pub setsum: Setsum,
    /// The size of the fragment in bytes.
    pub num_bytes: usize,
    /// The regions that successfully received the fragment.
    /// Empty for single-region deployments (all regions are implied).
    pub successful_regions: Vec<String>,
}

#[async_trait::async_trait]
pub trait FragmentUploader<FP: FragmentPointer>: Send + Sync + 'static {
    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &FP,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<UploadResult, Error>;

    /// The preferred region for this cluster.
    async fn preferred_storage(&self) -> Storage;

    /// The prefix for the preferred storage.
    async fn preferred_prefix(&self) -> String;

    /// The preferred storage wrapper for this cluster.
    async fn preferred_storage_wrapper(&self) -> &StorageWrapper;

    /// The full list of storage wrappers for this cluster
    async fn storages(&self) -> &[StorageWrapper];
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
    ) -> Result<UploadResult, Error>;

    async fn read_json_file(&self, path: &str) -> Result<(Arc<Vec<u8>>, Option<ETag>), Error>;

    /// Returns the preferred storage for this fragment publisher.
    async fn preferred_storage(&self) -> Storage;

    /// Returns the preferred storage's prefix for this fragment publisher.
    async fn preferred_prefix(&self) -> String;

    /// Returns all storages for this fragment publisher.
    async fn storages(&self) -> Vec<repl::StorageWrapper>;

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self);
    /// Finish shutting down.
    fn shutdown_finish(&self);

    /// Write garbage to storage on the preferred region, returning the new ETag if successful.
    async fn write_garbage(
        &self,
        options: &ThrottleOptions,
        existing: Option<&ETag>,
        garbage: &Garbage,
    ) -> Result<Option<ETag>, Error>;

    /// Reset the garbage on the preferred region.
    async fn reset_garbage(&self, options: &ThrottleOptions, e_tag: &ETag) -> Result<(), Error>;
}

///////////////////////////////////////// FragmentConsumer /////////////////////////////////////////

#[async_trait::async_trait]
pub trait FragmentConsumer: Send + Sync + 'static {
    async fn read_parquet(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        let bytes = self.read_bytes(path).await?;
        self.parse_parquet(&bytes, fragment_first_log_position)
            .await
    }

    async fn read_bytes(&self, path: &str) -> Result<Arc<Vec<u8>>, Error>;

    async fn parse_parquet(
        &self,
        parquet: &[u8],
        starting_log_position: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error>;

    async fn parse_parquet_fast(
        &self,
        parquet: &[u8],
        starting_log_position: LogPosition,
    ) -> Result<(Vec<(LogPosition, Vec<u8>)>, u64, u64), Error>;

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
    ///
    /// The `successful_regions` parameter contains the list of regions that successfully stored
    /// the fragment during upload. For single-region deployments, this is empty (all regions
    /// are implied). For multi-region deployments, only these regions should be recorded as
    /// having the fragment.
    async fn publish_fragment(
        &self,
        pointer: &FP,
        path: &str,
        messages_len: u64,
        num_bytes: u64,
        setsum: Setsum,
        successful_regions: &[String],
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

    /// Destroy the named manifest.
    async fn destroy(&self) -> Result<(), Error>;

    /// Load the intrinsic cursor position.
    ///
    /// Returns `Ok(Some(position))` if an intrinsic cursor has been set, or `Ok(None)` if no
    /// intrinsic cursor exists yet.  GC uses this to include the intrinsic cursor in the minimum
    /// of all cursors.
    async fn load_intrinsic_cursor(&self) -> Result<Option<LogPosition>, Error>;
}

///////////////////////////////////////// ManifestConsumer /////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManifestConsumer<FP: FragmentPointer>: Send + Sync + 'static {
    /// Snapshot storers and accessors
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
    /// Manifest storers and accessors
    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error>;
    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error>;
    async fn manifest_bounds_and_witness(&self) -> Result<Option<ManifestBoundsAndWitness>, Error> {
        Ok(self
            .manifest_load()
            .await?
            .map(|(manifest, witness)| ManifestBoundsAndWitness {
                bounds: ManifestBounds {
                    oldest_timestamp: manifest.oldest_timestamp(),
                    next_write_timestamp: manifest.next_write_timestamp(),
                },
                witness,
            }))
    }

    async fn scan_partial(
        &self,
        from: LogPosition,
        limits: Limits,
    ) -> Result<Option<Vec<Fragment>>, Error> {
        let _ = (from, limits);
        Ok(None)
    }

    /// Update the intrinsic cursor using an init-or-swap pattern.
    ///
    /// Loads the current cursor value, then either initializes (if absent) or conditionally
    /// updates (if present) the cursor to the new position.  When `allow_rollback` is false and
    /// the existing cursor is already ahead of `position`, the update is skipped and `Ok(None)` is
    /// returned.  Otherwise returns `Ok(Some(witness))` with the resulting cursor witness.
    async fn update_intrinsic_cursor(
        &self,
        position: LogPosition,
        epoch_us: u64,
        writer: &str,
        allow_rollback: bool,
    ) -> Result<Option<CursorWitness>, Error>;

    /// Load the intrinsic cursor position.
    ///
    /// Returns `Ok(Some(position))` if an intrinsic cursor has been set, or `Ok(None)` if no
    /// intrinsic cursor exists yet.
    async fn load_intrinsic_cursor(&self) -> Result<Option<LogPosition>, Error>;
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
    compute_setsum: bool,
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
            if compute_setsum {
                setsum.insert_vectored(&[
                    &raw_offset.to_be_bytes(),
                    &epoch_micros.to_be_bytes(),
                    body,
                ]);
            }
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

async fn read_raw_bytes(
    path: &str,
    storages: &[repl::StorageWrapper],
) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
    let mut err: Option<StorageError> = None;
    for storage in storages.iter() {
        let path = crate::fragment_path(&storage.prefix, path);
        match storage
            .storage
            .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
            .await
        {
            Ok((parquet, e_tag)) => return Ok((parquet, e_tag)),
            Err(e @ StorageError::NotFound { .. }) => err = Some(e),
            Err(e) => {
                tracing::error!("reading from region {} failed", storage.region);
                err = Some(e);
            }
        }
    }
    if let Some(err) = err {
        Err(err)
    } else {
        Err(StorageError::NotFound {
            path: path.into(),
            source: Arc::new(std::io::Error::other("replicas exhausted")),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{FragmentSeqNo, LogPosition};
    use chroma_storage::Storage;

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
            checksum_parquet(&buffer, true, Some(LogPosition::from_offset(42)))
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
            checksum_parquet(&buffer, true, Some(starting_position))
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
            checksum_parquet(&buffer, true, None).expect("checksum_parquet should succeed");

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

    #[derive(Clone)]
    struct InjectUnavailable;

    #[test]
    fn fragment_upload_replica_fault_labels_are_hard_coded() {
        assert_eq!(
            fragment_upload_replica_fault_label(0),
            Some("wal3.fragment_upload.0")
        );
        assert_eq!(
            fragment_upload_replica_fault_label(1),
            Some("wal3.fragment_upload.1")
        );
        assert_eq!(
            fragment_upload_replica_fault_label(2),
            Some("wal3.fragment_upload.2")
        );
        assert_eq!(fragment_upload_replica_fault_label(3), None);
    }

    impl FragmentUploadFaultInjector for InjectUnavailable {
        fn fault_for_upload(&self) -> Option<FragmentUploadFault> {
            Some(FragmentUploadFault::Unavailable)
        }
    }

    #[derive(Clone)]
    struct RecordingFactory {
        storage: Storage,
        upload_attempts: Arc<AtomicUsize>,
    }

    struct RecordingUploader {
        storage: Storage,
        upload_attempts: Arc<AtomicUsize>,
    }

    struct RecordingConsumer;

    #[async_trait::async_trait]
    impl FragmentUploader<(FragmentSeqNo, LogPosition)> for RecordingUploader {
        async fn upload_parquet(
            &self,
            _pointer: &(FragmentSeqNo, LogPosition),
            _messages: Vec<Vec<u8>>,
            _cmek: Option<Cmek>,
            _epoch_micros: u64,
        ) -> Result<UploadResult, Error> {
            self.upload_attempts.fetch_add(1, Ordering::Relaxed);
            Ok(UploadResult {
                path: "fragment".to_string(),
                setsum: Setsum::default(),
                num_bytes: 0,
                successful_regions: vec![],
            })
        }

        async fn preferred_storage(&self) -> Storage {
            self.storage.clone()
        }

        async fn preferred_prefix(&self) -> String {
            "prefix".to_string()
        }

        async fn preferred_storage_wrapper(&self) -> &StorageWrapper {
            panic!("not used")
        }

        async fn storages(&self) -> &[StorageWrapper] {
            panic!("not used")
        }
    }

    #[async_trait::async_trait]
    impl FragmentConsumer for RecordingConsumer {
        async fn read_bytes(&self, _path: &str) -> Result<Arc<Vec<u8>>, Error> {
            panic!("not used")
        }

        async fn parse_parquet(
            &self,
            _parquet: &[u8],
            _starting_log_position: LogPosition,
        ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
            panic!("not used")
        }

        async fn parse_parquet_fast(
            &self,
            _parquet: &[u8],
            _starting_log_position: LogPosition,
        ) -> Result<(Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
            panic!("not used")
        }

        async fn read_fragment(
            &self,
            _path: &str,
            _fragment_first_log_position: LogPosition,
        ) -> Result<Option<Fragment>, Error> {
            panic!("not used")
        }
    }

    #[async_trait::async_trait]
    impl FragmentManagerFactory for RecordingFactory {
        type FragmentPointer = (FragmentSeqNo, LogPosition);
        type Publisher = BatchManager<Self::FragmentPointer, RecordingUploader>;
        type Consumer = RecordingConsumer;
        type Uploader = RecordingUploader;

        async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
            let fragment_uploader = self.make_fragment_uploader().await?;
            BatchManager::new(LogWriterOptions::default(), fragment_uploader)
                .ok_or_else(|| Error::internal(file!(), line!()))
        }

        async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
            Ok(RecordingConsumer)
        }

        async fn make_fragment_uploader(&self) -> Result<Self::Uploader, Error> {
            Ok(RecordingUploader {
                storage: self.storage.clone(),
                upload_attempts: Arc::clone(&self.upload_attempts),
            })
        }

        async fn preferred_storage(&self) -> Storage {
            self.storage.clone()
        }

        fn write_options(&self) -> LogWriterOptions {
            LogWriterOptions::default()
        }
    }

    #[tokio::test]
    async fn fault_injecting_fragment_manager_factory_short_circuits_uploads() {
        let storage = Storage::Local(chroma_storage::local::LocalStorage::new("/tmp"));
        let upload_attempts = Arc::new(AtomicUsize::new(0));
        let factory = FaultInjectingFragmentManagerFactory::new(
            RecordingFactory {
                storage,
                upload_attempts: Arc::clone(&upload_attempts),
            },
            Some(Arc::new(InjectUnavailable)),
        );

        let publisher = factory.make_publisher().await.expect("publisher");
        let err = publisher
            .upload_parquet(
                &(FragmentSeqNo::BEGIN, LogPosition::from_offset(1)),
                vec![b"hello".to_vec()],
                None,
                0,
            )
            .await
            .expect_err("fault injection should reject upload");

        assert!(
            matches!(&err, Error::TonicError(status) if status.code() == tonic::Code::Unavailable)
        );
        assert_eq!(upload_attempts.load(Ordering::Relaxed), 0);
    }
}
