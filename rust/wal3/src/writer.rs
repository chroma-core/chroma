use std::future::Future;
use std::iter::Iterator;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{PutMode, PutOptions, Storage, StorageError};
use chroma_types::Cmek;
use opentelemetry::trace::TraceContextExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use setsum::Setsum;
use tracing::{Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    unprefixed_fragment_path, BatchManager, CursorStore, CursorStoreOptions, Error,
    ExponentialBackoff, Fragment, FragmentIdentifier, Garbage, GarbageCollectionOptions,
    LogPosition, LogReader, LogReaderOptions, LogWriterOptions, Manifest, ManifestAndETag,
    ManifestManager, ThrottleOptions,
};

/// The epoch writer is a counting writer.  Every epoch exists.  An epoch goes
/// unused->used->discarded.  The epoch of a writer is used to determine if and when log contention
/// indicates that a new writer should be created.  The epoch is incremented when a new writer is
/// created and checked before creating a new writer.
#[derive(Clone, Default)]
pub struct EpochWriter {
    epoch: u64,
    writer: Option<Arc<OnceLogWriter>>,
}

///////////////////////////////////////////// MarkDirty ////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MarkDirty: Send + Sync + 'static {
    async fn mark_dirty(&self, log_position: LogPosition, num_records: usize) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl MarkDirty for () {
    async fn mark_dirty(&self, _: LogPosition, _: usize) -> Result<(), Error> {
        Ok(())
    }
}

///////////////////////////////////////////// LogWriter ////////////////////////////////////////////

pub struct LogWriter {
    options: LogWriterOptions,
    storage: Arc<Storage>,
    prefix: String,
    writer: String,
    mark_dirty: Arc<dyn MarkDirty>,
    inner: Mutex<EpochWriter>,
    reopen_protection: tokio::sync::Mutex<()>,
    cmek: Option<Cmek>,
}

impl LogWriter {
    pub async fn initialize(
        options: &LogWriterOptions,
        storage: &Storage,
        prefix: &str,
        writer: &str,
    ) -> Result<(), Error> {
        Manifest::initialize(options, storage, prefix, writer).await
    }

    /// Open the log, possibly writing a new manifest to recover it.
    pub async fn open<D: MarkDirty>(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: &str,
        writer: &str,
        mark_dirty: D,
        cmek: Option<Cmek>,
    ) -> Result<Self, Error> {
        let mark_dirty = Arc::new(mark_dirty) as _;
        let inner = EpochWriter::default();
        let prefix = prefix.to_string();
        let writer = writer.to_string();
        let reopen_protection = tokio::sync::Mutex::new(());
        let this = Self {
            options,
            storage,
            prefix,
            writer,
            mark_dirty,
            inner: Mutex::new(inner),
            reopen_protection,
            cmek,
        };
        this.ensure_open().await?;
        Ok(this)
    }

    /// Open or try once to initialize the log.
    pub async fn open_or_initialize<D: MarkDirty>(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: &str,
        writer: &str,
        mark_dirty: D,
        cmek: Option<Cmek>,
    ) -> Result<Self, Error> {
        let mark_dirty = Arc::new(mark_dirty) as _;
        let inner = EpochWriter::default();
        let prefix = prefix.to_string();
        let writer = writer.to_string();
        let reopen_protection = tokio::sync::Mutex::new(());
        let this = Self {
            options,
            storage,
            prefix,
            writer,
            mark_dirty,
            inner: Mutex::new(inner),
            reopen_protection,
            cmek,
        };
        match this.ensure_open().await {
            Ok(_) => {}
            Err(Error::UninitializedLog) => {
                Self::initialize(&this.options, &this.storage, &this.prefix, &this.writer).await?;
                this.ensure_open().await?;
            }
            Err(err) => {
                return Err(err);
            }
        }
        Ok(this)
    }

    /// Given a contiguous subset of data from some other location (preferably another log),
    /// construct a new log under storage/prefix using the provided options.
    ///
    /// This function is safe to run again on failure and will not bootstrap over a partially
    /// bootstrapped collection.
    ///
    /// It is my intention to make this more robust as time goes on.  Concretely, that means that
    /// as we encounter partial failures left by the tool we fix them.  There are 3 failure points
    /// and I'd prefer to manually inspect failures than get the automation right to do it always
    /// automatically.  Bootstrap is intended only to last as long as there is a migration from the
    /// go to the rust log services.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap<D: MarkDirty>(
        options: &LogWriterOptions,
        storage: &Arc<Storage>,
        prefix: &str,
        writer: &str,
        mark_dirty: D,
        first_record_offset: LogPosition,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
    ) -> Result<(), Error> {
        let num_records = messages.len();
        let start = first_record_offset;
        let limit = first_record_offset + num_records;
        // SAFETY(rescrv):  This is a speculative load to narrow the window in which we would see a
        // race between writers.
        let manifest = Manifest::load(&ThrottleOptions::default(), storage, prefix).await?;
        if manifest.is_some() {
            return Err(Error::LogContentionFailure);
        }
        // SAFETY(rescrv):  This will only succeed if the file doesn't exist.  Technically the log
        // could be initialized and garbage collected to leave a prefix hole, but our timing
        // assumption is that every op happens in less than 1/2 the GC interval, so there's no way
        // for that to happen.
        //
        // If the file exists, this will fail with LogContention, which fails us with
        // LogContention.  Other errors fail transparently, too.
        if num_records > 0 {
            let (path, setsum, num_bytes) = upload_parquet(
                options,
                storage,
                prefix,
                FragmentIdentifier::SeqNo(1),
                Some(first_record_offset),
                messages,
                cmek,
            )
            .await?;
            let seq_no = FragmentIdentifier::SeqNo(1);
            let num_bytes = num_bytes as u64;
            let frag = Fragment {
                path,
                seq_no,
                start,
                limit,
                num_bytes,
                setsum,
            };
            let empty_manifest = Manifest::new_empty(writer);
            let mut new_manifest = empty_manifest.clone();
            new_manifest.initial_offset = Some(start);
            // SAFETY(rescrv):  This is unit tested to never happen.  If it happens, add more tests.
            if !new_manifest.can_apply_fragment(&frag) {
                tracing::error!("Cannot apply frag to a clean manifest.");
                return Err(Error::internal(file!(), line!()));
            }
            new_manifest.apply_fragment(frag);
            // SAFETY(rescrv):  If this fails, there's nothing left to do.
            empty_manifest
                .install(
                    //TODO(rescrv): Thread throttle options.
                    &ThrottleOptions::default(),
                    storage,
                    prefix,
                    None,
                    &new_manifest,
                )
                .await?;
            // Not Safety:
            // We mark dirty, but if we lose that we lose that.
            // Failure to mark dirty fails the bootstrap.
            mark_dirty.mark_dirty(start, num_records).await?;
        } else {
            let empty_manifest = Manifest::new_empty("bootstrap");
            let mut new_manifest = empty_manifest.clone();
            new_manifest.initial_offset = Some(start);
            // SAFETY(rescrv):  If this fails, there's nothing left to do.
            empty_manifest
                .install(
                    //TODO(rescrv): Thread throttle options.
                    &ThrottleOptions::default(),
                    storage,
                    prefix,
                    None,
                    &new_manifest,
                )
                .await?;
            // No need to mark dirty as the manifest is empty.
        }
        Ok(())
    }

    /// This will close the log.
    pub async fn close(self) -> Result<(), Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let writer = { self.inner.lock().unwrap().writer.take() };
        if let Some(writer) = writer {
            writer.close().await
        } else {
            Ok(())
        }
    }

    /// Append a message to a log.
    pub async fn append(&self, message: Vec<u8>) -> Result<LogPosition, Error> {
        self.append_many(vec![message]).await
    }

    #[tracing::instrument(skip(self, messages))]
    pub async fn append_many(&self, messages: Vec<Vec<u8>>) -> Result<LogPosition, Error> {
        let once_log_append_many = move |log: &Arc<OnceLogWriter>| {
            let messages = messages.clone();
            let log = Arc::clone(log);
            async move { log.append(messages).await }
        };
        self.handle_errors_and_contention(once_log_append_many)
            .await
    }

    // TODO(rescrv):  No option
    pub fn reader(&self, options: LogReaderOptions) -> Option<LogReader> {
        Some(LogReader::new(
            options,
            Arc::clone(&self.storage),
            self.prefix.clone(),
        ))
    }

    // TODO(rescrv):  No option
    pub fn cursors(&self, options: CursorStoreOptions) -> Option<CursorStore> {
        Some(CursorStore::new(
            options,
            Arc::clone(&self.storage),
            self.prefix.clone(),
            self.writer.clone(),
        ))
    }

    pub fn manifest(&self) -> Option<Manifest> {
        self.manifest_and_etag().map(|m| m.manifest)
    }

    pub fn manifest_and_etag(&self) -> Option<ManifestAndETag> {
        // SAFETY(rescrv):  Mutex poisoning.
        let inner = self.inner.lock().unwrap();
        inner
            .writer
            .as_ref()
            .map(|writer| writer.manifest_manager.latest())
    }

    pub async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error> {
        let once_log_garbage_collect = move |log: &Arc<OnceLogWriter>| {
            let options = options.clone();
            let log = Arc::clone(log);
            async move {
                log.garbage_collect_phase1_compute_garbage(&options, keep_at_least)
                    .await
            }
        };
        self.handle_errors_and_contention(once_log_garbage_collect)
            .await
    }

    pub async fn garbage_collect_phase2_update_manifest(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        let once_log_garbage_collect = move |log: &Arc<OnceLogWriter>| {
            let options = options.clone();
            let log = Arc::clone(log);
            async move { log.garbage_collect_phase2_update_manifest(&options).await }
        };
        self.handle_errors_and_contention(once_log_garbage_collect)
            .await
    }

    pub async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        let once_log_garbage_collect = move |log: &Arc<OnceLogWriter>| {
            let options = options.clone();
            let log = Arc::clone(log);
            async move { log.garbage_collect_phase3_delete_garbage(&options).await }
        };
        self.handle_errors_and_contention(once_log_garbage_collect)
            .await
    }

    pub async fn garbage_collect(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<(), Error> {
        let once_log_garbage_collect = move |log: &Arc<OnceLogWriter>| {
            let options = options.clone();
            let log = Arc::clone(log);
            async move { log.garbage_collect(&options, keep_at_least).await }
        };
        self.handle_errors_and_contention(once_log_garbage_collect)
            .await
    }

    async fn handle_errors_and_contention<O, F: Future<Output = Result<O, Error>>>(
        &self,
        f: impl Fn(&Arc<OnceLogWriter>) -> F,
    ) -> Result<O, Error> {
        for _ in 0..3 {
            let (writer, epoch) = self.ensure_open().await?;
            match f(&writer).await {
                Ok(out) => {
                    return Ok(out);
                }
                Err(Error::LogContentionDurable) => {
                    {
                        // SAFETY(rescrv):  Mutex poisoning.
                        let mut inner = self.inner.lock().unwrap();
                        if inner.epoch == epoch {
                            if let Some(writer) = inner.writer.take() {
                                writer.shutdown();
                            }
                        }
                    }
                    // Silence this error in favor of the one we got from f.
                    if self.ensure_open().await.is_ok() {
                        return Err(Error::LogContentionDurable);
                    } else {
                        return Err(Error::LogContentionFailure);
                    }
                }
                Err(Error::LogContentionFailure) => {
                    // SAFETY(rescrv):  Mutex poisoning.
                    let mut inner = self.inner.lock().unwrap();
                    if inner.epoch == epoch {
                        if let Some(writer) = inner.writer.take() {
                            writer.shutdown();
                        }
                    }
                    return Err(Error::LogContentionFailure);
                }
                Err(Error::LogContentionRetry) => {
                    // SAFETY(rescrv):  Mutex poisoning.
                    let mut inner = self.inner.lock().unwrap();
                    if inner.epoch == epoch {
                        if let Some(writer) = inner.writer.take() {
                            writer.shutdown();
                        }
                    }
                }
                Err(Error::Backoff) => {
                    return Err(Error::Backoff);
                }
                Err(err) => {
                    let mut inner = self.inner.lock().unwrap();
                    if inner.epoch == epoch {
                        if let Some(writer) = inner.writer.take() {
                            writer.shutdown();
                        }
                    }
                    return Err(err);
                }
            }
        }
        Err(Error::LogContentionFailure)
    }

    async fn ensure_open(&self) -> Result<(Arc<OnceLogWriter>, u64), Error> {
        let _guard = self.reopen_protection.lock().await;
        for _ in 0..3 {
            let epoch = {
                // SAFETY(rescrv):  Mutex poisoning.
                let mut inner = self.inner.lock().unwrap();
                if let Some(writer) = inner.writer.as_ref() {
                    if !writer.done.load(std::sync::atomic::Ordering::Relaxed) {
                        return Ok((Arc::clone(writer), inner.epoch));
                    } else {
                        writer.shutdown();
                        inner.writer.take();
                        inner.epoch += 1;
                        continue;
                    }
                }
                inner.epoch
            };
            let writer = match OnceLogWriter::open(
                self.options.clone(),
                self.storage.clone(),
                self.prefix.clone(),
                self.writer.clone(),
                Arc::clone(&self.mark_dirty),
                self.cmek.clone(),
            )
            .await
            {
                Ok(writer) => writer,
                Err(Error::LogContentionRetry) => continue,
                Err(err) => return Err(err),
            };
            // SAFETY(rescrv):  Mutex poisoning.
            let mut inner = self.inner.lock().unwrap();
            if inner.epoch == epoch && inner.writer.is_none() {
                inner.epoch += 1;
                if let Some(writer) = inner.writer.take() {
                    writer.shutdown();
                }
                inner.writer = Some(Arc::clone(&writer));
                return Ok((writer, inner.epoch));
            }
        }
        Err(Error::LogContentionRetry)
    }

    pub fn count_waiters(&self) -> Option<(usize, usize)> {
        // SAFETY(rescrv):  Mutex poisoning.
        let inner = self.inner.lock().unwrap();
        inner.writer.as_ref().map(|writer| {
            (
                writer.batch_manager.count_waiters(),
                writer.manifest_manager.count_waiters(),
            )
        })
    }

    pub fn debug_dump(&self) -> String {
        // SAFETY(rescrv):  Mutex poisoning.
        let inner = self.inner.lock().unwrap();
        let Some(writer) = inner.writer.as_ref() else {
            return "<no writer>\n".to_string();
        };
        let mut output = String::new();
        output += &writer.batch_manager.debug_dump();
        output += &writer.manifest_manager.debug_dump();
        output
    }
}

impl std::fmt::Debug for LogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogWriter")
            .field("writer", &self.writer)
            .finish()
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(writer) = inner.writer.as_mut() {
            writer.shutdown();
        }
    }
}

/////////////////////////////////////////// OnceLogWriter //////////////////////////////////////////

/// OnceLogWriter writes to a log once until contention is discovered.  It must then be thrown away
/// and recovered.  Because throw-away and recovery have the exact same network round-trip
/// structure as the recovery procedure does, this allows us to re-use exactly one code path for
/// both.  That code path can then be well-tested because any contention state gets exercised from
/// the perspective of initialization.
pub(crate) struct OnceLogWriter {
    /// LogWriterOptions are fixed at log creation time.
    /// LogWriter is intentionally cheap to construct and destroy.
    /// Reopen the log to change the options.
    options: LogWriterOptions,
    /// A chroma object store.
    storage: Arc<Storage>,
    /// The prefix to store the log under in object storage.
    prefix: String,
    /// True iff the log is done.
    done: AtomicBool,
    /// Mark each write dirty via this mechanism.
    mark_dirty: Arc<dyn MarkDirty>,
    /// ManifestManager coordinates updates to the manifest.
    manifest_manager: ManifestManager,
    /// BatchManager coordinates batching writes to the log.
    batch_manager: BatchManager,
    /// Customer-managed encryption key for encrypting log fragments.
    cmek: Option<Cmek>,
}

impl OnceLogWriter {
    async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
        mark_dirty: Arc<dyn MarkDirty>,
        cmek: Option<Cmek>,
    ) -> Result<Arc<Self>, Error> {
        let done = AtomicBool::new(false);
        let batch_manager = BatchManager::new(options.throttle_fragment)
            .ok_or_else(|| Error::internal(file!(), line!()))?;
        let mut manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            options.snapshot_manifest,
            Arc::clone(&storage),
            prefix.clone(),
            writer,
        )
        .await?;
        manifest_manager.recover(&*mark_dirty).await?;
        let this = Arc::new(Self {
            options,
            storage,
            prefix,
            done,
            mark_dirty,
            manifest_manager,
            batch_manager,
            cmek,
        });
        let that = Arc::downgrade(&this);
        let _flusher = tokio::task::spawn(async move {
            loop {
                let Some(that) = that.upgrade() else {
                    break;
                };
                if !that.done.load(std::sync::atomic::Ordering::Relaxed) {
                    that.batch_manager.wait_for_writable().await;
                    match that.batch_manager.take_work(&that.manifest_manager) {
                        Ok(Some((fragment_identifier, log_position, work))) => {
                            Arc::clone(&that)
                                .append_batch(fragment_identifier, log_position, work)
                                .await;
                        }
                        Ok(None) => {
                            let sleep_for = that.batch_manager.until_next_time();
                            drop(that);
                            tokio::time::sleep(sleep_for).await;
                        }
                        Err(err) => {
                            let sleep_for = that.batch_manager.until_next_time();
                            drop(that);
                            tracing::error!("batch_manager.take_work: {:?}", err);
                            tokio::time::sleep(sleep_for).await;
                        }
                    }
                } else {
                    break;
                }
            }
        });
        Ok(this)
    }

    pub(crate) async fn open_for_read_only_and_stale_ops(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
        mark_dirty: Arc<dyn MarkDirty>,
    ) -> Result<Arc<Self>, Error> {
        let done = AtomicBool::new(false);
        let batch_manager = BatchManager::new(options.throttle_fragment)
            .ok_or_else(|| Error::internal(file!(), line!()))?;
        let manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            options.snapshot_manifest,
            Arc::clone(&storage),
            prefix.clone(),
            writer,
        )
        .await?;
        Ok(Arc::new(Self {
            options,
            storage,
            prefix,
            done,
            mark_dirty,
            manifest_manager,
            batch_manager,
            cmek: None, // Read-only operations don't need CMEK
        }))
    }

    fn shutdown(&self) {
        self.batch_manager.shutdown();
        self.manifest_manager.shutdown();
        self.done.store(true, std::sync::atomic::Ordering::Relaxed);
        self.batch_manager.pump_write_finished();
    }

    async fn close(mut self: Arc<Self>) -> Result<(), Error> {
        self.shutdown();
        loop {
            match Arc::try_unwrap(self) {
                Ok(_) => {
                    break;
                }
                Err(arc) => {
                    std::thread::sleep(Duration::from_millis(100));
                    self = arc;
                }
            }
        }
        Ok(())
    }

    async fn append(self: &Arc<Self>, messages: Vec<Vec<u8>>) -> Result<LogPosition, Error> {
        if messages.is_empty() {
            return Err(Error::EmptyBatch);
        }
        let append_span = tracing::info_span!("append_span");
        let append_span_clone = append_span.clone();
        async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.batch_manager.push_work(messages, tx, append_span);
            match self.batch_manager.take_work(&self.manifest_manager) {
                Ok(Some(work)) => {
                    let (fragment_identifier, log_position, work) = work;
                    {
                        tokio::task::spawn(Arc::clone(self).append_batch(
                            fragment_identifier,
                            log_position,
                            work,
                        ));
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::error!(error = %err, "batch manager failed");
                }
            }
            let span = tracing::info_span!("wait_for_durability");
            rx.instrument(span)
                .await
                .map_err(|_| Error::internal(file!(), line!()))?
        }
        .instrument(append_span_clone)
        .await
    }

    #[allow(clippy::type_complexity)]
    async fn append_batch(
        self: Arc<Self>,
        fragment_identifier: FragmentIdentifier,
        log_position: LogPosition,
        work: Vec<(
            Vec<Vec<u8>>,
            tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
            Span,
        )>,
    ) {
        let append_batch_span = tracing::info_span!("append_batch");
        let mut messages = Vec::with_capacity(work.len());
        let mut notifies = Vec::with_capacity(work.len());
        for work in work.into_iter() {
            notifies.push((work.0.len(), work.1));
            messages.extend(work.0);
            // NOTE(rescrv):  This returns a context that returns a reference to the span, from
            // which we get a span context that we clone.  My initial read of this was to interpret
            // it as creating a span and that is not the case.
            work.2
                .add_link(append_batch_span.context().span().span_context().clone());
        }
        async move {
            if notifies.is_empty() {
                tracing::error!("somehow got empty messages");
                return;
            }
            match self
                .append_batch_internal(fragment_identifier, log_position, messages)
                .await
            {
                Ok(mut log_position) => {
                    for (num_messages, notify) in notifies.into_iter() {
                        if notify.send(Ok(log_position)).is_err() {
                            // TODO(rescrv):  Counter this.
                        }
                        log_position += num_messages;
                    }
                }
                Err(e) => {
                    for (_, notify) in notifies.into_iter() {
                        if notify.send(Err(e.clone())).is_err() {
                            // TODO(rescrv):  Counter this.
                        }
                    }
                }
            }
        }
        .instrument(append_batch_span)
        .await
    }

    #[tracing::instrument(skip(self, messages))]
    async fn append_batch_internal(
        &self,
        fragment_identifier: FragmentIdentifier,
        log_position: LogPosition,
        messages: Vec<Vec<u8>>,
    ) -> Result<LogPosition, Error> {
        assert!(!messages.is_empty());
        let messages_len = messages.len();
        let fut1 = upload_parquet(
            &self.options,
            &self.storage,
            &self.prefix,
            fragment_identifier,
            Some(log_position),
            messages,
            self.cmek.clone(),
        );
        let fut2 = async {
            match self.mark_dirty.mark_dirty(log_position, messages_len).await {
                Ok(_) | Err(Error::LogContentionDurable) => Ok(()),
                Err(err) => Err(err),
            }
        };
        let (res1, res2) = futures::future::join(fut1, fut2).await;
        res2.inspect_err(|_| {
            self.shutdown();
        })?;
        let (path, setsum, num_bytes) = res1.inspect_err(|_| {
            self.shutdown();
        })?;
        // Upload to a coalesced manifest.
        let fragment = Fragment {
            path: path.to_string(),
            seq_no: fragment_identifier,
            start: log_position,
            limit: log_position + messages_len,
            num_bytes: num_bytes as u64,
            setsum,
        };
        self.manifest_manager
            .publish_fragment(fragment)
            .await
            .inspect_err(|_| {
                self.shutdown();
            })?;
        // Record the records/batches written.
        self.batch_manager.finish_write();
        Ok(log_position)
    }

    /// Perform phase 1 of garbage collection.
    ///
    /// Pre-condition:  manifest/MANIFEST exists.
    ///
    /// Post-condition:
    /// - gc/GARBAGE exists as a non-empty file.
    /// - snapshots created by gc/GARBAGE get created.
    ///
    /// Returns Ok(false) if there is no garbage to act upon (e.g., it's already been collected).
    /// Returns Ok(true) if there is garbage to act upon.
    #[tracing::instrument(skip(self, options))]
    pub(crate) async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error> {
        let cutoff = self.garbage_collection_cutoff().await?;
        let cutoff = if let Some(keep_at_least) = keep_at_least {
            keep_at_least.min(cutoff)
        } else {
            cutoff
        };
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 3 {
                return Err(Error::LogContentionFailure);
            }
            let garbage_and_e_tag = match Garbage::load(
                &self.options.throttle_manifest,
                &self.storage,
                &self.prefix,
            )
            .await
            {
                Ok(Some((garbage, e_tag))) => {
                    if garbage.is_empty() || self.manifest_manager.garbage_applies_cleanly(&garbage)
                    {
                        Some((garbage, e_tag))
                    } else if let Some(e_tag) = e_tag {
                        tracing::info!("resetting garbage because a concurrent snapshot write invalidated prior garbage");
                        garbage
                            .reset(
                                &self.options.throttle_manifest,
                                &self.storage,
                                &self.prefix,
                                &e_tag,
                            )
                            .await?;
                        continue;
                    } else {
                        return Err(Error::GarbageCollection(
                            "non-empty garbage without ETag".to_string(),
                        ));
                    }
                }
                Ok(None) => None,
                Err(err) => {
                    return Err(err);
                }
            };
            let e_tag = if let Some((garbage, e_tag)) = garbage_and_e_tag {
                if !garbage.is_empty() {
                    return Ok(true);
                }
                e_tag
            } else {
                None
            };
            let garbage = self
                .manifest_manager
                .compute_garbage(options, cutoff, &())
                .await?;
            let Some(garbage) = garbage else {
                return Ok(false);
            };
            match garbage
                .install(
                    &self.options.throttle_manifest,
                    &self.storage,
                    &self.prefix,
                    e_tag.as_ref(),
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(Error::LogContentionFailure)
                | Err(Error::LogContentionRetry)
                | Err(Error::LogContentionDurable) => {}
                Err(err) => {
                    return Err(err);
                }
            };
        }
    }

    /// Perform phase 2 of grabage collection.
    ///
    /// Pre-conditions:
    /// - manifest/MANIFEST exists.
    /// - gc/GARBAGE exists.
    ///
    /// Post-condition:
    /// - contents of gc/GARBAGE are removed from manifest/MANIFEST.
    #[tracing::instrument(skip(self, _options))]
    pub(crate) async fn garbage_collect_phase2_update_manifest(
        &self,
        _options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        let (garbage, _) =
            match Garbage::load(&self.options.throttle_manifest, &self.storage, &self.prefix).await
            {
                Ok(Some((garbage, e_tag))) => (garbage, e_tag),
                Ok(None) => return Ok(()),
                Err(err) => {
                    return Err(err);
                }
            };
        if !garbage.is_empty() {
            self.manifest_manager.apply_garbage(garbage.clone()).await.inspect_err(|err| {
                if let Error::GarbageCollectionPrecondition(_) = err {
                    tracing::event!(Level::ERROR, name = "garbage collection precondition failed", manifest =? self.manifest_manager.latest(), garbage =? garbage);
                }
            })?;
        }
        Ok(())
    }

    /// Perform phase 3 of garbage collection.
    ///
    /// Pre-conditions:
    /// - manifest/MANIFEST exists
    /// - gc/GARBAGE exists
    /// - manifest/MANIFEST does not reference any part of gc/GARBAGE
    ///
    /// Post-condition:
    /// - gc/GARBAGE and the files it references get deleted.
    #[tracing::instrument(skip(self, options))]
    pub(crate) async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        let exp_backoff: ExponentialBackoff = options.throttle.into();
        let start = Instant::now();
        let (garbage, e_tag) =
            match Garbage::load(&self.options.throttle_manifest, &self.storage, &self.prefix).await
            {
                Ok(Some((garbage, e_tag))) => (garbage, e_tag),
                Ok(None) => return Ok(()),
                Err(err) => {
                    return Err(err);
                }
            };
        let Some(e_tag) = e_tag.as_ref() else {
            return Err(Error::GarbageCollection(
                "loaded garbage without e_tag".to_string(),
            ));
        };
        let mut batch = vec![];
        let delete_batch = |batch: Vec<String>, exp_backoff: ExponentialBackoff| {
            let storage = Arc::clone(&self.storage);
            async move {
                let paths = batch.iter().map(String::as_str).collect::<Vec<_>>();
                loop {
                    match storage.delete_many(&paths).await {
                        Ok(mut deleted_objects) => {
                            for err in deleted_objects.errors.iter() {
                                tracing::error!(error = ?err, "could not clean up");
                            }
                            if let Some(err) = deleted_objects.errors.pop() {
                                return Err(Arc::new(err).into());
                            } else {
                                return Ok(());
                            }
                        }
                        Err(StorageError::NotFound { .. }) => break,
                        Err(err) => {
                            tracing::error!("could not cleanup garbage: {err:?}");
                            if start.elapsed() > Duration::from_secs(600) {
                                tracing::error!(
                                    "could not cleanup garbage within 10 minutes, returning"
                                );
                                return Err(Error::StorageError(Arc::new(err)));
                            }
                            let mut backoff = exp_backoff.next();
                            if backoff > Duration::from_secs(600) {
                                backoff = Duration::from_secs(600);
                            }
                            tokio::time::sleep(backoff).await;
                        }
                    }
                }
                Ok(())
            }
        };
        for path in garbage.prefixed_paths_to_delete(&self.prefix) {
            batch.push(path);
            if batch.len() >= 100 {
                let batch = std::mem::take(&mut batch);
                delete_batch(batch, exp_backoff.clone()).await?;
            }
        }
        if !batch.is_empty() {
            delete_batch(batch, exp_backoff.clone()).await?;
        }
        garbage
            .reset(
                &self.options.throttle_manifest,
                &self.storage,
                &self.prefix,
                e_tag,
            )
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn garbage_collect(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<(), Error> {
        self.garbage_collect_phase1_compute_garbage(options, keep_at_least)
            .await?;
        self.garbage_collect_phase2_update_manifest(options).await?;
        self.garbage_collect_phase3_delete_garbage(options).await?;
        Ok(())
    }

    // NOTE(rescrv): Garbage collection cutoff is responsible for determining the crucial amount of
    // how much to garbage collect.  If there are no cursors, it must bail with an error.
    async fn garbage_collection_cutoff(&self) -> Result<LogPosition, Error> {
        let cursors = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            "garbage collection writer".to_string(),
        );
        // This will be None if there are no cursors, upholding the function invariant.
        let mut collect_up_to = None;
        for cursor_name in cursors.list().await? {
            let witness = cursors.load(&cursor_name).await?;
            let Some(cursor) = witness.map(|w| w.cursor) else {
                return Err(Error::LogContentionFailure);
            };
            if cursor.position <= collect_up_to.unwrap_or(cursor.position) {
                collect_up_to = Some(cursor.position);
            }
        }
        let Some(collect_up_to) = collect_up_to else {
            return Err(Error::NoSuchCursor(format!(
                "there is no cursor for prefix {}",
                self.prefix
            )));
        };
        Ok(collect_up_to)
    }
}

impl Drop for OnceLogWriter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[tracing::instrument(skip(messages))]
pub fn construct_parquet(
    log_position: Option<LogPosition>,
    messages: &[Vec<u8>],
) -> Result<(Vec<u8>, Setsum), Error> {
    // Construct the columns and construct the setsum.
    let mut setsum = Setsum::default();
    let messages_len = messages.len();
    let mut positions = Vec::with_capacity(messages_len);
    let mut bodies = Vec::with_capacity(messages_len);
    let epoch_micros = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros() as u64;
    let relative = log_position.is_none();
    let log_position = log_position.unwrap_or_default();
    for (index, message) in messages.iter().enumerate() {
        let position = log_position + index;
        setsum.insert_vectored(&[
            &position.offset.to_be_bytes(),
            &epoch_micros.to_be_bytes(),
            message.as_slice(),
        ]);
        positions.push(position);
        bodies.push(message.as_slice());
    }
    let offsets = positions.iter().map(|p| p.offset).collect::<Vec<_>>();
    let timestamps_us = vec![epoch_micros; offsets.len()];

    // Create an Arrow record batch
    let offsets = UInt64Array::from(offsets);
    let timestamps_us = UInt64Array::from(timestamps_us);
    let bodies = BinaryArray::from(bodies);
    let offset_column_name = if relative {
        "relative_offset"
    } else {
        "offset"
    };
    // SAFETY(rescrv):  The try_from_iter call will always succeed.
    // TODO(rescrv):  Arrow pre-allocator.
    let batch = RecordBatch::try_from_iter(vec![
        (offset_column_name, Arc::new(offsets) as ArrayRef),
        ("timestamp_us", Arc::new(timestamps_us) as ArrayRef),
        ("body", Arc::new(bodies) as ArrayRef),
    ])
    .unwrap();

    // Write to parquet.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).map_err(Arc::new)?;
    writer.close().map_err(Arc::new)?;
    Ok((buffer, setsum))
}

#[tracing::instrument(skip(options, storage, messages))]
pub async fn upload_parquet(
    options: &LogWriterOptions,
    storage: &Storage,
    prefix: &str,
    fragment_identifier: FragmentIdentifier,
    log_position: Option<LogPosition>,
    messages: Vec<Vec<u8>>,
    cmek: Option<Cmek>,
) -> Result<(String, Setsum, usize), Error> {
    // Upload the log.
    let unprefixed_path = unprefixed_fragment_path(fragment_identifier);
    let path = format!("{prefix}/{unprefixed_path}");
    let exp_backoff: ExponentialBackoff = options.throttle_fragment.into();
    let start = Instant::now();
    let (buffer, setsum) = construct_parquet(log_position, &messages)?;
    let mut put_options = PutOptions::default()
        .with_priority(StorageRequestPriority::P0)
        .with_mode(PutMode::IfNotExist);
    if let Some(cmek) = cmek {
        put_options = put_options.with_cmek(cmek);
    }
    loop {
        tracing::info!("upload_parquet: {:?} with {} bytes", path, buffer.len());
        // NOTE(rescrv):  This match block has been thoroughly reasoned through within the
        // `bootstrap` call above.  Don't change the error handling here without re-reasoning
        // there.
        match storage
            .put_bytes(&path, buffer.clone(), put_options.clone())
            .await
        {
            Ok(_) => {
                return Ok((unprefixed_path, setsum, buffer.len()));
            }
            // NOTE(sicheng): Permission denied requests should continue to fail if retried
            Err(err @ StorageError::PermissionDenied { .. }) => {
                return Err(Error::StorageError(Arc::new(err)));
            }
            Err(StorageError::Precondition { path: _, source: _ }) => {
                return Err(Error::LogContentionFailure);
            }
            Err(err) => {
                tracing::error!(
                    error.message = err.to_string(),
                    "failed to upload parquet, backing off"
                );
                // NOTE(sicheng): The frontend will fail the request on its end if we retry for too long here
                // TODO(sicheng): Organize the magic numbers in the code at one place
                if start.elapsed() > Duration::from_secs(20) {
                    return Err(Error::StorageError(Arc::new(err)));
                }
                let mut backoff = exp_backoff.next();
                if backoff > Duration::from_secs(10) {
                    backoff = Duration::from_secs(10);
                }
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Array;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// Verifies that construct_parquet with Some(log_position) creates absolute offsets.
    #[test]
    fn construct_parquet_with_some_log_position_uses_absolute_offsets() {
        let log_position = LogPosition::from_offset(100);
        let messages = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];

        let (buffer, setsum) = construct_parquet(Some(log_position), &messages)
            .expect("construct_parquet should succeed");

        // Verify that the parquet buffer is non-empty.
        assert!(!buffer.is_empty(), "parquet buffer should not be empty");

        // Verify the setsum is non-default (data was inserted).
        assert_ne!(setsum, Setsum::default(), "setsum should not be default");

        // Parse the parquet and verify the column structure.
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(buffer))
            .expect("parquet should be parseable");
        let reader = builder.build().expect("parquet reader should build");

        for batch in reader {
            let batch = batch.expect("batch should be readable");

            // Verify that "offset" column exists (not "relative_offset").
            let offset_column = batch.column_by_name("offset");
            assert!(
                offset_column.is_some(),
                "should have 'offset' column for Some(log_position)"
            );

            let relative_offset_column = batch.column_by_name("relative_offset");
            assert!(
                relative_offset_column.is_none(),
                "should not have 'relative_offset' column for Some(log_position)"
            );

            // Verify offset values are absolute (starting at 100).
            let offset_array = offset_column
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("offset column should be UInt64Array");

            assert_eq!(offset_array.len(), 3, "should have 3 records");
            println!(
                "construct_parquet_with_some_log_position_uses_absolute_offsets: offsets = {:?}",
                (0..offset_array.len())
                    .map(|i| offset_array.value(i))
                    .collect::<Vec<_>>()
            );
            assert_eq!(offset_array.value(0), 100, "first offset should be 100");
            assert_eq!(offset_array.value(1), 101, "second offset should be 101");
            assert_eq!(offset_array.value(2), 102, "third offset should be 102");

            // Verify body column exists and has correct data.
            let body_column = batch
                .column_by_name("body")
                .expect("body column should exist");
            let body_array = body_column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("body column should be BinaryArray");

            assert_eq!(body_array.value(0), &[1, 2, 3]);
            assert_eq!(body_array.value(1), &[4, 5, 6]);
            assert_eq!(body_array.value(2), &[7, 8, 9]);
        }
    }

    /// Verifies that construct_parquet with None creates relative offsets.
    #[test]
    fn construct_parquet_with_none_log_position_uses_relative_offsets() {
        let messages = vec![
            vec![10, 20, 30],
            vec![40, 50, 60],
            vec![70, 80, 90],
            vec![100, 110, 120],
        ];

        let (buffer, setsum) =
            construct_parquet(None, &messages).expect("construct_parquet should succeed");

        // Verify that the parquet buffer is non-empty.
        assert!(!buffer.is_empty(), "parquet buffer should not be empty");

        // Verify the setsum is non-default (data was inserted).
        assert_ne!(setsum, Setsum::default(), "setsum should not be default");

        // Parse the parquet and verify the column structure.
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(buffer))
            .expect("parquet should be parseable");
        let reader = builder.build().expect("parquet reader should build");

        for batch in reader {
            let batch = batch.expect("batch should be readable");

            // Verify that "relative_offset" column exists (not "offset").
            let relative_offset_column = batch.column_by_name("relative_offset");
            assert!(
                relative_offset_column.is_some(),
                "should have 'relative_offset' column for None log_position"
            );

            let offset_column = batch.column_by_name("offset");
            assert!(
                offset_column.is_none(),
                "should not have 'offset' column for None log_position"
            );

            // Verify relative offset values start at 0.
            let relative_offset_array = relative_offset_column
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("relative_offset column should be UInt64Array");

            assert_eq!(relative_offset_array.len(), 4, "should have 4 records");
            println!(
                "construct_parquet_with_none_log_position_uses_relative_offsets: relative_offsets = {:?}",
                (0..relative_offset_array.len())
                    .map(|i| relative_offset_array.value(i))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                relative_offset_array.value(0),
                0,
                "first relative offset should be 0"
            );
            assert_eq!(
                relative_offset_array.value(1),
                1,
                "second relative offset should be 1"
            );
            assert_eq!(
                relative_offset_array.value(2),
                2,
                "third relative offset should be 2"
            );
            assert_eq!(
                relative_offset_array.value(3),
                3,
                "fourth relative offset should be 3"
            );

            // Verify body column exists and has correct data.
            let body_column = batch
                .column_by_name("body")
                .expect("body column should exist");
            let body_array = body_column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("body column should be BinaryArray");

            assert_eq!(body_array.value(0), &[10, 20, 30]);
            assert_eq!(body_array.value(1), &[40, 50, 60]);
            assert_eq!(body_array.value(2), &[70, 80, 90]);
            assert_eq!(body_array.value(3), &[100, 110, 120]);
        }
    }

    /// Verifies setsum computation differs between Some and None log_position.
    #[test]
    fn construct_parquet_setsum_differs_based_on_log_position() {
        let messages = vec![vec![1, 2, 3]];

        let (_, setsum_with_position) =
            construct_parquet(Some(LogPosition::from_offset(100)), &messages)
                .expect("construct_parquet with Some should succeed");

        let (_, setsum_without_position) =
            construct_parquet(None, &messages).expect("construct_parquet with None should succeed");

        // The setsums should differ because the offset used in the setsum calculation differs.
        // With Some(100), offset is 100. With None, offset is 0.
        println!(
            "construct_parquet_setsum_differs_based_on_log_position: setsum_with_position = {}, setsum_without_position = {}",
            setsum_with_position.hexdigest(),
            setsum_without_position.hexdigest()
        );
        assert_ne!(
            setsum_with_position, setsum_without_position,
            "setsums should differ when log_position differs"
        );
    }

    /// Verifies construct_parquet handles empty messages.
    #[test]
    fn construct_parquet_with_empty_messages() {
        let messages: Vec<Vec<u8>> = vec![];

        let (buffer_with_pos, setsum_with_pos) =
            construct_parquet(Some(LogPosition::from_offset(1)), &messages)
                .expect("construct_parquet with Some and empty messages should succeed");

        let (buffer_without_pos, setsum_without_pos) = construct_parquet(None, &messages)
            .expect("construct_parquet with None and empty messages should succeed");

        // Both should produce non-empty parquet files (even with 0 rows).
        assert!(
            !buffer_with_pos.is_empty(),
            "parquet buffer should not be empty"
        );
        assert!(
            !buffer_without_pos.is_empty(),
            "parquet buffer should not be empty"
        );

        // Setsums should be default (no data inserted).
        println!(
            "construct_parquet_with_empty_messages: setsum_with_pos = {}, setsum_without_pos = {}",
            setsum_with_pos.hexdigest(),
            setsum_without_pos.hexdigest()
        );
        assert_eq!(
            setsum_with_pos,
            Setsum::default(),
            "setsum should be default for empty messages"
        );
        assert_eq!(
            setsum_without_pos,
            Setsum::default(),
            "setsum should be default for empty messages"
        );
    }

    /// Verifies that construct_parquet with log_position starting at 0 works correctly.
    #[test]
    fn construct_parquet_with_zero_log_position() {
        let log_position = LogPosition::from_offset(0);
        let messages = vec![vec![1], vec![2]];

        let (buffer, _setsum) = construct_parquet(Some(log_position), &messages)
            .expect("construct_parquet should succeed");

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(buffer))
            .expect("parquet should be parseable");
        let reader = builder.build().expect("parquet reader should build");

        for batch in reader {
            let batch = batch.expect("batch should be readable");

            // Should have "offset" column since Some was passed.
            let offset_column = batch
                .column_by_name("offset")
                .expect("should have 'offset' column");
            let offset_array = offset_column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("offset column should be UInt64Array");

            println!(
                "construct_parquet_with_zero_log_position: offsets = {:?}",
                (0..offset_array.len())
                    .map(|i| offset_array.value(i))
                    .collect::<Vec<_>>()
            );
            assert_eq!(offset_array.value(0), 0, "first offset should be 0");
            assert_eq!(offset_array.value(1), 1, "second offset should be 1");
        }
    }

    /// Verifies that construct_parquet with a large log_position offset works correctly.
    #[test]
    fn construct_parquet_with_large_log_position() {
        let log_position = LogPosition::from_offset(u64::MAX - 2);
        let messages = vec![vec![1], vec![2], vec![3]];

        let (buffer, _setsum) = construct_parquet(Some(log_position), &messages)
            .expect("construct_parquet should succeed");

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(buffer))
            .expect("parquet should be parseable");
        let reader = builder.build().expect("parquet reader should build");

        for batch in reader {
            let batch = batch.expect("batch should be readable");

            let offset_column = batch
                .column_by_name("offset")
                .expect("should have 'offset' column");
            let offset_array = offset_column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("offset column should be UInt64Array");

            println!(
                "construct_parquet_with_large_log_position: offsets = {:?}",
                (0..offset_array.len())
                    .map(|i| offset_array.value(i))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                offset_array.value(0),
                u64::MAX - 2,
                "first offset should be u64::MAX - 2"
            );
            assert_eq!(
                offset_array.value(1),
                u64::MAX - 1,
                "second offset should be u64::MAX - 1"
            );
            assert_eq!(
                offset_array.value(2),
                u64::MAX,
                "third offset should be u64::MAX - 0"
            );
        }
    }
}
