use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{DeleteOptions, GetOptions, PutOptions, Storage, StorageError};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use setsum::Setsum;
use tracing::Instrument;

use crate::{
    unprefixed_fragment_path, BatchManager, CursorStore, CursorStoreOptions, Error,
    ExponentialBackoff, Fragment, FragmentSeqNo, Garbage, GarbageCollectionOptions, LogPosition,
    LogReader, LogReaderOptions, LogWriterOptions, Manifest, ManifestManager, ThrottleOptions,
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
    pub async fn bootstrap<D: MarkDirty>(
        options: &LogWriterOptions,
        storage: &Arc<Storage>,
        prefix: &str,
        writer: &str,
        mark_dirty: D,
        first_record_offset: LogPosition,
        messages: Vec<Vec<u8>>,
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
                FragmentSeqNo(1),
                first_record_offset,
                messages,
            )
            .await?;
            let seq_no = FragmentSeqNo(1);
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
                return Err(Error::Internal);
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
                            inner.writer.take();
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
                        inner.writer.take();
                    }
                    return Err(Error::LogContentionFailure);
                }
                Err(Error::LogContentionRetry) => {
                    // SAFETY(rescrv):  Mutex poisoning.
                    let mut inner = self.inner.lock().unwrap();
                    if inner.epoch == epoch {
                        inner.writer.take();
                    }
                }
                Err(err) => {
                    let mut inner = self.inner.lock().unwrap();
                    if inner.epoch == epoch {
                        inner.writer.take();
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
                let inner = self.inner.lock().unwrap();
                if let Some(writer) = inner.writer.as_ref() {
                    return Ok((Arc::clone(writer), inner.epoch));
                }
                inner.epoch
            };
            let writer = match OnceLogWriter::open(
                self.options.clone(),
                self.storage.clone(),
                self.prefix.clone(),
                self.writer.clone(),
                Arc::clone(&self.mark_dirty),
            )
            .await
            {
                Ok(writer) => writer,
                Err(Error::LogContentionRetry) => continue,
                Err(err) => return Err(err),
            };
            // SAFETY(rescrv):  Mutex poisoning.
            let mut inner = self.inner.lock().unwrap();
            if inner.epoch == epoch {
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
struct OnceLogWriter {
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
    /// A background future that flushes the log.
    flusher: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl OnceLogWriter {
    async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
        mark_dirty: Arc<dyn MarkDirty>,
    ) -> Result<Arc<Self>, Error> {
        let done = AtomicBool::new(false);
        let batch_manager = BatchManager::new(options.throttle_fragment).ok_or(Error::Internal)?;
        let mut manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            options.snapshot_manifest,
            Arc::clone(&storage),
            prefix.clone(),
            writer,
        )
        .await?;
        manifest_manager.recover(&*mark_dirty).await?;
        let flusher = Mutex::new(None);
        let this = Arc::new(Self {
            options,
            storage,
            prefix,
            done,
            mark_dirty,
            manifest_manager,
            batch_manager,
            flusher,
        });
        let that = Arc::clone(&this);
        let flusher = tokio::task::spawn(async move {
            while !that.done.load(std::sync::atomic::Ordering::Relaxed) {
                that.batch_manager.wait_for_writable().await;
                match that.batch_manager.take_work(&that.manifest_manager) {
                    Ok(Some((fragment_seq_no, log_position, work))) => {
                        let _ = tokio::task::spawn(Arc::clone(&that).append_batch(
                            fragment_seq_no,
                            log_position,
                            work,
                        ))
                        .await;
                    }
                    Ok(None) => {
                        tokio::time::sleep(that.batch_manager.until_next_time()).await;
                    }
                    Err(err) => {
                        tracing::error!("batch_manager.take_work: {:?}", err);
                        tokio::time::sleep(that.batch_manager.until_next_time()).await;
                    }
                }
            }
        });
        // SAFETY(rescrv): Mutex poisoning.
        this.flusher.lock().unwrap().replace(flusher);
        Ok(this)
    }

    fn shutdown(&self) {
        self.done.store(true, std::sync::atomic::Ordering::Relaxed);
        // SAFETY(rescrv): Mutex poisoning.
        if let Some(flusher) = self.flusher.lock().unwrap().take() {
            flusher.abort();
        }
        self.batch_manager.shutdown();
        self.manifest_manager.shutdown();
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

    #[tracing::instrument(skip(self, messages))]
    async fn append(self: &Arc<Self>, messages: Vec<Vec<u8>>) -> Result<LogPosition, Error> {
        if messages.is_empty() {
            return Err(Error::EmptyBatch);
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.batch_manager.push_work(messages, tx);
        if let Some((fragment_seq_no, log_position, work)) =
            self.batch_manager.take_work(&self.manifest_manager)?
        {
            let this = Arc::clone(self);
            tokio::task::spawn(this.append_batch(fragment_seq_no, log_position, work));
        }
        let span = tracing::info_span!("wait_for_durability");
        rx.instrument(span).await.map_err(|_| Error::Internal)?
    }

    #[tracing::instrument(skip(self, work))]
    #[allow(clippy::type_complexity)]
    async fn append_batch(
        self: Arc<Self>,
        fragment_seq_no: FragmentSeqNo,
        log_position: LogPosition,
        work: Vec<(
            Vec<Vec<u8>>,
            tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        )>,
    ) {
        let mut messages = Vec::with_capacity(work.len());
        let mut notifies = Vec::with_capacity(work.len());
        for work in work.into_iter() {
            notifies.push((work.0.len(), work.1));
            messages.extend(work.0);
        }
        if messages.is_empty() {
            tracing::error!("somehow got empty messages");
            return;
        }
        match self
            .append_batch_internal(fragment_seq_no, log_position, messages)
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
                for (_, notify) in notifies {
                    if notify.send(Err(e.clone())).is_err() {
                        // TODO(rescrv):  Counter this.
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip(self, messages))]
    async fn append_batch_internal(
        &self,
        fragment_seq_no: FragmentSeqNo,
        log_position: LogPosition,
        messages: Vec<Vec<u8>>,
    ) -> Result<LogPosition, Error> {
        assert!(!messages.is_empty());
        let messages_len = messages.len();
        let fut1 = upload_parquet(
            &self.options,
            &self.storage,
            &self.prefix,
            fragment_seq_no,
            log_position,
            messages,
        );
        let fut2 = async {
            match self.mark_dirty.mark_dirty(log_position, messages_len).await {
                Ok(_) | Err(Error::LogContentionDurable) => Ok(()),
                Err(err) => Err(err),
            }
        };
        let (res1, res2) = futures::future::join(fut1, fut2).await;
        res2?;
        let (path, setsum, num_bytes) = res1?;
        // Upload to a coalesced manifest.
        let fragment = Fragment {
            path: path.to_string(),
            seq_no: fragment_seq_no,
            start: log_position,
            limit: log_position + messages_len,
            num_bytes: num_bytes as u64,
            setsum,
        };
        self.manifest_manager.publish_fragment(fragment).await?;
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
    async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error> {
        self.manifest_manager.heartbeat().await?;
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
            let garbage_and_e_tag =
                match Garbage::load(&self.options.throttle_manifest, &self.storage, &self.prefix)
                    .await
                {
                    Ok(Some((garbage, e_tag))) => Some((garbage, e_tag)),
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
    async fn garbage_collect_phase2_update_manifest(
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
            self.manifest_manager.apply_garbage(garbage.clone()).await?;
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
    async fn garbage_collect_phase3_delete_garbage(
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
        let paths = garbage
            .prefixed_paths_to_delete(&self.prefix)
            .collect::<Vec<_>>();
        for path in paths {
            loop {
                match self.storage.delete(&path, DeleteOptions::default()).await {
                    Ok(()) => break,
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
    async fn garbage_collect(
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

#[tracing::instrument(skip(messages))]
pub fn construct_parquet(
    log_position: LogPosition,
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
    // SAFETY(rescrv):  The try_from_iter call will always succeed.
    // TODO(rescrv):  Arrow pre-allocator.
    let batch = RecordBatch::try_from_iter(vec![
        ("offset", Arc::new(offsets) as ArrayRef),
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
    fragment_seq_no: FragmentSeqNo,
    log_position: LogPosition,
    messages: Vec<Vec<u8>>,
) -> Result<(String, Setsum, usize), Error> {
    // Upload the log.
    let unprefixed_path = unprefixed_fragment_path(fragment_seq_no);
    let path = format!("{prefix}/{unprefixed_path}");
    let exp_backoff: ExponentialBackoff = options.throttle_fragment.into();
    let start = Instant::now();
    loop {
        let (buffer, setsum) = construct_parquet(log_position, &messages)?;
        tracing::info!("upload_parquet: {:?} with {} bytes", path, buffer.len());
        // NOTE(rescrv):  This match block has been thoroughly reasoned through within the
        // `bootstrap` call above.  Don't change the error handling here without re-reasoning
        // there.
        match storage
            .put_bytes(
                &path,
                buffer.clone(),
                PutOptions::if_not_exists(StorageRequestPriority::P0),
            )
            .await
        {
            Ok(_) => {
                return Ok((unprefixed_path, setsum, buffer.len()));
            }
            Err(StorageError::Precondition { path: _, source: _ }) => {
                return Err(Error::LogContentionFailure);
            }
            Err(err) => {
                if start.elapsed() > Duration::from_secs(60) {
                    return Err(Error::StorageError(Arc::new(err)));
                }
                let mut backoff = exp_backoff.next();
                if backoff > Duration::from_secs(3_600) {
                    backoff = Duration::from_secs(3_600);
                }
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

#[tracing::instrument(skip(options, storage))]
pub async fn copy_parquet(
    options: &LogWriterOptions,
    storage: &Storage,
    source: &str,
    target: &str,
) -> Result<(), Error> {
    let parquet = storage
        .get(source, GetOptions::new(StorageRequestPriority::P0))
        .await
        .map_err(Arc::new)?;
    let exp_backoff: ExponentialBackoff = options.throttle_fragment.into();
    let start = Instant::now();
    loop {
        match storage
            .put_bytes(
                target,
                parquet.to_vec(),
                PutOptions::if_not_exists(StorageRequestPriority::P0),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(StorageError::Precondition { path: _, source: _ }) => return Ok(()),
            Err(err) => {
                if start.elapsed() > Duration::from_secs(60) {
                    return Err(Error::StorageError(Arc::new(err)));
                }
                let mut backoff = exp_backoff.next();
                if backoff > Duration::from_secs(3_600) {
                    backoff = Duration::from_secs(3_600);
                }
                tokio::time::sleep(backoff).await;
            }
        }
    }
}
