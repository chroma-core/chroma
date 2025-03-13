use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use chroma_storage::{PutOptions, Storage, StorageError};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use setsum::Setsum;

use crate::{
    BatchManager, Error, ExponentialBackoff, Fragment, FragmentSeqNo, LogPosition,
    LogWriterOptions, Manifest, ManifestManager,
};

fn fragment_path(prefix: &str, fragment_seq_no: FragmentSeqNo) -> String {
    format!(
        "{}/log/Bucket={}/FragmentSeqNo={}.parquet",
        prefix,
        fragment_seq_no.bucket(),
        fragment_seq_no.0,
    )
}

/// The epoch writer is a counting writer.  Every epoch exists.  An epoch goes
/// unused->used->discarded.  The epoch of a writer is used to determine if and when log contention
/// indicates that a new writer should be created.  The epoch is incremented when a new writer is
/// created and checked before creating a new writer.
#[derive(Clone)]
pub struct EpochWriter {
    epoch: u64,
    writer: Arc<OnceLogWriter>,
}

///////////////////////////////////////////// LogWriter ////////////////////////////////////////////

pub struct LogWriter {
    inner: Mutex<Option<EpochWriter>>,
}

impl LogWriter {
    pub async fn initialize(
        options: &LogWriterOptions,
        storage: &Storage,
        prefix: String,
    ) -> Result<(), Error> {
        Manifest::initialize(options, storage, &prefix).await
    }

    /// Open the log, possibly writing a new manifest to recover it.
    pub async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
    ) -> Result<Self, Error> {
        let writer = OnceLogWriter::open(options, storage, prefix).await?;
        let inner = EpochWriter { epoch: 1, writer };
        Ok(Self {
            inner: Mutex::new(Some(inner)),
        })
    }

    /// This will close the log.
    pub async fn close(self) -> Result<(), Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let inner = { self.inner.lock().unwrap().take() };
        if let Some(inner) = inner {
            inner.writer.close().await
        } else {
            Ok(())
        }
    }

    /// Append a message to a log.
    pub async fn append(&self, message: Vec<u8>) -> Result<LogPosition, Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let inner = { self.inner.lock().unwrap().clone() };
        if let Some(epoch_writer) = inner {
            let res = epoch_writer.writer.append(message).await;
            if matches!(res, Err(Error::LogContention)) {
                let writer = OnceLogWriter::open(
                    epoch_writer.writer.options.clone(),
                    epoch_writer.writer.storage.clone(),
                    epoch_writer.writer.prefix.clone(),
                )
                .await?;
                // SAFETY(rescrv):  Mutex poisoning.
                let mut inner = self.inner.lock().unwrap();
                if let Some(second) = inner.as_mut() {
                    if second.epoch == epoch_writer.epoch {
                        second.epoch += 1;
                        second.writer = writer;
                    }
                } else {
                    // This should never happen, so just be polite with an error.
                    return Err(Error::LogClosed);
                }
            }
            res
        } else {
            // This should never happen, so just be polite with an error.
            Err(Error::LogClosed)
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
    /// ManifestManager coordinates updates to the manifest.
    manifest_manager: ManifestManager,
    /// BatchManager coordinates batching writes to the log.
    batch_manager: BatchManager,
    /// A channel to keep work alive on.
    #[allow(dead_code)]
    reap: tokio::sync::mpsc::Sender<tokio::task::JoinHandle<()>>,
    /// A background future that collects dead tasks
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// A background future that flushes the log.
    flusher: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl OnceLogWriter {
    async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
    ) -> Result<Arc<Self>, Error> {
        let done = AtomicBool::new(false);
        // NOTE(rescrv):  The channel size is relatively meaningless if it can hold the number of
        // outstanding operations on the log.  10x it for headroom.
        let (reap, mut rx) = tokio::sync::mpsc::channel(10 * options.throttle_fragment.outstanding);
        let batch_manager = BatchManager::new(options.throttle_fragment).ok_or(Error::Internal)?;
        let mut manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            options.snapshot_manifest,
            Arc::clone(&storage),
            prefix.clone(),
        )
        .await?;
        manifest_manager.recover().await?;
        let reaper = Mutex::new(None);
        let flusher = Mutex::new(None);
        let this = Arc::new(Self {
            options,
            storage,
            prefix,
            done,
            manifest_manager,
            batch_manager,
            reap,
            reaper,
            flusher,
        });
        let reaper = tokio::task::spawn(async move {
            while let Some(handle) = rx.recv().await {
                let _ = handle.await;
            }
        });
        let that = Arc::clone(&this);
        let flusher = tokio::task::spawn(async move {
            while !that.done.load(std::sync::atomic::Ordering::Relaxed) {
                that.batch_manager.wait_for_writable().await;
                match that.batch_manager.take_work(&that.manifest_manager) {
                    Ok(Some((fragment_seq_no, log_position, work))) => {
                        Arc::clone(&that)
                            .append_batch(fragment_seq_no, log_position, work)
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
        this.reaper.lock().unwrap().replace(reaper);
        this.flusher.lock().unwrap().replace(flusher);
        Ok(this)
    }

    async fn close(mut self: Arc<Self>) -> Result<(), Error> {
        // SAFETY(rescrv): Mutex poisoning.
        if let Some(flusher) = self.flusher.lock().unwrap().take() {
            flusher.abort();
        }
        // SAFETY(rescrv): Mutex poisoning.
        if let Some(reaper) = self.reaper.lock().unwrap().take() {
            reaper.abort();
        }
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

    async fn append(self: &Arc<Self>, message: Vec<u8>) -> Result<LogPosition, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.batch_manager.push_work(message, tx);
        if let Some((fragment_seq_no, log_position, work)) =
            self.batch_manager.take_work(&self.manifest_manager)?
        {
            let this = Arc::clone(self);
            let jh = tokio::task::spawn(async move {
                this.append_batch(fragment_seq_no, log_position, work).await
            });
            let _ = self.reap.send(jh).await;
        }
        rx.await.map_err(|_| Error::Internal)?
    }

    #[allow(clippy::type_complexity)]
    async fn append_batch(
        self: Arc<Self>,
        fragment_seq_no: FragmentSeqNo,
        log_position: LogPosition,
        work: Vec<(
            Vec<u8>,
            tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        )>,
    ) {
        let mut messages = Vec::with_capacity(work.len());
        let mut notifies = Vec::with_capacity(work.len());
        for work in work.into_iter() {
            messages.push(work.0);
            notifies.push(work.1);
        }
        if messages.is_empty() {
            tracing::error!("somehow got empty messages");
            return;
        }
        match self
            .append_batch_internal(fragment_seq_no, log_position, messages)
            .await
        {
            Ok(log_position) => {
                for (idx, notify) in notifies.into_iter().enumerate() {
                    let log_position = log_position + idx;
                    if notify.send(Ok(log_position)).is_err() {
                        // TODO(rescrv):  Counter this.
                    }
                }
            }
            Err(e) => {
                for notify in notifies {
                    if notify.send(Err(e.clone())).is_err() {
                        // TODO(rescrv):  Counter this.
                    }
                }
            }
        }
    }

    async fn append_batch_internal(
        &self,
        fragment_seq_no: FragmentSeqNo,
        log_position: LogPosition,
        messages: Vec<Vec<u8>>,
    ) -> Result<LogPosition, Error> {
        assert!(
            !messages.is_empty(),
            "empty batch; this should be checked by caller"
        );

        // Construct the columns.
        let messages_len = messages.len();
        let mut positions = Vec::with_capacity(messages_len);
        let mut bodies = Vec::with_capacity(messages_len);
        for (index, message) in messages.iter().enumerate() {
            let position = log_position + index;
            positions.push(position);
            bodies.push(message.as_slice());
        }
        let offsets = positions.iter().map(|p| p.offset).collect::<Vec<_>>();
        let timestamps_us = positions.iter().map(|p| p.timestamp_us).collect::<Vec<_>>();
        let offsets = UInt64Array::from(offsets);
        let timestamps_us = UInt64Array::from(timestamps_us);
        let bodies = BinaryArray::from(bodies);
        // SAFETY(rescrv):  The try_from_iter call will always succeed because the three arrays
        // have same length and the types check.
        // TODO(rescrv):  Arrow pre-allocator.
        let batch = RecordBatch::try_from_iter(vec![
            ("offset", Arc::new(offsets) as ArrayRef),
            ("timestamp_us", Arc::new(timestamps_us) as ArrayRef),
            ("body", Arc::new(bodies) as ArrayRef),
        ])
        .unwrap();

        let path = fragment_path(&self.prefix, fragment_seq_no);
        let exp_backoff: ExponentialBackoff = self.options.throttle_fragment.into();
        let start = Instant::now();
        let mut num_bytes;
        loop {
            // Write to parquet.
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut buffer = vec![];
            let mut writer =
                ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).map_err(Arc::new)?;
            writer.close().map_err(Arc::new)?;
            num_bytes = buffer.len() as u64;

            // Upload the log.
            match self
                .storage
                .put_bytes(&path, buffer, PutOptions::if_not_exists())
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    return Err(Error::LogContention);
                }
                Err(_) => {
                    if start.elapsed() > Duration::from_secs(60) {
                        return Err(Error::LogWriteTimeout);
                    }
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        // Upload to a coalesced manifest.
        let fragment = Fragment {
            path: path.to_string(),
            seq_no: fragment_seq_no,
            start: log_position,
            limit: log_position + messages_len,
            num_bytes,
            // TODO(rescrv):  This is a placeholder.
            setsum: Setsum::default(),
        };
        self.manifest_manager.add_fragment(fragment).await?;
        // Record the records/batches written.
        self.batch_manager.update_average_batch_size(messages_len);
        self.batch_manager.finish_write();
        Ok(log_position)
    }
}
