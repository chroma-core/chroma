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

///////////////////////////////////////////// LogWriter ////////////////////////////////////////////

pub struct LogWriter {
    /// LogWriterOptions are fixed at log creation time.
    /// LogWriter is intentionally cheap to construct and destroy.
    /// Reopen the log to change the options.
    options: Arc<LogWriterOptions>,
    /// A chroma object store.
    storage: Arc<Storage>,
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

impl LogWriter {
    pub async fn initialize(options: &LogWriterOptions, storage: &Storage) -> Result<(), Error> {
        Manifest::initialize(options, storage).await
    }

    pub async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
    ) -> Result<Arc<Self>, Error> {
        let options = Arc::new(options);
        let done = AtomicBool::new(false);
        let (reap, mut rx) = tokio::sync::mpsc::channel(1_000);
        let batch_manager = BatchManager::new(options.throttle_fragment).ok_or(Error::Internal)?;
        let manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            options.snapshot_manifest,
            Arc::clone(&storage),
        )
        .await?;
        let reaper = Mutex::new(None);
        let flusher = Mutex::new(None);
        let this = Arc::new(Self {
            options,
            storage,
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

    /// This will close the log.  If any references to the log exist outside those created by this
    /// library, this call will hang until they are dropped.
    pub async fn close(mut self: Arc<Self>) -> Result<(), Error> {
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

    /// Append a message to a stream.
    pub async fn append(self: &Arc<Self>, message: Vec<u8>) -> Result<LogPosition, Error> {
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
        assert!(!messages.is_empty());

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
        // SAFETY(rescrv):  The try_from_iter call will always succeed.
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
        let setsum = Setsum::default();

        // Upload the log.
        let path = format!(
            "log/Bucket={}/FragmentSeqNo={}.parquet",
            fragment_seq_no.bucket(),
            fragment_seq_no.0,
        );
        let exp_backoff: ExponentialBackoff = self.options.throttle_fragment.into();
        let start = Instant::now();
        loop {
            match self
                .storage
                .put_bytes(&path, buffer.clone(), PutOptions::if_not_exists())
                .await
            {
                Ok(_) => {
                    println!("installed fragment");
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
        let delta = Fragment {
            path: path.to_string(),
            seq_no: fragment_seq_no,
            start: log_position,
            limit: log_position + messages_len,
            setsum,
        };
        self.manifest_manager.apply_delta(delta).await?;
        // Record the records/batches written.
        self.batch_manager.update_average_batch_size(messages_len);
        self.batch_manager.finish_write();
        Ok(log_position)
    }
}
