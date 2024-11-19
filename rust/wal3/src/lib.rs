#![doc = include_str!("../README.md")]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use biometrics::{Collector, Counter, Moments};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload, Result};
use serde::{Deserialize, Serialize};
use sst::log::{LogBuilder, LogOptions};
use sst::Builder as SstBuilder;
use uuid::Uuid;

mod backoff;
mod cursor;
mod latency_simulator;
mod manifest;
mod manifest_manager;
mod robust_object_store;
mod shard_manager;
mod stream_manager;

use backoff::ExponentialBackoff;
use shard_manager::ShardManager;
use stream_manager::StreamManager;

pub use cursor::{Cursor, CursorStore};
pub use latency_simulator::{LatencyControlledObjectStore, SimulationOptions};
pub use manifest::{Manifest, ShardFragment, ShardID, ShardSeqNo};
pub use manifest_manager::{DeltaSeqNo, ManifestManager};
pub use robust_object_store::RobustObjectStore;

//////////////////////////////////////////// biometrics ////////////////////////////////////////////

static LOG_UPLOADED: Counter = Counter::new("wal3.log_uploaded");
static MANIFEST_UPLOADED: Counter = Counter::new("wal3.manifest_uploaded");

pub static LOG_TTFB_LATENCY: Moments = Moments::new("wal3.log_ttfb_latency");
pub static LOG_FETCH_LATENCY: Moments = Moments::new("wal3.log_fetch_latency");

static BATCH_SIZE: Moments = Moments::new("wal3.batch_size");

static APPLY_DELTA: Counter = Counter::new("wal3__manifest_manager__apply_delta");
static PUSH_DELTA: Counter = Counter::new("wal3__manifest_manager__push_delta");
static PULL_WORK: Counter = Counter::new("wal3__manifest_manager__pull_work");
static NO_DELTAS: Counter = Counter::new("wal3__manifest_manager__no_deltas");
static NO_APPROPRIATE_DELTA: Counter = Counter::new("wal3__manifest_manager__no_appropriate_delta");
static GENERATE_NEXT_POINTER: Counter =
    Counter::new("wal3__manifest_manager__generate_next_pointer");

static DROPPED_MESSAGES: Counter = Counter::new("wal3.dropped_messages");
static LOG_FULL: Counter = Counter::new("wal3.log_full");
static CLOCK_JUMP: Counter = Counter::new("wal3.clock_jump");
static STORE_LOG_BACKOFF: Counter = Counter::new("wal3.store_log_backoff");
static STORE_MANIFEST_BACKOFF: Counter = Counter::new("wal3.store_manifest_backoff");

pub fn register_biometrics(collector: &Collector) {
    collector.register_counter(&LOG_UPLOADED);
    collector.register_counter(&MANIFEST_UPLOADED);

    collector.register_moments(&LOG_TTFB_LATENCY);
    collector.register_moments(&LOG_FETCH_LATENCY);

    collector.register_moments(&BATCH_SIZE);

    collector.register_counter(&APPLY_DELTA);
    collector.register_counter(&PUSH_DELTA);
    collector.register_counter(&PULL_WORK);
    collector.register_counter(&NO_DELTAS);
    collector.register_counter(&NO_APPROPRIATE_DELTA);
    collector.register_counter(&GENERATE_NEXT_POINTER);

    collector.register_counter(&DROPPED_MESSAGES);
    collector.register_counter(&LOG_FULL);
    collector.register_counter(&CLOCK_JUMP);
    collector.register_counter(&STORE_LOG_BACKOFF);
    collector.register_counter(&STORE_MANIFEST_BACKOFF);
    latency_simulator::register_biometrics(collector);
}

/////////////////////////////////////////////// Error //////////////////////////////////////////////

#[derive(Clone, Debug, Default)]
pub enum Error {
    #[default]
    Success,
    UninitializedLog,
    AlreadyInitialized,
    AlreadyOpen,
    ClosedStream,
    LogContention,
    LogFull,
    Internal,
    CorruptManifest(String),
    CorruptCursor(String),
    NoSuchCursor(Cursor),
    Sst(sst::Error),
    ObjectStore(Arc<object_store::Error>),
    ScrubError(ScrubError),
}

impl From<sst::Error> for Error {
    fn from(e: sst::Error) -> Self {
        Error::Sst(e)
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::ObjectStore(Arc::new(e))
    }
}

impl From<ScrubError> for Error {
    fn from(e: ScrubError) -> Self {
        Error::ScrubError(e)
    }
}

//////////////////////////////////////////// ScrubError ////////////////////////////////////////////

#[derive(Clone, Debug)]
pub enum ScrubError {
    CorruptManifest(String),
    CorruptFragment {
        shard_id: ShardID,
        seq_no: ShardSeqNo,
        what: String,
    },
}

///////////////////////////////////////////// StreamID /////////////////////////////////////////////

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamID(pub Uuid);

impl StreamID {
    pub const INTERNAL: StreamID =
        StreamID(Uuid::from_u128(0x610ba100d0600f00d00c0ffeec0ffee0_u128));
}

//////////////////////////////////////////// CursorName ////////////////////////////////////////////

pub struct CursorName(pub String);

////////////////////////////////////////////// Message /////////////////////////////////////////////

#[derive(Clone, Debug)]
pub enum Message {
    Control(Vec<u8>),
    Payload(Vec<u8>),
}

impl Message {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Message::Control(v) => v.len(),
            Message::Payload(v) => v.len(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Message::Control(v) => v.as_slice(),
            Message::Payload(v) => v.as_slice(),
        }
    }
}

//////////////////////////////////////////// LogPosition ///////////////////////////////////////////

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct LogPosition(pub u64);

impl std::ops::Add<LogPosition> for usize {
    type Output = LogPosition;

    fn add(self, rhs: LogPosition) -> Self::Output {
        LogPosition((self as u64).wrapping_add(rhs.0))
    }
}

impl std::ops::Add<usize> for LogPosition {
    type Output = LogPosition;

    fn add(self, rhs: usize) -> Self::Output {
        LogPosition(self.0.wrapping_add(rhs as u64))
    }
}

impl std::ops::AddAssign<usize> for LogPosition {
    fn add_assign(&mut self, rhs: usize) {
        self.0 = self.0.wrapping_add(rhs as u64);
    }
}

////////////////////////////////////////// ThrottleOptions /////////////////////////////////////////

#[derive(Copy, Clone, Debug, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct ThrottleOptions {
    #[arrrg(optional, "Maximum batch size in bytes.")]
    pub batch_size: usize,
    #[arrrg(optional, "Minimum batch interval if not full in us. ")]
    pub batch_interval: usize,
    #[arrrg(optional, "Maximum throughput in ops/s.")]
    pub throughput: usize,
    #[arrrg(optional, "Maximum headroom in ops/s.")]
    pub headroom: usize,
    #[arrrg(optional, "Maximum number of outstanding ops.")]
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

///////////////////////////////////////// LogWriterOptions /////////////////////////////////////////

#[derive(Clone, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct LogWriterOptions {
    #[arrrg(nested)]
    pub format: LogOptions,
    #[arrrg(optional, "Number of shards in the log.")]
    pub shards: usize,
    #[arrrg(nested)]
    pub throttle_shard: ThrottleOptions,
    #[arrrg(nested)]
    pub throttle_manifest: ThrottleOptions,
    #[arrrg(optional, "The alpha for manifest load.")]
    pub load_alpha: usize,
    #[arrrg(optional, "The alpha for manifest store.")]
    pub store_alpha: usize,
}

impl Default for LogWriterOptions {
    fn default() -> Self {
        LogWriterOptions {
            // Default log options.
            format: LogOptions::default(),
            // Start with a log of one shard.
            shards: 1,
            // Default throttling options for shards.
            throttle_shard: ThrottleOptions::default(),
            throttle_manifest: ThrottleOptions::default(),
            // Default alpha for manifest load.
            load_alpha: 1,
            // Default alpha for manifest store.
            store_alpha: 1,
        }
    }
}

///////////////////////////////////////////// LogWriter ////////////////////////////////////////////

pub struct LogWriter<O: ObjectStore> {
    /// LogWriterOptions are fixed at log creation time.
    /// LogWriter is intentionally cheap to construct and destroy.
    /// Reopen the log to change the options.
    options: Arc<LogWriterOptions>,
    /// A generic object store.
    object_store: Arc<O>,
    /// True iff the log is done.
    done: AtomicBool,
    /// StreamManager manages the open streams on this log.
    stream_manager: StreamManager,
    /// ManifestManager coordinates updates to the manifest.
    manifest_manager: ManifestManager,
    /// ShardManager coordinates writes to the non-manifest shards.
    shard_manager: ShardManager,
    /// A channel to keep work alive on.
    #[allow(dead_code)]
    reap: tokio::sync::mpsc::Sender<tokio::task::JoinHandle<()>>,
    /// A background future that collects dead tasks
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// A background future that flushes the log.
    flusher: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<O: ObjectStore> LogWriter<O> {
    pub async fn initialize(options: &LogWriterOptions, object_store: &O) -> Result<(), Error> {
        Manifest::initialize(options, object_store).await
    }

    pub async fn open(mut options: LogWriterOptions, object_store: O) -> Result<Arc<Self>, Error> {
        // We clamp throttle_manifest_outstanding and store_alpha to the min of the two.  It is
        // always acceptable to operate with a smaller write alpha or bigger read alpha, so what
        // we're effectively doing is allowing two independent constraints on the write ahead.
        options.throttle_manifest.outstanding =
            std::cmp::min(options.throttle_manifest.outstanding, options.store_alpha);
        options.store_alpha = options.throttle_manifest.outstanding;
        let options = Arc::new(options);
        let object_store = Arc::new(object_store);
        let manifest = Manifest::load(&*object_store, options.load_alpha)
            .await?
            .ok_or(Error::UninitializedLog)?;
        let done = AtomicBool::new(false);
        let (reap, mut rx) = tokio::sync::mpsc::channel(1_000);
        let stream_manager = StreamManager::default();
        let shard_manager = ShardManager::new(options.throttle_shard, &manifest, options.shards);
        let manifest_manager = ManifestManager::new(
            options.throttle_manifest,
            manifest,
            Arc::clone(&object_store),
        )
        .await;
        let reaper = Mutex::new(None);
        let flusher = Mutex::new(None);
        let this = Arc::new(Self {
            options,
            object_store,
            done,
            stream_manager,
            manifest_manager,
            shard_manager,
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
                that.shard_manager.wait_for_writable().await;
                if let Some((shard_id, shard_seq_no, log_position, delta_seq_no, work)) =
                    that.shard_manager.take_work(&that.manifest_manager)
                {
                    Arc::clone(&that)
                        .append_batch(shard_id, shard_seq_no, log_position, delta_seq_no, work)
                        .await;
                } else {
                    tokio::time::sleep(that.shard_manager.until_next_time()).await;
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
    pub async fn close(mut self: Arc<Self>) -> Result<()> {
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

    /// Return a list of streams this log currently allows for writing.
    pub async fn streams(self: &Arc<Self>) -> Result<Vec<StreamID>, Error> {
        Ok(self.stream_manager.streams())
    }

    /// Open a new stream in the log.
    pub async fn open_stream(self: &Arc<Self>, stream_id: StreamID) -> Result<(), Error> {
        self.stream_manager.open_stream(stream_id)
    }

    /// Close a stream in the log.
    pub async fn close_stream(self: &Arc<Self>, stream_id: StreamID) -> Result<(), Error> {
        self.stream_manager.close_stream(stream_id)
    }

    /// Append a message to a stream.
    pub async fn append(
        self: &Arc<Self>,
        stream_id: StreamID,
        message: Message,
    ) -> Result<LogPosition, Error> {
        if !self.stream_manager.stream_is_open(stream_id) {
            return Err(Error::ClosedStream);
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shard_manager.push_work(stream_id, message, tx);
        if let Some((shard_id, shard_seq_no, log_position, delta_seq_no, work)) =
            self.shard_manager.take_work(&self.manifest_manager)
        {
            let this = Arc::clone(self);
            let jh = tokio::task::spawn(async move {
                this.append_batch(shard_id, shard_seq_no, log_position, delta_seq_no, work)
                    .await
            });
            let _ = self.reap.send(jh).await;
        }
        rx.await.map_err(|_| Error::Internal)?
    }

    #[allow(clippy::type_complexity)]
    async fn append_batch(
        self: Arc<Self>,
        shard_id: ShardID,
        shard_seq_no: ShardSeqNo,
        log_position: LogPosition,
        delta_seq_no: DeltaSeqNo,
        work: Vec<(
            StreamID,
            Message,
            tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        )>,
    ) {
        let mut messages = Vec::with_capacity(work.len());
        let mut notifies = Vec::with_capacity(work.len());
        for work in work.into_iter() {
            messages.push((work.0, work.1));
            notifies.push(work.2);
        }
        match self
            .append_batch_internal(shard_id, shard_seq_no, log_position, delta_seq_no, messages)
            .await
        {
            Ok(log_position) => {
                for (idx, notify) in notifies.into_iter().enumerate() {
                    let log_position = log_position + idx;
                    if notify.send(Ok(log_position)).is_err() {
                        DROPPED_MESSAGES.click();
                    }
                }
            }
            Err(e) => {
                for notify in notifies {
                    if notify.send(Err(e.clone())).is_err() {
                        DROPPED_MESSAGES.click();
                    }
                }
            }
        }
    }

    #[allow(clippy::type_complexity)]
    async fn append_batch_internal(
        &self,
        shard_id: ShardID,
        shard_seq_no: ShardSeqNo,
        log_position: LogPosition,
        delta_seq_no: DeltaSeqNo,
        messages: Vec<(StreamID, Message)>,
    ) -> Result<LogPosition, Error> {
        assert!(!messages.is_empty());
        #[derive(Default)]
        struct Buffer(Vec<u8>);

        impl std::io::Write for Buffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.write(buf)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl sst::log::Write for Buffer {
            fn fsync(&mut self) -> Result<(), sst::Error> {
                Ok(())
            }
        }

        // Construct the log.
        let mut log_builder =
            LogBuilder::from_write(self.options.format.clone(), Buffer::default())?;
        let messages_len = messages.len();
        for (idx, (stream_id, message)) in messages.iter().enumerate() {
            let log_position = log_position + idx;
            let key = format!("stream:{}:{}", stream_id.0, log_position.0);
            let value = message.as_bytes();
            log_builder.put(key.as_bytes(), log_position.0, value)?;
        }
        let (setsum, buffer) = log_builder.seal()?;
        let min_timestamp = log_position;
        let max_timestamp = log_position + messages_len;
        let path = Path::from(format!(
            "/shard{:02x}/{:016x}_{}_{}_{}.log",
            shard_id.0,
            shard_seq_no.0,
            min_timestamp.0,
            max_timestamp.0,
            setsum.hexdigest(),
        ));

        // Upload the log.
        let payload = PutPayload::from_bytes(buffer.0.into());
        let exp_backoff: ExponentialBackoff = self.options.throttle_shard.into();
        loop {
            match self.object_store.put(&path, payload.clone()).await {
                Ok(_) => {
                    LOG_UPLOADED.click();
                    println!("uploaded log to {}", path);
                    break;
                }
                Err(e) => {
                    eprintln!("error uploading log to {}: {}", path, e);
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        // Upload to a coalesced manifest.
        let delta = ShardFragment {
            path: path.to_string(),
            shard_id,
            seq_no: shard_seq_no,
            start: log_position,
            limit: log_position + messages_len,
            setsum,
        };
        self.manifest_manager
            .apply_delta(delta, delta_seq_no)
            .await?;
        // Record the records/batches written.
        self.shard_manager.update_average_batch_size(messages_len);
        self.shard_manager.finish_write(shard_id);
        Ok(log_position)
    }
}

///////////////////////////////////////// LogReaderOptions /////////////////////////////////////////

#[derive(Clone, Default, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct LogReaderOptions {
    #[arrrg(optional, "The alpha for manifest load.")]
    pub load_alpha: usize,
}

///////////////////////////////////////////// LogReader ////////////////////////////////////////////

// TODO(rescrv): Finish implementing reader to get rid of dead code.
#[allow(dead_code)]
pub struct LogReader<O: ObjectStore> {
    /// LogReaderOptions are fixed at log creation time.
    /// LogReader is intentionally cheap to construct and destroy.
    /// Reopen the log to change the options.
    options: Arc<LogReaderOptions>,
    /// A generic object store.
    object_store: Arc<O>,
    /// The manifest for the log's current position.
    manifest: Manifest,
    /// The position of the log reader.
    position: LogPosition,
}

impl<O: ObjectStore> LogReader<O> {
    pub async fn open(options: LogReaderOptions, object_store: O) -> Result<Self, Error> {
        let options = Arc::new(options);
        let object_store = Arc::new(object_store);
        let Some(manifest) = Manifest::load(&*object_store, options.load_alpha).await? else {
            return Err(Error::UninitializedLog);
        };
        let position = LogPosition::default();
        Ok(Self {
            options,
            object_store,
            manifest,
            position,
        })
    }

    pub async fn scrub(&mut self) -> Result<(), Error> {
        self.manifest.scrub()?;
        for fragment in self.manifest.fragments.iter() {
            let start_of_fetch = Instant::now();
            let object = self
                .object_store
                .get(&Path::from(fragment.path.as_str()))
                .await?;
            let ttfb = start_of_fetch.elapsed();
            LOG_TTFB_LATENCY.add(ttfb.as_micros() as f64);
            let bytes = object.bytes().await?;
            let complete = start_of_fetch.elapsed();
            LOG_FETCH_LATENCY.add(complete.as_micros() as f64);
            let bytes = std::io::Cursor::new(bytes.to_vec());
            let mut log =
                sst::log::LogIterator::from_reader(sst::log::LogOptions::default(), bytes)?;
            let mut acc = sst::Setsum::default();
            while let Some(kvr) = log.next()? {
                if let Some(value) = kvr.value.as_ref() {
                    acc.put(kvr.key, kvr.timestamp, value);
                } else {
                    acc.del(kvr.key, kvr.timestamp);
                }
            }
            if fragment.setsum != acc {
                return Err(Error::CorruptManifest(format!(
                    "fragment {} has incorrect setsum",
                    fragment.path
                )));
            }
        }
        Ok(())
    }
}

/////////////////////////////////////////////// util ///////////////////////////////////////////////

pub fn deserialize_setsum<'de, D>(deserializer: D) -> Result<sst::Setsum, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    sst::Setsum::from_hexdigest(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("invalid setsum: {}", s)))
}

pub fn serialize_setsum<S>(setsum: &sst::Setsum, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let s = setsum.hexdigest();
    s.serialize(serializer)
}
