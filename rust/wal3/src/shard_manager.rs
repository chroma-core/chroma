use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{
    DeltaSeqNo, Error, LogPosition, Manifest, ManifestManager, Message, ShardID, ShardSeqNo,
    StreamID, ThrottleOptions, BATCH_SIZE,
};

//////////////////////////////////////////// ShardState ////////////////////////////////////////////

#[derive(Debug)]
struct ShardState {
    shard_id: ShardID,
    next_seq_no: ShardSeqNo,
    next_write: Instant,
    writers_active: usize,
}

impl ShardState {
    fn set_next_write(&mut self, options: &ThrottleOptions) {
        self.next_seq_no += 1;
        self.next_write =
            Instant::now() + Duration::from_micros(1_000_000 / options.throughput as u64);
    }
}

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    shards: Vec<ShardState>,
    enqueued: Vec<(
        StreamID,
        Message,
        tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    )>,
    last_batch: Instant,
}

impl ManagerState {
    fn select_shard_for_write(
        &mut self,
        options: &ThrottleOptions,
        manifest_manager: &ManifestManager,
        record_count: usize,
    ) -> Result<Option<(ShardID, ShardSeqNo, LogPosition, DeltaSeqNo)>, Error> {
        let Some(shard_state) = self
            .shards
            .iter_mut()
            .filter(|x| x.writers_active < options.outstanding)
            .min_by_key(|x| x.next_write)
        else {
            return Ok(None);
        };
        if shard_state.next_write > Instant::now() {
            return Ok(None);
        }
        let (log_position, delta_seq_no) = match manifest_manager.assign_timestamp(record_count) {
            Some(log_position) => log_position,
            None => {
                return Err(Error::LogFull);
            }
        };
        let next_seq_no = shard_state.next_seq_no;
        shard_state.writers_active += 1;
        shard_state.set_next_write(options);
        Ok(Some((
            shard_state.shard_id,
            next_seq_no,
            log_position,
            delta_seq_no,
        )))
    }

    fn finish_write(&mut self, shard_id: ShardID) {
        let shard_state = self
            .shards
            .iter_mut()
            .find(|x| x.shard_id == shard_id)
            .unwrap();
        shard_state.writers_active -= 1;
    }
}

/////////////////////////////////////////// ShardManager ///////////////////////////////////////////

#[derive(Debug)]
pub struct ShardManager {
    options: ThrottleOptions,
    state: Mutex<ManagerState>,
    records_written: AtomicUsize,
    batches_written: AtomicUsize,
    write_finished: tokio::sync::Notify,
}

impl ShardManager {
    pub fn new(options: ThrottleOptions, initial_manifest: &Manifest, shards: usize) -> Self {
        let shards = (1..=shards)
            .map(|idx| {
                let shard_id = ShardID(idx);
                let next_seq_no = initial_manifest
                    .next_seq_no_for_shard(shard_id)
                    .unwrap_or(ShardSeqNo(1));
                let next_write = Instant::now();
                ShardState {
                    shard_id,
                    next_seq_no,
                    next_write,
                    writers_active: 0,
                }
            })
            .collect();
        Self {
            options,
            state: Mutex::new(ManagerState {
                shards,
                enqueued: Vec::new(),
                last_batch: Instant::now(),
            }),
            // Set these to 100k and 1 to avoid division by zero.  100k is a reasonable batch size,
            // so this should give a good starting point.
            records_written: AtomicUsize::new(100_000),
            batches_written: AtomicUsize::new(1),
            write_finished: tokio::sync::Notify::new(),
        }
    }

    pub fn push_work(
        &self,
        stream_id: StreamID,
        message: Message,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    ) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        state.enqueued.push((stream_id, message, tx));
    }

    pub async fn wait_for_writable(&self) {
        self.write_finished.notified().await;
    }

    pub fn until_next_time(&self) -> Duration {
        // SAFETY(rescrv): Mutex poisoning.
        let state = self.state.lock().unwrap();
        let elapsed = state.last_batch.elapsed();
        let threshold = Duration::from_micros(self.options.batch_interval as u64);
        if elapsed > threshold {
            Duration::ZERO
        } else {
            threshold - elapsed
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn take_work(
        &self,
        manifest_manager: &ManifestManager,
    ) -> Option<(
        ShardID,
        ShardSeqNo,
        LogPosition,
        DeltaSeqNo,
        Vec<(
            StreamID,
            Message,
            tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        )>,
    )> {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        if state.enqueued.is_empty() {
            return None;
        }
        let batch_size = self.batch_size();
        let mut batch_size =
            // If our estimate is wildly under-estimating, just take everything available.
            if state.enqueued.len() > batch_size * 2 || state.enqueued.len() < batch_size {
                state.enqueued.len()
            } else {
                batch_size
            };
        let mut size = 0usize;
        for idx in 0..batch_size {
            if size > self.options.batch_size {
                batch_size = idx;
                break;
            }
            size += state.enqueued[idx].1.len();
        }
        if size < self.options.batch_size / 2
            && state.last_batch.elapsed()
                < Duration::from_micros(self.options.batch_interval as u64)
        {
            self.write_finished.notify_one();
            return None;
        }
        let (shard_id, shard_seq_no, log_position, delta_seq_no) =
            match state.select_shard_for_write(&self.options, manifest_manager, batch_size) {
                Ok(Some(x)) => x,
                Ok(None) => return None,
                Err(e) => {
                    // NOTE(rescrv):  I don't like this, but the only way that
                    // select_shard_for_write can /fail/ is to have a full log, which will never
                    // really happen.  So we error and leave the log full.
                    for (_, _, tx) in state.enqueued.drain(..) {
                        let _ = tx.send(Err(e.clone()));
                    }
                    return None;
                }
            };
        BATCH_SIZE.add(batch_size as f64);
        let mut work = std::mem::take(&mut state.enqueued);
        state.enqueued = work.split_off(batch_size);
        state.last_batch = Instant::now();
        Some((shard_id, shard_seq_no, log_position, delta_seq_no, work))
    }

    pub fn update_average_batch_size(&self, records: usize) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    fn batch_size(&self) -> usize {
        let average = self.records_written.load(Ordering::Relaxed)
            / self.batches_written.load(Ordering::Relaxed);
        average.saturating_add(average / 10).saturating_add(1)
    }

    pub fn finish_write(&self, shard_id: ShardID) {
        self.state.lock().unwrap().finish_write(shard_id);
        self.write_finished.notify_one();
    }
}
