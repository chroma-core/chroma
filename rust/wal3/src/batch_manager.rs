use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{Error, FragmentSeqNo, LogPosition, ManifestManager, ThrottleOptions};

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

/// ManagerState captures the state necessary to batch manifests.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    last_batch: Instant,
    next_write: Instant,
    writers_active: usize,
    enqueued: Vec<(
        Vec<u8>,
        tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    )>,
}

impl ManagerState {
    /// Set the next_write instant based upon the current time and throttle options.
    fn set_next_write(&mut self, options: &ThrottleOptions) {
        self.next_write =
            Instant::now() + Duration::from_micros(1_000_000 / options.throughput as u64);
    }

    /// Select a fragment seq no and log position for writing, if possible.
    fn select_for_write(
        &mut self,
        options: &ThrottleOptions,
        manifest_manager: &ManifestManager,
        record_count: usize,
    ) -> Result<Option<(FragmentSeqNo, LogPosition)>, Error> {
        if self.next_write > Instant::now() {
            return Ok(None);
        }
        if self.writers_active > options.outstanding {
            return Ok(None);
        }
        let (next_seq_no, log_position) = match manifest_manager.assign_timestamp(record_count) {
            Some(log_position) => log_position,
            None => {
                return Err(Error::LogFull);
            }
        };
        self.writers_active += 1;
        self.set_next_write(options);
        Ok(Some((next_seq_no, log_position)))
    }

    fn finish_write(&mut self) {
        self.writers_active -= 1;
    }
}

/////////////////////////////////////////// BatchManager ///////////////////////////////////////////

#[derive(Debug)]
pub struct BatchManager {
    options: ThrottleOptions,
    state: Mutex<ManagerState>,
    records_written: AtomicUsize,
    batches_written: AtomicUsize,
    write_finished: tokio::sync::Notify,
}

impl BatchManager {
    pub fn new(mut options: ThrottleOptions) -> Option<Self> {
        // NOTE(rescrv):  Once upon a time we allowed concurrency here.  Deny it for safety.
        options.outstanding = 1;
        let next_write = Instant::now();
        Some(Self {
            options,
            state: Mutex::new(ManagerState {
                next_write,
                writers_active: 0,
                enqueued: Vec::new(),
                last_batch: Instant::now(),
            }),
            // Set these to 100k and 1 to avoid division by zero.  100k is a reasonable batch size,
            // to cold-start with as it favors fast ramp-up.
            records_written: AtomicUsize::new(100_000),
            batches_written: AtomicUsize::new(1),
            write_finished: tokio::sync::Notify::new(),
        })
    }

    pub fn push_work(
        &self,
        message: Vec<u8>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    ) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        state.enqueued.push((message, tx));
    }

    pub async fn wait_for_writable(&self) {
        self.write_finished.notified().await;
    }

    pub fn until_next_time(&self) -> Duration {
        // SAFETY(rescrv): Mutex poisoning.
        let state = self.state.lock().unwrap();
        let elapsed = state.last_batch.elapsed();
        let threshold = Duration::from_micros(self.options.batch_interval_us as u64);
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
    ) -> Result<
        Option<(
            FragmentSeqNo,
            LogPosition,
            Vec<(
                Vec<u8>,
                tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
            )>,
        )>,
        Error,
    > {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        if state.enqueued.is_empty() {
            // No work, no notify.
            return Ok(None);
        }
        // Clamp first by the number of items in this batch.
        let batch_size = self.batch_size();
        let mut batch_size =
            // If our estimate is wildly under-estimating or is an over-estimate, just take
            // everything available.
            if state.enqueued.len() > batch_size * 2 || state.enqueued.len() < batch_size {
                state.enqueued.len()
            } else {
                batch_size
            };
        // Clamp second by the number of bytes in this batch.
        let mut batch_size_bytes = 0usize;
        for idx in 0..batch_size {
            if batch_size_bytes > self.options.batch_size_bytes {
                batch_size = idx;
                break;
            }
            batch_size_bytes += state.enqueued[idx].0.len();
        }
        // If the batch size is less than half full and we haven't waited the batch interval since
        // last write, wait for more data.
        if batch_size_bytes < self.options.batch_size_bytes / 2
            && state.last_batch.elapsed()
                < Duration::from_micros(self.options.batch_interval_us as u64)
        {
            // This notify makes sure the background picks up the work and makes progress at end of
            // the batching interval.
            self.write_finished.notify_one();
            return Ok(None);
        }
        let Some((fragment_seq_no, log_position)) =
            state.select_for_write(&self.options, manifest_manager, batch_size)?
        else {
            // No fragment can be written at this time.
            return Ok(None);
        };
        let mut work = std::mem::take(&mut state.enqueued);
        state.enqueued = work.split_off(batch_size);
        state.last_batch = Instant::now();
        Ok(Some((fragment_seq_no, log_position, work)))
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

    pub fn finish_write(&self) {
        self.state.lock().unwrap().finish_write();
        self.write_finished.notify_one();
    }
}
