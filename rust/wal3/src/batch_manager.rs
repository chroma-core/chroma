use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{Error, FragmentSeqNo, LogPosition, ManifestManager, ThrottleOptions};

/////////////////////////////////////////// FragmentState //////////////////////////////////////////

#[derive(Debug)]
struct FragmentState {
    next_write: Instant,
    writers_active: usize,
}

impl FragmentState {
    fn set_next_write(&mut self, options: &ThrottleOptions) {
        self.next_write =
            Instant::now() + Duration::from_micros(1_000_000 / options.throughput as u64);
    }
}

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    fragment: FragmentState,
    enqueued: Vec<(
        Vec<u8>,
        tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    )>,
    last_batch: Instant,
}

impl ManagerState {
    fn select_for_write(
        &mut self,
        options: &ThrottleOptions,
        manifest_manager: &ManifestManager,
        record_count: usize,
    ) -> Result<Option<(FragmentSeqNo, LogPosition)>, Error> {
        if self.fragment.next_write > Instant::now() {
            return Ok(None);
        }
        if self.fragment.writers_active > options.outstanding {
            return Ok(None);
        }
        let (next_seq_no, log_position) = match manifest_manager.assign_timestamp(record_count) {
            Some(log_position) => log_position,
            None => {
                return Err(Error::LogFull);
            }
        };
        self.fragment.writers_active += 1;
        self.fragment.set_next_write(options);
        Ok(Some((next_seq_no, log_position)))
    }

    fn finish_write(&mut self) {
        self.fragment.writers_active -= 1;
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
    pub fn new(options: ThrottleOptions) -> Option<Self> {
        let next_write = Instant::now();
        let fragment = FragmentState {
            next_write,
            writers_active: 0,
        };
        Some(Self {
            options,
            state: Mutex::new(ManagerState {
                fragment,
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
            return Ok(None);
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
            if size > self.options.batch_size_bytes {
                batch_size = idx;
                break;
            }
            size += state.enqueued[idx].0.len();
        }
        if size < self.options.batch_size_bytes / 2
            && state.last_batch.elapsed()
                < Duration::from_micros(self.options.batch_interval_us as u64)
        {
            self.write_finished.notify_one();
            return Ok(None);
        }
        let Some((fragment_seq_no, log_position)) =
            state.select_for_write(&self.options, manifest_manager, batch_size)?
        else {
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
