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
        Vec<Vec<u8>>,
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
        if self.writers_active > 0 {
            return Ok(None);
        }
        let (next_seq_no, log_position) = match manifest_manager.assign_timestamp(record_count) {
            Some((next_seq_no, log_position)) => (next_seq_no, log_position),
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
    pub fn new(options: ThrottleOptions) -> Option<Self> {
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
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    ) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        state.enqueued.push((messages, tx));
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
                Vec<Vec<u8>>,
                tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
            )>,
        )>,
        Error,
    > {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        // If there is no work, there is no notify.
        if state.enqueued.is_empty() {
            // No work, no notify.
            return Ok(None);
        }

        let batch_size = self.batch_size();
        let enqueued_records = state.enqueued.iter().map(|(r, _)| r.len()).sum::<usize>();
        let batch_size =
            // If our estimate is wildly under-estimating or is an over-estimate, just take
            // everything available.
            if enqueued_records > batch_size * 2 || enqueued_records < batch_size {
                enqueued_records
            } else {
                batch_size
            };
        let mut split_off = 0usize;
        let mut acc_count = 0usize;
        let mut acc_bytes = 0usize;
        let mut did_split = false;
        for (batch, _) in state.enqueued.iter() {
            let cur_count = batch.len();
            let cur_bytes = batch.iter().map(|r| r.len()).sum::<usize>();
            if split_off > 0 && acc_count + cur_count >= batch_size {
                did_split = true;
                break;
            }
            if split_off > 0 && acc_bytes + cur_bytes >= self.options.batch_size_bytes {
                did_split = true;
                break;
            }
            acc_count += cur_count;
            acc_bytes += cur_bytes;
            split_off += 1;
        }
        // If we haven't waited the batch interval since last write, and we didn't break early, wait for more data.
        if !did_split
            && state.last_batch.elapsed()
                < Duration::from_micros(self.options.batch_interval_us as u64)
        {
            // This notify makes sure the background picks up the work and makes progress at end of
            // the batching interval.
            self.write_finished.notify_one();
            return Ok(None);
        }
        if split_off == 0 {
            // No work to do.
            self.write_finished.notify_one();
            return Ok(None);
        }
        let Some((fragment_seq_no, log_position)) =
            state.select_for_write(&self.options, manifest_manager, acc_count)?
        else {
            // No fragment can be written at this time.
            return Ok(None);
        };
        let mut work = std::mem::take(&mut state.enqueued);
        state.enqueued = work.split_off(split_off);
        state.last_batch = Instant::now();
        Ok(Some((fragment_seq_no, log_position, work)))
    }

    pub fn update_average_batch_size(&self, records: usize) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Calculate the batch size based upon the average number of records written per batch.  Add
    /// 10% to the batch size to make it always grow and open up up to the limits of throttling.
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

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chroma_storage::s3_client_for_test_with_new_bucket;

    use super::*;
    use crate::manifest_manager::ManifestManager;
    use crate::{LogWriterOptions, Manifest, SnapshotOptions, ThrottleOptions};

    #[tokio::test]
    async fn test_k8s_integration_batches() {
        let batch_manager = BatchManager::new(ThrottleOptions {
            throughput: 100,
            headroom: 1,
            batch_size_bytes: 4,
            batch_interval_us: 1_000_000,
        })
        .unwrap();
        let storage = s3_client_for_test_with_new_bucket().await;
        Manifest::initialize(
            &LogWriterOptions::default(),
            &storage,
            "test-batches-prefix",
            "initializer",
        )
        .await
        .unwrap();
        let manifest_manager = ManifestManager::new(
            ThrottleOptions::default(),
            SnapshotOptions::default(),
            storage.into(),
            "test-batches-prefix".to_string(),
            "writer".to_string(),
        )
        .await
        .unwrap();
        let (tx, _rx1) = tokio::sync::oneshot::channel();
        batch_manager.push_work(vec![vec![1]], tx);
        let (tx, _rx2) = tokio::sync::oneshot::channel();
        batch_manager.push_work(vec![vec![2, 3]], tx);
        let (tx, _rx3) = tokio::sync::oneshot::channel();
        batch_manager.push_work(vec![vec![4, 5, 6]], tx);
        let (seq_no, log_position, work) =
            batch_manager.take_work(&manifest_manager).unwrap().unwrap();
        assert_eq!(seq_no, FragmentSeqNo(1));
        assert_eq!(log_position.offset(), 1);
        assert_eq!(2, work.len());
        // Check batch 1
        assert_eq!(vec![vec![1]], work[0].0);
        // Check batch 2
        assert_eq!(vec![vec![2, 3]], work[1].0);
    }
}
