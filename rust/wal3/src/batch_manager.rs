use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{Error, FragmentSeqNo, LogPosition, ManifestManager, ThrottleOptions};

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

/// ManagerState captures the state necessary to batch manifests.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    backoff: bool,
    next_write: Instant,
    writers_active: usize,
    enqueued: Vec<(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
    )>,
}

impl ManagerState {
    /// Set the next_write instant based upon the current time and throttle options.  We wait at
    /// least the 1/\lambda to accommodate throughput, and at least the batch interval.
    fn set_next_write(&mut self, options: &ThrottleOptions) {
        let offset = std::cmp::max(
            Duration::from_micros(1_000_000 / options.throughput as u64),
            Duration::from_micros(options.batch_interval_us as u64),
        );
        self.next_write = Instant::now() + offset;
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
    write_finished: tokio::sync::Notify,
}

impl BatchManager {
    pub fn new(options: ThrottleOptions) -> Option<Self> {
        let next_write = Instant::now();
        Some(Self {
            options,
            state: Mutex::new(ManagerState {
                backoff: false,
                next_write,
                writers_active: 0,
                enqueued: Vec::new(),
            }),
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
        if state.backoff {
            let _ = tx.send(Err(Error::Backoff));
        } else {
            state.enqueued.push((messages, tx));
        }
    }

    pub async fn wait_for_writable(&self) {
        self.write_finished.notified().await;
    }

    pub fn until_next_time(&self) -> Duration {
        // SAFETY(rescrv): Mutex poisoning.
        let state = self.state.lock().unwrap();
        let now = Instant::now();
        if now < state.next_write {
            state.next_write - now
        } else {
            Duration::ZERO
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

        let mut split_off = 0usize;
        let mut acc_count = 0usize;
        let mut acc_bytes = 0usize;
        let mut did_split = false;
        // This loop has two sets of exit conditions that are identical, but switched on
        // `short_read`.
        for (batch, _) in state.enqueued.iter() {
            let cur_count = batch.len();
            let cur_bytes = batch.iter().map(|r| r.len()).sum::<usize>();
            if split_off > 0 && acc_bytes + cur_bytes >= self.options.batch_size_bytes {
                did_split = true;
                break;
            }
            acc_count += cur_count;
            acc_bytes += cur_bytes;
            split_off += 1;
        }
        // If we haven't waited the batch interval since last write, and we didn't break early, wait for more data.
        if !did_split && state.next_write > Instant::now() {
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
        if !state.enqueued.is_empty() {
            state.backoff = state
                .enqueued
                .iter()
                .map(|(recs, _)| recs.iter().map(|r| r.len()).sum::<usize>())
                .sum::<usize>()
                >= self.options.batch_size_bytes;
            self.write_finished.notify_one();
        } else {
            state.backoff = false;
        }
        Ok(Some((fragment_seq_no, log_position, work)))
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
