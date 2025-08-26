use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::Span;

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
        Span,
    )>,
    tearing_down: bool,
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

impl Drop for ManagerState {
    fn drop(&mut self) {
        for (_, notify, _) in std::mem::take(&mut self.enqueued).into_iter() {
            let _ = notify.send(Err(Error::LogContentionRetry));
        }
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
                tearing_down: false,
            }),
            write_finished: tokio::sync::Notify::new(),
        })
    }

    pub fn push_work(
        &self,
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        span: Span,
    ) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        if state.tearing_down {
            let _ = tx.send(Err(Error::LogContentionRetry));
            self.write_finished.notify_one();
        } else if state.backoff {
            let _ = tx.send(Err(Error::Backoff));
            self.write_finished.notify_one();
        } else {
            state.enqueued.push((messages, tx, span));
        }
    }

    pub async fn wait_for_writable(&self) {
        self.write_finished.notified().await;
    }

    pub fn pump_write_finished(&self) {
        self.write_finished.notify_one();
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
                Span,
            )>,
        )>,
        Error,
    > {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();

        // We're shutting down.  Throw the work away.
        if state.tearing_down {
            self.write_finished.notify_one();
            return Ok(None);
        }

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
        for (batch, _, _) in state.enqueued.iter() {
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
            // Cannot yet select for write.  Notify will come from the timeout background is on.
            return Ok(None);
        };
        let mut work = std::mem::take(&mut state.enqueued);
        state.enqueued = work.split_off(split_off);
        if !state.enqueued.is_empty() {
            state.backoff = state
                .enqueued
                .iter()
                .map(|(recs, _, _)| recs.iter().map(|r| r.len()).sum::<usize>())
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

    pub fn shutdown(&self) {
        let enqueued = {
            let mut state = self.state.lock().unwrap();
            state.tearing_down = true;
            std::mem::take(&mut state.enqueued)
        };
        for (_, tx, _) in enqueued {
            let _ = tx.send(Err(Error::LogContentionRetry));
        }
    }

    pub fn count_waiters(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.enqueued.len()
    }

    pub fn debug_dump(&self) -> String {
        let mut output = "[batch manager]\n".to_string();
        let state = self.state.lock().unwrap();
        output += &format!("backoff: {:?}\n", state.backoff);
        output += &format!("next_write: {:?}\n", state.next_write);
        output += &format!("writers_active: {:?}\n", state.writers_active);
        output += &format!("enqueued: {}\n", state.enqueued.len());
        output
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
        batch_manager.push_work(vec![vec![1]], tx, tracing::Span::current());
        let (tx, _rx2) = tokio::sync::oneshot::channel();
        batch_manager.push_work(vec![vec![2, 3]], tx, tracing::Span::current());
        let (tx, _rx3) = tokio::sync::oneshot::channel();
        batch_manager.push_work(vec![vec![4, 5, 6]], tx, tracing::Span::current());
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
