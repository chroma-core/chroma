use std::collections::LinkedList;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use object_store::{ObjectStore, Result};

use crate::manifest::{Manifest, ShardFragment, Snapshot, SnapshotOptions};
use crate::{
    Error, LogPosition, ThrottleOptions, APPLY_DELTA, APPLY_SNAPSHOT_FAILED, GENERATE_POINTERS,
    LOG_FULL, NO_APPROPRIATE_DELTA, NO_DELTAS, PULL_WORK, PUSH_DELTA,
};

////////////////////////////////////////////// Staging /////////////////////////////////////////////

#[derive(Debug)]
struct Staging {
    /// Options for rate limiting.
    throttle: ThrottleOptions,
    // Options related to snapshots.
    snapshot: SnapshotOptions,
    /// The manifest that is most recently created.  This will be the most recent manifest
    /// in-flight.
    manifest: Manifest,
    /// Deltas that are waiting to be applied.  These are shard fragments that are out of order.
    deltas: Vec<(
        ShardFragment,
        DeltaSeqNo,
        tokio::sync::oneshot::Sender<Option<Error>>,
    )>,
    /// In-flight snapshots.
    snapshots_in_flight: Vec<Snapshot>,
    /// Snapshots that have been uploaded and are free for a manifest to claim.
    snapshots_staged: Vec<sst::Setsum>,
    /// The next timestamp to assign.
    timestamp: LogPosition,
    /// The sequence number of the next shard assigned.
    next_seq_no_to_assign: u64,
    /// The sequence number of the next delta to apply.
    next_seq_no_to_apply: u64,
    /// The instant at which the last batch was generated.
    last_batch: Instant,
}

impl Staging {
    #[allow(clippy::type_complexity)]
    fn pull_work(
        &mut self,
    ) -> Option<(
        Manifest,
        Manifest,
        Option<Snapshot>,
        Vec<tokio::sync::oneshot::Sender<Option<Error>>>,
    )> {
        if self.deltas.is_empty() {
            NO_DELTAS.click();
            return None;
        }
        let mut notifiers = vec![];
        let mut new_manifest = self.manifest.clone();
        let mut postpone = vec![];
        let mut deltas = std::mem::take(&mut self.deltas);
        deltas.sort_by_key(|(_, delta_seq_no, _)| *delta_seq_no);
        for (delta, delta_seq_no, tx) in deltas.into_iter() {
            if delta_seq_no == DeltaSeqNo(self.next_seq_no_to_apply)
                && new_manifest.can_apply_fragment(&delta)
            {
                self.next_seq_no_to_apply += 1;
                new_manifest.apply_fragment(delta);
                notifiers.push(tx);
            } else {
                postpone.push((delta, delta_seq_no, tx));
            }
        }
        self.deltas = postpone;
        if notifiers.is_empty() {
            NO_APPROPRIATE_DELTA.click();
            return None;
        }
        if new_manifest.generate_pointers(&self.manifest).is_err() {
            GENERATE_POINTERS.click();
            return None;
        }
        PULL_WORK.click();
        self.last_batch = Instant::now();
        let mut snapshot = new_manifest.generate_snapshot(self.snapshot);
        if let Some(s) = snapshot.as_ref() {
            if self.snapshots_staged.contains(&s.setsum) {
                self.snapshots_staged.retain(|ss| ss != &s.setsum);
                if let Err(err) = new_manifest.apply_snapshot(s) {
                    println!("apply snapshot failed: {:?}", err);
                    APPLY_SNAPSHOT_FAILED.click();
                } else {
                    snapshot = None;
                }
            } else if self
                .snapshots_in_flight
                .iter()
                .any(|ss| ss.setsum == s.setsum)
            {
                snapshot = None;
            } else {
                self.snapshots_in_flight.push(s.clone());
            }
        }
        let mut old_manifest = new_manifest.clone();
        std::mem::swap(&mut old_manifest, &mut self.manifest);
        Some((old_manifest, new_manifest, snapshot, notifiers))
    }
}

//////////////////////////////////////////// DeltaSeqNo ////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeltaSeqNo(u64);

////////////////////////////////////////// ManifestManager /////////////////////////////////////////

#[derive(Debug)]
pub struct ManifestManager {
    staging: Arc<Mutex<Staging>>,
    timer: Option<tokio::task::JoinHandle<()>>,
    background: Option<tokio::task::JoinHandle<()>>,
    notifier: Arc<tokio::sync::Notify>,
}

impl ManifestManager {
    pub async fn new(
        throttle: ThrottleOptions,
        snapshot: SnapshotOptions,
        manifest: Manifest,
        object_store: Arc<impl ObjectStore>,
    ) -> Self {
        let timestamp = manifest
            .fragments
            .iter()
            .map(|f| f.limit)
            .max()
            .unwrap_or(LogPosition(1));
        let staging = Arc::new(Mutex::new(Staging {
            throttle,
            snapshot,
            manifest,
            deltas: vec![],
            snapshots_in_flight: vec![],
            snapshots_staged: vec![],
            timestamp,
            next_seq_no_to_assign: 1,
            next_seq_no_to_apply: 1,
            last_batch: Instant::now(),
        }));
        let notifier = Arc::new(tokio::sync::Notify::new());
        let timer = Some(tokio::task::spawn(Self::timer(
            Arc::clone(&staging),
            Arc::clone(&notifier),
        )));
        let background = Some(tokio::task::spawn(Self::background(
            Arc::clone(&staging),
            object_store,
            Arc::clone(&notifier),
        )));
        Self {
            staging,
            timer,
            background,
            notifier,
        }
    }

    /// Assign a timestamp to a record.
    pub fn assign_timestamp(&self, record_count: usize) -> Option<(LogPosition, DeltaSeqNo)> {
        let epoch_ms = LogPosition(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis()
                .try_into()
                .unwrap(),
        );
        // SAFETY(rescrv):  Mutex poisoning.
        let mut staging = self.staging.lock().unwrap();
        if staging.timestamp < epoch_ms {
            staging.timestamp = epoch_ms;
        }
        let position = staging.timestamp;
        let seq_no = DeltaSeqNo(staging.next_seq_no_to_assign);
        staging.timestamp = LogPosition(staging.timestamp.0.saturating_add(record_count as u64));
        staging.next_seq_no_to_assign = staging.next_seq_no_to_assign.saturating_add(1);
        if staging.timestamp < LogPosition(u64::MAX) {
            Some((position, seq_no))
        } else {
            LOG_FULL.click();
            None
        }
    }

    pub async fn apply_delta(
        &self,
        delta: ShardFragment,
        delta_seq_no: DeltaSeqNo,
    ) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_delta(delta, delta_seq_no, tx);
        match rx.await {
            Ok(None) => {
                APPLY_DELTA.click();
                Ok(())
            }
            Ok(Some(err)) => Err(err),
            Err(_) => Err(Error::Internal),
        }
    }

    fn push_delta(
        &self,
        delta: ShardFragment,
        delta_seq_no: DeltaSeqNo,
        notify: tokio::sync::oneshot::Sender<Option<Error>>,
    ) {
        PUSH_DELTA.click();
        let was_empty = {
            let mut staging = self.staging.lock().unwrap();
            let was_empty = staging.deltas.is_empty();
            staging.deltas.push((delta, delta_seq_no, notify));
            was_empty
        };
        if was_empty {
            self.notifier.notify_one();
        }
    }

    async fn timer(staging: Arc<Mutex<Staging>>, notifier: Arc<tokio::sync::Notify>) {
        loop {
            let (throttle, last_batch) = {
                // SAFETY(rescrv):  Mutex poisoning.
                let staging = staging.lock().unwrap();
                (staging.throttle, staging.last_batch)
            };
            let elapsed = last_batch.elapsed();
            let batch_interval = Duration::from_micros(throttle.batch_interval as u64);
            if elapsed > batch_interval {
                notifier.notify_one();
                tokio::time::sleep(batch_interval).await;
            } else {
                tokio::time::sleep(batch_interval - elapsed).await;
            }
        }
    }

    async fn background(
        staging: Arc<Mutex<Staging>>,
        object_store: Arc<impl ObjectStore>,
        notifier: Arc<tokio::sync::Notify>,
    ) {
        let mut in_flight = LinkedList::default();
        loop {
            notifier.notified().await;
            let (work, throttle) = {
                // SAFETY(rescrv):  Mutex poisoning.
                let mut staging = staging.lock().unwrap();
                if in_flight.len() < staging.throttle.outstanding {
                    (staging.pull_work(), staging.throttle)
                } else {
                    (None, staging.throttle)
                }
            };
            if let Some((old_manifest, new_manifest, snapshot, notifiers)) = work {
                let done = Arc::new(AtomicBool::new(false));
                let install_one = Self::install_one(
                    throttle,
                    Arc::clone(&object_store),
                    old_manifest,
                    new_manifest.clone(),
                    Arc::clone(&notifier),
                    Arc::clone(&done),
                );
                let handle = tokio::task::spawn(install_one);
                in_flight.push_back((done, handle, notifiers));
                if let Some(snapshot) = snapshot {
                    let done = Arc::new(AtomicBool::new(false));
                    let install = Self::install_snapshot(
                        throttle,
                        Arc::clone(&staging),
                        Arc::clone(&object_store),
                        snapshot,
                        Arc::clone(&notifier),
                        Arc::clone(&done),
                    );
                    let handle = tokio::task::spawn(install);
                    in_flight.push_back((done, handle, vec![]));
                }
            }
            while in_flight
                .front()
                .map(|f| f.0.load(Ordering::Relaxed))
                .unwrap_or_default()
                || in_flight.len() >= throttle.outstanding
            {
                if let Some((_, handle, notifiers)) = in_flight.pop_front() {
                    let err = handle.await.unwrap();
                    for notifier in notifiers {
                        let _ = notifier.send(err.clone());
                    }
                }
            }
        }
    }

    async fn install_one(
        throttle: ThrottleOptions,
        object_store: Arc<impl ObjectStore>,
        old_manifest: Manifest,
        new_manifest: Manifest,
        notifier: Arc<tokio::sync::Notify>,
        done: Arc<AtomicBool>,
    ) -> Option<Error> {
        match old_manifest
            .install(&throttle, &*object_store, &new_manifest)
            .await
        {
            Ok(_) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                None
            }
            Err(e) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                Some(e)
            }
        }
    }

    async fn install_snapshot(
        throttle: ThrottleOptions,
        staging: Arc<Mutex<Staging>>,
        object_store: Arc<impl ObjectStore>,
        snapshot: Snapshot,
        notifier: Arc<tokio::sync::Notify>,
        done: Arc<AtomicBool>,
    ) -> Option<Error> {
        let res = match snapshot.install(&throttle, &*object_store).await {
            Ok(_) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                None
            }
            Err(e) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                Some(e)
            }
        };
        // SAFETY(rescrv):  Mutex poisoning.
        let mut staging = staging.lock().unwrap();
        staging.snapshots_staged.push(snapshot.setsum);
        staging
            .snapshots_in_flight
            .retain(|s| s.setsum != snapshot.setsum);
        res
    }
}

impl Drop for ManifestManager {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        if let Some(background) = self.background.take() {
            background.abort();
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use crate::manifest::{manifest_path, NextPointer, PrevPointer, ShardFragment};
    use crate::{ShardID, ShardSeqNo};

    use super::*;

    #[tokio::test]
    async fn manager_staging() {
        let manifest = Manifest {
            path: String::from("manifest/MANIFEST.0"),
            writer: "manifest writer 1".to_string(),
            setsum: sst::Setsum::default(),
            snapshots: vec![],
            fragments: vec![],
            prev: None,
            next: NextPointer {
                path_to_manifest: manifest_path(42),
            },
        };
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let mut manager = ManifestManager::new(
            ThrottleOptions::default(),
            SnapshotOptions::default(),
            manifest,
            object_store,
        )
        .await;
        if let Some(background) = manager.background.take() {
            background.abort();
        }
        let (d1_tx, mut d1_rx) = tokio::sync::oneshot::channel();
        manager.push_delta(
            ShardFragment {
                path: "path2".to_string(),
                shard_id: ShardID(1),
                seq_no: ShardSeqNo(2),
                start: LogPosition(22),
                limit: LogPosition(42),
                setsum: sst::Setsum::default(),
            },
            DeltaSeqNo(2),
            d1_tx,
        );
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work()
        };
        assert!(work.is_none());
        assert!(d1_rx.try_recv().is_err());
        let (d2_tx, mut d2_rx) = tokio::sync::oneshot::channel();
        manager.push_delta(
            ShardFragment {
                path: "path1".to_string(),
                shard_id: ShardID(1),
                seq_no: ShardSeqNo(1),
                start: LogPosition(1),
                limit: LogPosition(22),
                setsum: sst::Setsum::default(),
            },
            DeltaSeqNo(1),
            d2_tx,
        );
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work().unwrap()
        };
        // pretend to install the manifest....
        // now finish work
        for n in work.3 {
            n.send(None).unwrap();
        }
        assert!(d1_rx.try_recv().is_ok());
        assert!(d2_rx.try_recv().is_ok());
        let staging = manager.staging.lock().unwrap();
        assert!(staging.deltas.is_empty());
        assert_eq!(
            Manifest {
                path: String::from("manifest/MANIFEST.42"),
                writer: "manifest writer 1".to_string(),
                setsum: sst::Setsum::default(),
                snapshots: vec![],
                fragments: vec![
                    ShardFragment {
                        path: "path1".to_string(),
                        shard_id: ShardID(1),
                        seq_no: ShardSeqNo(1),
                        start: LogPosition(1),
                        limit: LogPosition(22),
                        setsum: sst::Setsum::default(),
                    },
                    ShardFragment {
                        path: "path2".to_string(),
                        shard_id: ShardID(1),
                        seq_no: ShardSeqNo(2),
                        start: LogPosition(22),
                        limit: LogPosition(42),
                        setsum: sst::Setsum::default(),
                    }
                ],
                prev: Some(PrevPointer {
                    path_to_manifest: manifest_path(0),
                    setsum: sst::Setsum::default(),
                }),
                next: staging.manifest.next.clone(),
            },
            staging.manifest
        );
    }
}
