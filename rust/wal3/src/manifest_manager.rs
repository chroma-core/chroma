use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use chroma_storage::{ETag, Storage};
use setsum::Setsum;

use crate::manifest::{Manifest, Snapshot};
use crate::{
    DeltaSeqNo, Error, Fragment, FragmentSeqNo, LogPosition, SnapshotOptions, ThrottleOptions,
};

////////////////////////////////////////// ManifestAndETag /////////////////////////////////////////

#[derive(Debug)]
struct ManifestAndETag {
    manifest: Manifest,
    e_tag: ETag,
}

////////////////////////////////////////////// Staging /////////////////////////////////////////////

#[derive(Debug)]
struct Staging {
    /// Options for rate limiting.
    throttle: ThrottleOptions,
    /// Options related to snapshots.
    snapshot: SnapshotOptions,
    /// This is the manifest and e-tag most recently witnessed in storage.  It will be gotten at
    /// startup and will be maintained by the background thread.
    stable: ManifestAndETag,
    /// Deltas that are waiting to be applied.  These are fragments that are in any order.
    deltas: Vec<(
        Fragment,
        DeltaSeqNo,
        tokio::sync::oneshot::Sender<Option<Error>>,
    )>,
    /// In-flight snapshots.  These are being uploaded.  This serves to dedupe the uploads.
    snapshots_in_flight: Vec<Snapshot>,
    /// Snapshots that have been uploaded and are free for a manifest to claim.
    snapshots_staged: Vec<Setsum>,
    /// The next timestamp to assign.
    next_log_position: LogPosition,
    /*
    /// The sequence number of the next shard assigned.
    next_seq_no_to_assign: u64,
    /// The sequence number of the next delta to apply.
    next_seq_no_to_apply: u64,
    */
    /// The instant at which the last batch was generated.
    last_batch: Instant,
}

impl Staging {
    #[allow(clippy::type_complexity)]
    fn pull_work(
        &mut self,
    ) -> Option<(
        Manifest,
        ETag,
        Manifest,
        Option<Snapshot>,
        Vec<tokio::sync::oneshot::Sender<Option<Error>>>,
    )> {
        /*
        if self.deltas.is_empty() {
            return None;
        }
        let mut notifiers = vec![];
        let mut new_manifest = self.stable.manifest.clone();
        let mut postpone = vec![];
        let mut deltas = std::mem::take(&mut self.deltas);
        let mut next_seq_no_to_apply = self.next_seq_no_to_apply;
        deltas.sort_by_key(|(_, delta_seq_no, _)| *delta_seq_no);
        for (delta, delta_seq_no, tx) in deltas.into_iter() {
            if delta_seq_no == DeltaSeqNo(next_seq_no_to_apply)
                && new_manifest.can_apply_fragment(&delta)
            {
                next_seq_no_to_apply += 1;
                new_manifest.apply_fragment(delta);
                notifiers.push(tx);
            } else {
                postpone.push((delta, delta_seq_no, tx));
            }
        }
        self.deltas = postpone;
        if notifiers.is_empty() {
            return None;
        }
        self.last_batch = Instant::now();
        let mut snapshot = new_manifest.generate_snapshot(self.snapshot);
        if let Some(s) = snapshot.as_ref() {
            if self.snapshots_staged.contains(&s.setsum) {
                if let Err(err) = new_manifest.apply_snapshot(s) {
                    // It failed to apply, so error everyone waiting.  The backoff/retry/reseat
                    // logic has to accommodate this use case.
                    tracing::error!("Failed to apply snapshot: {:?}", err);
                    for notifier in notifiers {
                        let _ = notifier.send(Some(err.clone()));
                    }
                    return None;
                } else {
                    // This snapshot has been applied.  Remove it from the staged snapshots.
                    self.snapshots_staged.retain(|ss| ss != &s.setsum);
                    snapshot = None;
                }
            } else if self
                .snapshots_in_flight
                .iter()
                .any(|ss| ss.setsum == s.setsum)
            {
                // This snapshot is already in flight.
                // Do not return it and instead rely upon our bookkeeping.
                snapshot = None;
            } else {
                self.snapshots_in_flight.push(s.clone());
            }
        }
        Some((
            self.stable.manifest.clone(),
            self.stable.e_tag.clone(),
            new_manifest,
            snapshot,
            notifiers,
        ))
        */
        todo!();
    }
}

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
        storage: Arc<Storage>,
    ) -> Result<Self, Error> {
        let Some((manifest, e_tag)) = Manifest::load(&storage).await? else {
            return Err(Error::UninitializedLog);
        };
        let latest_fragment = manifest.fragments.iter().max_by_key(|f| f.limit.offset());
        let next_log_position = latest_fragment
            .map(|f| f.limit)
            .unwrap_or(LogPosition::from_offset(1));
        let stable = ManifestAndETag { manifest, e_tag };
        let staging = Arc::new(Mutex::new(Staging {
            throttle,
            snapshot,
            stable,
            deltas: vec![],
            snapshots_in_flight: vec![],
            snapshots_staged: vec![],
            next_log_position,
            last_batch: Instant::now(),
        }));
        let notifier = Arc::new(tokio::sync::Notify::new());
        let timer = Some(tokio::task::spawn(Self::timer(
            Arc::clone(&staging),
            Arc::clone(&notifier),
        )));
        let background = Some(tokio::task::spawn(Self::background(
            Arc::clone(&staging),
            storage,
            Arc::clone(&notifier),
        )));
        Ok(Self {
            staging,
            timer,
            background,
            notifier,
        })
    }

    /// Assign a timestamp to a record.
    pub fn assign_timestamp(
        &self,
        record_count: usize,
    ) -> Option<(FragmentSeqNo, LogPosition, DeltaSeqNo)> {
        /*
        let epoch_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros() as u64;
        // SAFETY(rescrv):  Mutex poisoning.
        let mut staging = self.staging.lock().unwrap();
        // Advance time.
        if staging.next_log_position.timestamp_us < epoch_micros {
            staging.next_log_position.timestamp_us = epoch_micros;
        }
        // Steal the offset.
        let position = staging.next_log_position;
        // Advance the offset for the next assign_timestamp call.
        staging.next_log_position.offset = staging
            .next_log_position
            .offset
            .saturating_add(record_count as u64);
        let seq_no = DeltaSeqNo(staging.next_seq_no_to_assign);
        staging.next_seq_no_to_assign = staging.next_seq_no_to_assign.saturating_add(1);
        if position.offset < u64::MAX {
            Some((todo!(), position, seq_no))
        } else {
            None
        }
        */
        todo!();
    }

    /// Given a delta to the manifest, batch its application and wait for it to apply.
    pub async fn apply_delta(
        &self,
        delta: Fragment,
        delta_seq_no: DeltaSeqNo,
    ) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_delta(delta, delta_seq_no, tx);
        match rx.await {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(err),
            Err(_) => Err(Error::Internal),
        }
    }

    /// Add the delta to the queue/vector of deltas waiting to be applied.
    fn push_delta(
        &self,
        delta: Fragment,
        delta_seq_no: DeltaSeqNo,
        notify: tokio::sync::oneshot::Sender<Option<Error>>,
    ) {
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

    /// At a periodic interval consistent with throttle options, pump the notifier to wake up the
    /// background thread.  This drives the batching.
    async fn timer(staging: Arc<Mutex<Staging>>, notifier: Arc<tokio::sync::Notify>) {
        loop {
            let (throttle, last_batch) = {
                // SAFETY(rescrv):  Mutex poisoning.
                let staging = staging.lock().unwrap();
                (staging.throttle, staging.last_batch)
            };
            let elapsed = last_batch.elapsed();
            let batch_interval = Duration::from_micros(throttle.batch_interval_us as u64);
            if elapsed > batch_interval {
                notifier.notify_one();
                tokio::time::sleep(batch_interval).await;
            } else {
                tokio::time::sleep(batch_interval - elapsed).await;
            }
        }
    }

    /// The background thread for installing manifests.
    async fn background(
        staging: Arc<Mutex<Staging>>,
        storage: Arc<Storage>,
        notifier: Arc<tokio::sync::Notify>,
    ) {
        /*
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
                let old_e_tag = todo!();
                let done = Arc::new(AtomicBool::new(false));
                let install_one = Self::install_one(
                    throttle,
                    Arc::clone(&storage),
                    old_manifest,
                    old_e_tag,
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
                        Arc::clone(&storage),
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
                        let _ = notifier.send(err.clone().err());
                    }
                }
            }
        }
        */
    }

    async fn install_one(
        throttle: ThrottleOptions,
        storage: Arc<Storage>,
        old_manifest: Manifest,
        old_e_tag: &ETag,
        new_manifest: Manifest,
        notifier: Arc<tokio::sync::Notify>,
        done: Arc<AtomicBool>,
    ) -> Result<ETag, Error> {
        match old_manifest
            .install(&throttle, &storage, Some(old_e_tag), &new_manifest)
            .await
        {
            Ok(e_tag) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                Ok(e_tag)
            }
            Err(e) => {
                done.store(true, Ordering::Relaxed);
                notifier.notify_one();
                Err(e)
            }
        }
    }

    async fn install_snapshot(
        throttle: ThrottleOptions,
        staging: Arc<Mutex<Staging>>,
        storage: Arc<Storage>,
        snapshot: Snapshot,
        notifier: Arc<tokio::sync::Notify>,
        done: Arc<AtomicBool>,
    ) -> Option<Error> {
        let res = match snapshot.install(&throttle, &storage).await {
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
    use crate::manifest::manifest_path;
    use crate::{Fragment, FragmentSeqNo};

    use chroma_storage::{test_storage, ETag};

    use super::*;

    /*
        #[tokio::test]
        async fn manager_staging() {
            // NOTE(rescrv):  This stest doesn't check writes to storage.  It just tracks the logic of
            // the manager.
            let manifest = Manifest {
                path: manifest_path(),
                writer: "manifest writer 1".to_string(),
                setsum: Setsum::default(),
                snapshots: vec![],
                fragments: vec![],
            };
            let storage = Arc::new(test_storage());
            let mut manager = ManifestManager::new(
                ThrottleOptions::default(),
                SnapshotOptions::default(),
                manifest,
                storage,
            )
            .await;
            if let Some(background) = manager.background.take() {
                background.abort();
            }
            let (d1_tx, mut d1_rx) = tokio::sync::oneshot::channel();
            manager.push_delta(
                Fragment {
                    path: "path2".to_string(),
                    seq_no: FragmentSeqNo(2),
                    start: LogPosition::uni(22),
                    limit: LogPosition::uni(42),
                    setsum: Setsum::default(),
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
                Fragment {
                    path: "path1".to_string(),
                    seq_no: FragmentSeqNo(1),
                    start: LogPosition::uni(1),
                    limit: LogPosition::uni(22),
                    setsum: Setsum::default(),
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
            for n in work.4 {
                n.send(None).unwrap();
            }
            assert!(d1_rx.try_recv().is_ok());
            assert!(d2_rx.try_recv().is_ok());
            let staging = manager.staging.lock().unwrap();
            assert!(staging.deltas.is_empty());
            assert_eq!(
                Manifest {
                    path: String::from("manifest/MANIFEST"),
                    writer: "manifest writer 1".to_string(),
                    setsum: Setsum::default(),
                    snapshots: vec![],
                    fragments: vec![
                        Fragment {
                            path: "path1".to_string(),
                            seq_no: FragmentSeqNo(1),
                            start: LogPosition::uni(1),
                            limit: LogPosition::uni(22),
                            setsum: Setsum::default(),
                        },
                        Fragment {
                            path: "path2".to_string(),
                            seq_no: FragmentSeqNo(2),
                            start: LogPosition::uni(22),
                            limit: LogPosition::uni(42),
                            setsum: Setsum::default(),
                        }
                    ],
                },
                staging.stable.manifest
            );
        }
    */
}
