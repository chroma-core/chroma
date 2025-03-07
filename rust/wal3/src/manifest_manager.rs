use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use chroma_storage::{ETag, Storage};
use setsum::Setsum;

use crate::manifest::{Manifest, Snapshot};
use crate::{Error, Fragment, FragmentSeqNo, LogPosition, SnapshotOptions, ThrottleOptions};

////////////////////////////////////////// ManifestAndETag /////////////////////////////////////////

#[derive(Debug)]
struct ManifestAndETag {
    manifest: Manifest,
    e_tag: ETag,
}

////////////////////////////////////////////// Staging /////////////////////////////////////////////

#[derive(Debug)]
struct Staging {
    /// Poisioned by the background thread.
    poison: Option<Error>,
    /// Options for rate limiting and batching the manifest.
    throttle: ThrottleOptions,
    /// Options related to snapshots.
    snapshot: SnapshotOptions,
    /// The prefix to store the log under in object storage.
    prefix: String,
    /// The unique ID of the process doing the writing.
    writer: String,
    /// This is the manifest and e-tag most recently witnessed in storage.  It will be gotten at
    /// startup and will be maintained by the background thread.
    stable: ManifestAndETag,
    /// Fragments that are waiting to be applied.  These are fragments that are in any order.
    fragments: Vec<(Fragment, tokio::sync::oneshot::Sender<Option<Error>>)>,
    /// In-flight snapshots.  These are being uploaded.  This serves to dedupe the uploads.
    snapshots_in_flight: Vec<Snapshot>,
    /// Snapshots that have been uploaded and are free for a manifest to reference.
    snapshots_staged: Vec<Setsum>,
    /// The next timestamp to assign.
    next_log_position: LogPosition,
    /// The next fragment sequence number to assign to a not-yet completed fragment upload  .
    next_seq_no_to_assign: FragmentSeqNo,
    /// The next fragment sequence number to look for when applying fragments.
    next_seq_no_to_apply: FragmentSeqNo,
    /// The instant at which the last batch was generated.
    last_batch: Instant,
}

impl Staging {
    /// Pull work from the staging area.  This will return the stable manifest and its etag, the
    /// new manifest and the sequence number at which to write it.  Optionally returns a snapshot
    /// that the current log could use to trim itself.  Lastly, return a vector of notification
    /// channels to alert every fragment waiting on this manifest that it's ready.
    #[allow(clippy::type_complexity)]
    fn pull_work(
        &mut self,
    ) -> Option<(
        Manifest,
        ETag,
        Manifest,
        FragmentSeqNo,
        Option<Snapshot>,
        Vec<tokio::sync::oneshot::Sender<Option<Error>>>,
    )> {
        // No fragment, no work.
        if self.fragments.is_empty() {
            return None;
        }
        // Iterate the fragments that are queued up and apply them to the manifest in order of
        // sequence_number, making sure to never leave gaps.
        let mut notifiers = vec![];
        let mut new_manifest = self.stable.manifest.clone();
        new_manifest.writer = self.writer.clone();
        let mut postpone = vec![];
        let mut fragments = std::mem::take(&mut self.fragments);
        let mut next_seq_no_to_apply = self.next_seq_no_to_apply;
        fragments.sort_by_key(|(fragment, _)| fragment.seq_no);
        for (fragment, tx) in fragments.into_iter() {
            if fragment.seq_no == next_seq_no_to_apply && new_manifest.can_apply_fragment(&fragment)
            {
                next_seq_no_to_apply += 1;
                new_manifest.apply_fragment(fragment);
                notifiers.push(tx);
            } else {
                postpone.push((fragment, tx));
            }
        }
        // The fragments that didn't make the cut.
        self.fragments = postpone;
        // No-one to notify, no work.
        if notifiers.is_empty() {
            return None;
        }
        self.last_batch = Instant::now();
        // If the manifest can create a snapshot based upon the options.
        let mut snapshot =
            new_manifest.generate_snapshot(self.snapshot, &self.prefix, &self.writer);
        if let Some(s) = snapshot.as_ref() {
            // If the snapshot has been added to object storage it will be available for the next
            // manifest that gets installed.  Apply it to the new manifest.
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
        self.next_seq_no_to_apply = next_seq_no_to_apply;
        Some((
            self.stable.manifest.clone(),
            self.stable.e_tag.clone(),
            new_manifest,
            next_seq_no_to_apply,
            snapshot,
            notifiers,
        ))
    }
}

////////////////////////////////////////// ManifestManager /////////////////////////////////////////

/// ManifestManager is responsible for managing the manifest and batching writes to it.
#[derive(Debug)]
pub struct ManifestManager {
    staging: Arc<Mutex<Staging>>,
    timer: Option<tokio::task::JoinHandle<()>>,
    background: Option<tokio::task::JoinHandle<()>>,
    notifier: Arc<tokio::sync::Notify>,
}

impl ManifestManager {
    /// Create a new manifest manager.
    pub async fn new(
        mut throttle: ThrottleOptions,
        snapshot: SnapshotOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
    ) -> Result<Self, Error> {
        // NOTE(rescrv):  Once upon a time we allowed concurrency here.  Deny it for safety.
        throttle.outstanding = 1;
        let poison = None;
        let Some((manifest, e_tag)) = Manifest::load(&storage, &prefix).await? else {
            return Err(Error::UninitializedLog);
        };
        let latest_fragment = manifest.fragments.iter().max_by_key(|f| f.limit.offset());
        let next_log_position = latest_fragment
            .map(|f| f.limit)
            .unwrap_or(LogPosition::from_offset(1));
        let next_seq_no_to_assign = latest_fragment
            .map(|f| f.seq_no + 1)
            .unwrap_or(FragmentSeqNo(1));
        let next_seq_no_to_apply = next_seq_no_to_assign;
        let stable = ManifestAndETag { manifest, e_tag };
        let staging = Arc::new(Mutex::new(Staging {
            poison,
            throttle,
            snapshot,
            prefix,
            writer,
            stable,
            fragments: vec![],
            snapshots_in_flight: vec![],
            snapshots_staged: vec![],
            next_log_position,
            next_seq_no_to_assign,
            next_seq_no_to_apply,
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

    /// Recover from a fault in writing.  It is possible that fragments have been written that are
    /// not referenced by the manifest.  Scout ahead until an empty slot is observed.  Then write
    /// the manifest that includes the new fragments.
    pub async fn recover(&mut self) -> Result<(), Error> {
        // TODO(rescrv):  Implement recovery once LogReader is complete, as the features of
        // LogReader for reading log fragments and scrubbing the log will be necessary components
        // of recovery.  It's inherently a read operation.
        Ok(())
    }

    /// Assign a timestamp to a record.
    pub fn assign_timestamp(&self, record_count: usize) -> Option<(FragmentSeqNo, LogPosition)> {
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
        let seq_no = staging.next_seq_no_to_assign;
        staging.next_seq_no_to_assign += 1u64;
        if position.offset < u64::MAX {
            Some((seq_no, position))
        } else {
            None
        }
    }

    /// Given a fragment, add it to the manifest, batch its application and wait for it to apply.
    pub async fn add_fragment(&self, fragment: Fragment) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_fragment(fragment, tx)?;
        match rx.await {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(err),
            Err(_) => Err(Error::Internal),
        }
    }

    /// Add the fragment to the queue/vector of fragments waiting to be applied.
    fn push_fragment(
        &self,
        fragment: Fragment,
        notify: tokio::sync::oneshot::Sender<Option<Error>>,
    ) -> Result<(), Error> {
        let was_empty = {
            let mut staging = self.staging.lock().unwrap();
            if let Some(err) = staging.poison.clone() {
                return Err(err);
            }
            let was_empty = staging.fragments.is_empty();
            staging.fragments.push((fragment, notify));
            was_empty
        };
        if was_empty {
            self.notifier.notify_one();
        }
        Ok(())
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
        let mut in_flight_snapshots = VecDeque::new();
        loop {
            notifier.notified().await;
            let (work, throttle) = {
                // SAFETY(rescrv):  Mutex poisoning.
                let mut staging = staging.lock().unwrap();
                (staging.pull_work(), staging.throttle)
            };
            if let Some((
                old_manifest,
                old_e_tag,
                new_manifest,
                next_seq_no_to_apply,
                snapshot,
                notifiers,
            )) = work
            {
                let done = Arc::new(AtomicBool::new(false));
                let install_one = Self::install_one(
                    throttle,
                    Arc::clone(&storage),
                    old_manifest,
                    &old_e_tag,
                    new_manifest.clone(),
                    Arc::clone(&notifier),
                    Arc::clone(&done),
                );
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
                    in_flight_snapshots.push_back((done, handle));
                }
                match install_one.await {
                    Ok(e_tag) => {
                        let mut staging = staging.lock().unwrap();
                        staging.next_seq_no_to_apply = next_seq_no_to_apply;
                        staging.stable = ManifestAndETag {
                            manifest: new_manifest,
                            e_tag,
                        };
                        for notifier in notifiers.into_iter() {
                            notifier.send(None).unwrap();
                        }
                    }
                    Err(e) => {
                        for notifier in notifiers.into_iter() {
                            notifier.send(Some(e.clone())).unwrap();
                        }
                        let mut staging = staging.lock().unwrap();
                        staging.poison = Some(e);
                    }
                }
            }
        }
    }

    /// Install one manifest, returning its etag or erroring.
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

    /// Install a snapshot.  Can succeed or error.
    /// Clears the bookkeeping regardless so the manifest snapshot can retry.
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
    use chroma_storage::s3_client_for_test_with_new_bucket;

    use crate::*;

    #[tokio::test]
    async fn test_k8s_manager_staging() {
        // NOTE(rescrv):  This stest doesn't check writes to storage.  It just tracks the logic of
        // the manager.
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        Manifest::initialize(
            &LogWriterOptions::default(),
            &storage,
            "prefix",
            "init in test",
        )
        .await
        .unwrap();
        let mut manager = ManifestManager::new(
            ThrottleOptions::default(),
            SnapshotOptions::default(),
            storage,
            "prefix".to_string(),
            "manager in test".to_string(),
        )
        .await
        .unwrap();
        // Kill the background process so we can test the batch logic.
        if let Some(background) = manager.background.take() {
            background.abort();
        }
        let (d1_tx, mut d1_rx) = tokio::sync::oneshot::channel();
        manager
            .push_fragment(
                Fragment {
                    path: "path2".to_string(),
                    seq_no: FragmentSeqNo(2),
                    num_bytes: 20,
                    start: LogPosition::uni(22),
                    limit: LogPosition::uni(42),
                    setsum: Setsum::default(),
                },
                d1_tx,
            )
            .unwrap();
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work()
        };
        assert!(work.is_none());
        assert!(d1_rx.try_recv().is_err());
        let (d2_tx, mut d2_rx) = tokio::sync::oneshot::channel();
        manager
            .push_fragment(
                Fragment {
                    path: "path1".to_string(),
                    seq_no: FragmentSeqNo(1),
                    num_bytes: 30,
                    start: LogPosition::uni(1),
                    limit: LogPosition::uni(22),
                    setsum: Setsum::default(),
                },
                d2_tx,
            )
            .unwrap();
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work().unwrap()
        };
        // pretend to install the manifest....
        // now finish work
        for n in work.5 {
            n.send(None).unwrap();
        }
        assert!(d1_rx.try_recv().is_ok());
        assert!(d2_rx.try_recv().is_ok());
        let staging = manager.staging.lock().unwrap();
        assert!(staging.fragments.is_empty());
        assert_eq!(
            Manifest {
                path: String::from("prefix/manifest/MANIFEST"),
                writer: "init in test".to_string(),
                setsum: Setsum::default(),
                acc_bytes: 0,
                snapshots: vec![],
                fragments: vec![],
            },
            work.0
        );
        assert_eq!(
            Manifest {
                path: String::from("prefix/manifest/MANIFEST"),
                writer: "manager in test".to_string(),
                setsum: Setsum::default(),
                acc_bytes: 50,
                snapshots: vec![],
                fragments: vec![
                    Fragment {
                        path: "path1".to_string(),
                        seq_no: FragmentSeqNo(1),
                        num_bytes: 30,
                        start: LogPosition::uni(1),
                        limit: LogPosition::uni(22),
                        setsum: Setsum::default(),
                    },
                    Fragment {
                        path: "path2".to_string(),
                        seq_no: FragmentSeqNo(2),
                        num_bytes: 20,
                        start: LogPosition::uni(22),
                        limit: LogPosition::uni(42),
                        setsum: Setsum::default(),
                    }
                ],
            },
            work.2
        );
        assert_eq!(None, work.4);
    }
}
