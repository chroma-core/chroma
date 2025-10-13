use std::sync::{Arc, Mutex};
use std::time::Instant;

use chroma_storage::{ETag, Storage};

use crate::gc::Garbage;
use crate::manifest::{Manifest, ManifestAndETag, Snapshot};
use crate::reader::read_fragment;
use crate::writer::MarkDirty;
use crate::{
    unprefixed_fragment_path, Error, Fragment, FragmentSeqNo, GarbageCollectionOptions,
    LogPosition, SnapshotCache, SnapshotOptions, SnapshotPointerOrFragmentSeqNo, ThrottleOptions,
};

////////////////////////////////////////////// Staging /////////////////////////////////////////////

#[derive(Debug)]
struct Staging {
    /// This is the manifest and e-tag most recently witnessed in storage.  It will be gotten at
    /// startup and will be maintained by the background thread.
    stable: ManifestAndETag,
    /// Fragments that are waiting to be applied.  These are fragments that are in any order.
    fragments: Vec<(Fragment, tokio::sync::oneshot::Sender<Option<Error>>)>,
    /// The next timestamp to assign.
    next_log_position: LogPosition,
    /// The next fragment sequence number to assign to a not-yet completed fragment upload  .
    next_seq_no_to_assign: FragmentSeqNo,
    /// The next fragment sequence number to look for when applying fragments.
    next_seq_no_to_apply: FragmentSeqNo,
    /// A prefix of the log to be garbage collected.  This is added to the manager from somewhere
    /// else and the manager will apply the garbage to the next manifest that gets written.
    garbage: Option<(Garbage, tokio::sync::oneshot::Sender<Option<Error>>)>,
    /// The instant at which the last batch was generated.
    last_batch: Instant,
    /// True iff the manifest manager is closing, so we want to prevent late-arriving threads from
    /// being stuck waiting on a notify.
    tearing_down: bool,
}

impl Staging {
    /// Pull work from the staging area.  This will return the stable manifest and its etag, the
    /// new manifest and the sequence number at which to write it.  Optionally returns a snapshot
    /// that the current log could use to trim itself.  Lastly, return a vector of notification
    /// channels to alert every fragment waiting on this manifest that it's ready.
    #[allow(clippy::type_complexity)]
    fn pull_work(
        &mut self,
        manager: &ManifestManager,
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
            return self.pull_gc_work_only();
        }
        // Iterate the fragments that are queued up and apply them to the manifest in order of
        // sequence_number, making sure to never leave gaps.
        let mut notifiers = vec![];
        let mut new_manifest = self.stable.manifest.clone();
        new_manifest.writer = manager.writer.clone();
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
            return self.pull_gc_work_only();
        }
        self.last_batch = Instant::now();
        let snapshot = if let Some((garbage, notifier)) = self.garbage.take() {
            match new_manifest.apply_garbage(garbage) {
                Ok(Some(manifest)) => {
                    notifiers.push(notifier);
                    new_manifest = manifest
                }
                Ok(None) => {
                    tracing::error!("given empty garbage that did not apply");
                    let _ = notifier.send(Some(Error::GarbageCollectionPrecondition(
                        SnapshotPointerOrFragmentSeqNo::Stringy(
                            "given empty garbage that did not apply".to_string(),
                        ),
                    )));
                }
                Err(err) => {
                    notifiers.push(notifier);
                    tracing::error!("could not apply garabage: {err:?}");
                    for notifier in notifiers {
                        let _ = notifier.send(Some(err.clone()));
                    }
                    return None;
                }
            };
            None
        } else {
            // If the manifest can create a snapshot based upon the options.
            let snapshot = new_manifest.generate_snapshot(manager.snapshot, &manager.writer);
            if let Some(s) = snapshot.as_ref() {
                if let Err(err) = new_manifest.apply_snapshot(s) {
                    // It failed to apply, so error everyone waiting.  The backoff/retry/reseat
                    // logic has to accommodate this use case.
                    tracing::error!("Failed to apply snapshot: {:?}", err);
                    for notifier in notifiers {
                        let _ = notifier.send(Some(err.clone()));
                    }
                    return None;
                }
            }
            snapshot
        };
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

    /// Pull work from the staging area, focusing solely on garbage collection.
    #[allow(clippy::type_complexity)]
    fn pull_gc_work_only(
        &mut self,
    ) -> Option<(
        Manifest,
        ETag,
        Manifest,
        FragmentSeqNo,
        Option<Snapshot>,
        Vec<tokio::sync::oneshot::Sender<Option<Error>>>,
    )> {
        if let Some((garbage, notifier)) = self.garbage.take() {
            let new_manifest = match self.stable.manifest.apply_garbage(garbage) {
                Ok(Some(manifest)) => manifest,
                Ok(None) => {
                    tracing::error!("given empty garbage that did not apply");
                    let _ = notifier.send(None);
                    return None;
                }
                Err(err) => {
                    tracing::error!("could not apply garabage: {err:?}");
                    let _ = notifier.send(Some(err));
                    return None;
                }
            };
            Some((
                self.stable.manifest.clone(),
                self.stable.e_tag.clone(),
                new_manifest,
                self.next_seq_no_to_apply,
                None,
                vec![notifier],
            ))
        } else {
            None
        }
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        for (_, notify) in std::mem::take(&mut self.fragments).into_iter() {
            let _ = notify.send(Some(Error::LogContentionDurable));
        }
    }
}

////////////////////////////////////////// ManifestManager /////////////////////////////////////////

/// ManifestManager is responsible for managing the manifest and batching writes to it.
pub struct ManifestManager {
    /// Options for rate limiting and batching the manifest.
    throttle: ThrottleOptions,
    /// Options related to snapshots.
    snapshot: SnapshotOptions,
    /// Storage for the manifest manager
    storage: Arc<Storage>,
    /// The prefix to store the log under in object storage.
    prefix: String,
    /// The unique ID of the process doing the writing.
    writer: String,
    /// Staging area for manifests to be written.
    staging: Mutex<Staging>,
    /// Only one thread doing work at a time.
    do_work_mutex: tokio::sync::Mutex<()>,
}

impl ManifestManager {
    /// Create a new manifest manager.
    pub async fn new(
        throttle: ThrottleOptions,
        snapshot: SnapshotOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
    ) -> Result<Self, Error> {
        let Some((manifest, e_tag)) = Manifest::load(&throttle, &storage, &prefix).await? else {
            return Err(Error::UninitializedLog);
        };
        let next_log_position = manifest.next_write_timestamp();
        let Some(next_seq_no_to_assign) = manifest.next_fragment_seq_no() else {
            return Err(Error::LogFull);
        };
        let next_seq_no_to_apply = next_seq_no_to_assign;
        let stable = ManifestAndETag { manifest, e_tag };
        let staging = Mutex::new(Staging {
            stable,
            fragments: vec![],
            next_log_position,
            next_seq_no_to_assign,
            next_seq_no_to_apply,
            garbage: None,
            last_batch: Instant::now(),
            tearing_down: false,
        });
        let do_work_mutex = tokio::sync::Mutex::new(());
        Ok(Self {
            throttle,
            snapshot,
            storage,
            prefix,
            writer,
            staging,
            do_work_mutex,
        })
    }

    /// Signal log contention to anyone writing on the manifest.
    pub fn shutdown(&self) {
        let (fragments, garbage) = {
            let mut staging = self.staging.lock().unwrap();
            staging.tearing_down = true;
            (
                std::mem::take(&mut staging.fragments),
                std::mem::take(&mut staging.garbage),
            )
        };
        for (_, tx) in fragments {
            let _ = tx.send(Some(Error::LogContentionDurable));
        }
        if let Some((_, tx)) = garbage {
            let _ = tx.send(Some(Error::LogContentionDurable));
        }
    }

    /// Return the latest stable manifest
    pub fn latest(&self) -> ManifestAndETag {
        let staging = self.staging.lock().unwrap();
        staging.stable.clone()
    }

    /// Recover from a fault in writing.  It is possible that fragments have been written that are
    /// not referenced by the manifest.  Scout ahead until an empty slot is observed.  Then write
    /// the manifest that includes the new fragments.
    pub async fn recover(&mut self, mark_dirty: &dyn MarkDirty) -> Result<(), Error> {
        let next_seq_no_to_apply = {
            // SAFETY(rescrv):  Mutex poisoning.
            let staging = self.staging.lock().unwrap();
            staging.next_seq_no_to_apply
        };
        let next_fragment = read_fragment(
            &self.storage,
            &self.prefix,
            &unprefixed_fragment_path(next_seq_no_to_apply),
        )
        .await?;
        if let Some(fragment) = next_fragment {
            mark_dirty
                .mark_dirty(fragment.start, (fragment.limit - fragment.start) as usize)
                .await?;
            // NOTE(rescrv):  This is a hack.  We are recovering, we want to reset staging to
            // be totally consistent.  It's easier to throw it away and restart than to get the
            // adjustment right.
            match self.publish_fragment(fragment).await {
                Ok(()) => Err(Error::LogContentionRetry),
                Err(Error::LogContentionDurable) => Err(Error::LogContentionRetry),
                err => err,
            }
        } else {
            Ok(())
        }
    }

    /// Check for log contention at the cost of a read to S3.
    pub async fn heartbeat(&self) -> Result<(), Error> {
        let Some((_, e_tag)) = Manifest::load(&self.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        // SAFETY(rescrv):  Mutex poisoning.
        let staging = self.staging.lock().unwrap();
        if e_tag == staging.stable.e_tag {
            Ok(())
        } else {
            Err(Error::LogContentionFailure)
        }
    }

    /// Assign a timestamp to a record.
    pub fn assign_timestamp(&self, record_count: usize) -> Option<(FragmentSeqNo, LogPosition)> {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut staging = self.staging.lock().unwrap();
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

    fn push_work(&self, fragment: Fragment, tx: tokio::sync::oneshot::Sender<Option<Error>>) {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut staging = self.staging.lock().unwrap();
        if staging.tearing_down {
            let _ = tx.send(Some(Error::LogContentionDurable));
        } else {
            staging.fragments.push((fragment, tx));
        }
    }

    /// Given a fragment, add it to the manifest, batch its application and wait for it to apply.
    #[tracing::instrument(skip(self, fragment))]
    pub async fn publish_fragment(&self, fragment: Fragment) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_work(fragment, tx);
        self.do_work().await;
        match rx.await {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(err),
            Err(_) => Err(Error::Internal),
        }
    }

    pub fn garbage_applies_cleanly(&self, garbage: &Garbage) -> bool {
        let latest = {
            let staging = self.staging.lock().unwrap();
            staging.stable.manifest.clone()
        };
        matches!(latest.apply_garbage(garbage.clone()), Ok(Some(_)))
    }

    // Given garbage that has already been written to S3, apply the garbage collection to this
    // manifest.
    pub async fn apply_garbage(&self, garbage: Garbage) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // SAFETY(rescrv):  Mutex poisoning.
        {
            let mut staging = self.staging.lock().unwrap();
            if staging.garbage.is_some() {
                return Err(Error::GarbageCollection(
                    "tried collecting garbage twice".to_string(),
                ));
            }
            staging.garbage = Some((garbage, tx));
        }
        self.do_work().await;
        match rx.await {
            Ok(None) => Ok(()),
            Ok(Some(err)) => {
                tracing::error!("Unable to apply garbage: {err}");
                Err(err)
            }
            Err(err) => {
                tracing::error!(
                    "Unable to receive message for garbage application completion: {err}"
                );
                Err(Error::GarbageCollection(format!(
                    "Unable to receive message for garbage application completion: {err}"
                )))
            }
        }
    }

    async fn do_work(&self) {
        let _guard = self.do_work_mutex.lock().await;
        let mut iters = 0;
        for i in 0..u64::MAX {
            iters = i + 1;
            let work = {
                // SAFETY(rescrv):  Mutex poisoning.
                let mut staging = self.staging.lock().unwrap();
                staging.pull_work(self)
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
                if let Some(snapshot) = snapshot {
                    if let Err(err) = snapshot
                        .install(&self.throttle, &self.storage, &self.prefix)
                        .await
                    {
                        for notifier in notifiers.into_iter() {
                            let _ = notifier.send(Some(err.clone()));
                        }
                        continue;
                    }
                }
                match old_manifest
                    .install(
                        &self.throttle,
                        &self.storage,
                        &self.prefix,
                        Some(&old_e_tag),
                        &new_manifest,
                    )
                    .await
                {
                    Ok(e_tag) => {
                        // SAFETY(rescrv):  Mutex poisoning.
                        {
                            let mut staging = self.staging.lock().unwrap();
                            staging.next_seq_no_to_apply = next_seq_no_to_apply;
                            staging.stable = ManifestAndETag {
                                manifest: new_manifest,
                                e_tag,
                            };
                        }
                        for notifier in notifiers.into_iter() {
                            let _ = notifier.send(None);
                        }
                    }
                    Err(e) => {
                        for notifier in notifiers.into_iter() {
                            let _ = notifier.send(Some(e.clone()));
                        }
                    }
                }
            } else {
                break;
            }
        }
        if iters > 3 {
            tracing::event!(tracing::Level::INFO, name = "do work iterated", iters =? iters);
        }
    }

    pub async fn compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
        cache: &dyn SnapshotCache,
    ) -> Result<Option<Garbage>, Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let stable = {
            let staging = self.staging.lock().unwrap();
            staging.stable.manifest.clone()
        };
        Garbage::new(
            &self.storage,
            &self.prefix,
            &stable,
            &options.throttle,
            cache,
            first_to_keep,
        )
        .await
    }

    pub fn count_waiters(&self) -> usize {
        // SAFETY(rescrv):  Mutex poisoning.
        let staging = self.staging.lock().unwrap();
        staging.fragments.len()
    }

    pub fn debug_dump(&self) -> String {
        let mut output = "[manifest manager]\n".to_string();
        let staging = self.staging.lock().unwrap();
        output += &format!("next_log_position: {:?}\n", staging.next_log_position);
        output += &format!(
            "next_seq_no_to_assign: {:?}\n",
            staging.next_seq_no_to_assign
        );
        output += &format!("next_seq_no_to_apply: {:?}\n", staging.next_seq_no_to_apply);
        output += &format!("last_batch: {:?}\n", staging.last_batch);
        output += &format!("tearing_down: {:?}\n", staging.tearing_down);
        output += &format!("fragments: {}\n", staging.fragments.len());
        output
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chroma_storage::s3_client_for_test_with_new_bucket;

    use crate::*;

    #[tokio::test]
    async fn test_k8s_integration_manager_staging() {
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
        let manager = ManifestManager::new(
            ThrottleOptions::default(),
            SnapshotOptions::default(),
            storage,
            "prefix".to_string(),
            "manager in test".to_string(),
        )
        .await
        .unwrap();
        let (d1_tx, mut d1_rx) = tokio::sync::oneshot::channel();
        manager.push_work(
            Fragment {
                path: "path2".to_string(),
                seq_no: FragmentSeqNo(2),
                num_bytes: 20,
                start: LogPosition::from_offset(22),
                limit: LogPosition::from_offset(42),
                setsum: Setsum::default(),
            },
            d1_tx,
        );
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work(&manager)
        };
        assert!(work.is_none());
        assert!(d1_rx.try_recv().is_err());
        let (d2_tx, mut d2_rx) = tokio::sync::oneshot::channel();
        manager.push_work(
            Fragment {
                path: "path1".to_string(),
                seq_no: FragmentSeqNo(1),
                num_bytes: 30,
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(22),
                setsum: Setsum::default(),
            },
            d2_tx,
        );
        let work = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut staging = manager.staging.lock().unwrap();
            staging.pull_work(&manager).unwrap()
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
                writer: "init in test".to_string(),
                setsum: Setsum::default(),
                collected: Setsum::default(),
                acc_bytes: 0,
                snapshots: vec![],
                fragments: vec![],
                initial_offset: None,
                initial_seq_no: None,
            },
            work.0
        );
        assert_eq!(
            Manifest {
                writer: "manager in test".to_string(),
                setsum: Setsum::default(),
                collected: Setsum::default(),
                acc_bytes: 50,
                snapshots: vec![],
                fragments: vec![
                    Fragment {
                        path: "path1".to_string(),
                        seq_no: FragmentSeqNo(1),
                        num_bytes: 30,
                        start: LogPosition::from_offset(1),
                        limit: LogPosition::from_offset(22),
                        setsum: Setsum::default(),
                    },
                    Fragment {
                        path: "path2".to_string(),
                        seq_no: FragmentSeqNo(2),
                        num_bytes: 20,
                        start: LogPosition::from_offset(22),
                        limit: LogPosition::from_offset(42),
                        setsum: Setsum::default(),
                    }
                ],
                initial_offset: None,
                initial_seq_no: None,
            },
            work.2
        );
        assert_eq!(None, work.4);
    }
}
