use std::sync::{Arc, Mutex};
use std::time::Instant;

use chroma_storage::{ETag, Storage};
use setsum::Setsum;

use crate::manifest::{Manifest, Snapshot};
use crate::reader::read_fragment;
use crate::writer::MarkDirty;
use crate::{
    unprefixed_fragment_path, Error, Fragment, FragmentSeqNo, LogPosition, SnapshotOptions,
    ThrottleOptions,
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
            return None;
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
            return None;
        }
        self.last_batch = Instant::now();
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
    staging: Arc<Mutex<Staging>>,
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
            stable,
            fragments: vec![],
            next_log_position,
            next_seq_no_to_assign,
            next_seq_no_to_apply,
            last_batch: Instant::now(),
        }));
        Ok(Self {
            throttle,
            snapshot,
            storage,
            prefix,
            writer,
            staging,
        })
    }

    /// Recover from a fault in writing.  It is possible that fragments have been written that are
    /// not referenced by the manifest.  Scout ahead until an empty slot is observed.  Then write
    /// the manifest that includes the new fragments.
    pub async fn recover(&self, mark_dirty: &dyn MarkDirty) -> Result<(), Error> {
        loop {
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
                self.publish_fragment(fragment).await?;
            } else {
                break;
            }
        }
        Ok(())
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
        staging.fragments.push((fragment, tx));
    }

    /// Given a fragment, add it to the manifest, batch its application and wait for it to apply.
    #[tracing::instrument(skip(self, fragment))]
    pub async fn publish_fragment(&self, fragment: Fragment) -> Result<(), Error> {
        assert_ne!(fragment.setsum, Setsum::default(), "TODO(rescrv): remove");
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_work(fragment, tx);
        loop {
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
                    snapshot
                        .install(&self.throttle, &self.storage, &self.prefix)
                        .await?;
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
                        let mut staging = self.staging.lock().unwrap();
                        staging.next_seq_no_to_apply = next_seq_no_to_apply;
                        staging.stable = ManifestAndETag {
                            manifest: new_manifest,
                            e_tag,
                        };
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
        match rx.await {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(err),
            Err(_) => Err(Error::Internal),
        }
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
                acc_bytes: 0,
                snapshots: vec![],
                fragments: vec![],
            },
            work.0
        );
        assert_eq!(
            Manifest {
                writer: "manager in test".to_string(),
                setsum: Setsum::default(),
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
            },
            work.2
        );
        assert_eq!(None, work.4);
    }
}
