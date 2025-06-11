use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use setsum::Setsum;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};

use crate::manifest::unprefixed_snapshot_path;
use crate::{
    deserialize_setsum, serialize_setsum, Error, Fragment, LogPosition, Manifest, ScrubError,
    Snapshot, SnapshotCache, SnapshotPointer, ThrottleOptions,
};

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct Garbage {
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub dropped_setsum: Setsum,
    pub actions: Vec<GarbageAction>,
    pub cutoff: LogPosition,
}

impl Garbage {
    #[allow(clippy::result_large_err)]
    pub async fn new(
        storage: &Storage,
        prefix: &str,
        manifest: &Manifest,
        throttle: &ThrottleOptions,
        snapshots: &dyn SnapshotCache,
        first_to_keep: LogPosition,
    ) -> Result<Self, Error> {
        let dropped_snapshots = manifest
            .snapshots
            .iter()
            .filter(|snap| snap.limit <= first_to_keep)
            .collect::<Vec<_>>();
        let replaced_snapshots = manifest
            .snapshots
            .iter()
            .filter(|snap| (snap.start..snap.limit).contains(&first_to_keep))
            .collect::<Vec<_>>();
        let dropped_fragments = manifest
            .fragments
            .iter()
            .filter(|frag| frag.limit <= first_to_keep)
            .collect::<Vec<_>>();
        let mut actions = vec![];
        let mut drop_acc = Setsum::default();
        for snap in dropped_snapshots {
            let (action, setsum) =
                Self::drop_snapshot(storage, prefix, snap, throttle, snapshots).await?;
            actions.push(action);
            drop_acc += setsum;
        }
        for snap in replaced_snapshots {
            let (action, setsum) =
                Self::replace_snapshot(storage, prefix, snap, throttle, snapshots, first_to_keep)
                    .await?;
            actions.push(action);
            drop_acc += setsum;
        }
        for frag in dropped_fragments {
            let (action, setsum) = Self::drop_fragment(frag)?;
            actions.push(action);
            drop_acc += setsum;
        }
        let cutoff = first_to_keep;
        let garbage = Garbage {
            dropped_setsum: drop_acc,
            actions,
            cutoff,
        };
        garbage.scrub()?;
        Ok(garbage)
    }

    pub fn path(prefix: &str) -> String {
        format!("{}/gc/GARBAGE", prefix)
    }

    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<Option<Garbage>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retries = 0;
        let path = Self::path(prefix);
        loop {
            match storage
                .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
                .await
                .map_err(Arc::new)
            {
                Ok((ref garbage, _)) => {
                    let garbage: Garbage = serde_json::from_slice(garbage).map_err(|e| {
                        Error::CorruptGarbage(format!("could not decode JSON garbage: {e:?}"))
                    })?;
                    return Ok(Some(garbage));
                }
                Err(err) => match &*err {
                    StorageError::NotFound { path: _, source: _ } => return Ok(None),
                    err => {
                        let backoff = exp_backoff.next();
                        tokio::time::sleep(backoff).await;
                        if retries >= 3 {
                            return Err(Error::StorageError(Arc::new(err.clone())));
                        }
                        retries += 1;
                    }
                },
            }
        }
    }

    #[tracing::instrument(skip(self, storage), err(Display))]
    pub async fn install(
        &self,
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<(), Error> {
        self.install_new_snapshots(storage, prefix, options).await?;
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        loop {
            let path = Self::path(prefix);
            let payload = serde_json::to_string(&self)
                .map_err(|e| {
                    Error::CorruptManifest(format!("could not encode JSON garbage: {e:?}"))
                })?
                .into_bytes();
            let options = PutOptions::if_not_exists(StorageRequestPriority::P0);
            match storage.put_bytes(&path, payload, options).await {
                Ok(_) => return Ok(()),
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    // NOTE(rescrv):  We know that someone put the file before us, and therefore we
                    // know this write failed.  Because the garbage file is created and deleted
                    // we cannot just overwrite, so fail with log contention and let higher level
                    // protocol decide.
                    return Err(Error::LogContention);
                }
                Err(e) => {
                    tracing::error!("error uploading manifest: {e:?}");
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, Error> {
        let to_drop = self
            .actions
            .iter()
            .map(|x| x.scrub())
            .collect::<Result<Vec<_>, Error>>()?;
        let dropped_setsum = to_drop
            .clone()
            .into_iter()
            .fold(Setsum::default(), Setsum::add);
        if dropped_setsum != self.dropped_setsum {
            return Err(Box::new(ScrubError::CorruptGarbage {
                expected_setsum: self.dropped_setsum,
                returned_setsum: dropped_setsum,
            })
            .into());
        }
        Ok(dropped_setsum)
    }

    #[allow(clippy::result_large_err)]
    fn drop_fragment(frag: &Fragment) -> Result<(GarbageAction, Setsum), Error> {
        eprintln!("DROP_FRAGMENT({frag:?})");
        Ok((
            GarbageAction::DropFragment(DropFragment {
                path_to_fragment: frag.path.clone(),
                fragment_setsum: frag.setsum,
            }),
            frag.setsum,
        ))
    }

    #[allow(clippy::result_large_err)]
    async fn drop_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshots: &dyn SnapshotCache,
    ) -> Result<(GarbageAction, Setsum), Error> {
        eprintln!("DROP_SNAPSHOT({ptr:?})");
        let snapshot = match snapshots.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match Snapshot::load(throttle, storage, prefix, ptr).await? {
                Some(snapshot) => snapshot,
                None => {
                    return Err(Box::new(ScrubError::MissingSnapshot {
                        reference: ptr.clone(),
                    })
                    .into())
                }
            },
        };
        let mut drop_acc = Setsum::default();
        let mut children = vec![];
        // NOTE(rescrv):  Because of our tree structure, no snapshot will have two parents.  This
        // is critical because it means we can just drop all our children.  The setsum of the
        // snapshot includes everything dropped, so we don't need to drop individually.  For that
        // reason, provide a dummy drop_acc;
        for frag in snapshot.fragments.iter() {
            let (action, setsum) = Self::drop_fragment(frag)?;
            children.push(action);
            drop_acc += setsum;
        }
        for snap in snapshot.snapshots.iter() {
            let (action, setsum) = Box::pin(Self::drop_snapshot(
                storage, prefix, snap, throttle, snapshots,
            ))
            .await?;
            children.push(action);
            drop_acc += setsum;
        }
        if drop_acc != snapshot.setsum {
            return Err(Box::new(ScrubError::CorruptSnapshotDrop {
                lhs: drop_acc,
                rhs: snapshot.setsum,
            })
            .into());
        }
        Ok((
            GarbageAction::DropSnapshot(DropSnapshot {
                path_to_snapshot: snapshot.path.clone(),
                snapshot_setsum: snapshot.setsum,
                children,
            }),
            drop_acc,
        ))
    }

    #[allow(clippy::result_large_err)]
    async fn replace_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
        first_to_keep: LogPosition,
    ) -> Result<(GarbageAction, Setsum), Error> {
        eprintln!("REPLACE_SNAPSHOT({ptr:?})");
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match Snapshot::load(throttle, storage, prefix, ptr).await? {
                Some(snapshot) => snapshot,
                None => {
                    return Err(Box::new(ScrubError::MissingSnapshot {
                        reference: ptr.clone(),
                    })
                    .into())
                }
            },
        };
        let mut fragments_to_drop = vec![];
        let mut fragments_to_keep = vec![];
        let mut snapshots_to_drop = vec![];
        let mut snapshots_to_keep = vec![];
        let mut snapshots_to_split = vec![];
        for frag in snapshot.fragments.iter() {
            if frag.limit <= first_to_keep {
                fragments_to_drop.push(frag);
            } else {
                fragments_to_keep.push(frag.clone());
            }
        }
        for snap in snapshot.snapshots.iter() {
            if snap.limit <= first_to_keep {
                snapshots_to_drop.push(snap);
            } else if (snap.start..snap.limit).contains(&first_to_keep) {
                snapshots_to_split.push(snap.clone());
            } else {
                snapshots_to_keep.push(snap.clone());
            }
        }
        if fragments_to_drop.len() + fragments_to_keep.len() != snapshot.fragments.len() {
            return Err(
                Box::new(ScrubError::Internal("fragments don't balance".to_string())).into(),
            );
        }
        if snapshots_to_drop.len() + snapshots_to_split.len() + snapshots_to_keep.len()
            != snapshot.snapshots.len()
            || snapshots_to_split.len() > 1
        {
            return Err(
                Box::new(ScrubError::Internal("snapshots don't balance".to_string())).into(),
            );
        }
        if !fragments_to_drop.is_empty() && !snapshots_to_split.is_empty() {
            return Err(Box::new(ScrubError::Internal(
                "invalid state:  dropping fragments and splitting a snapshot".to_string(),
            ))
            .into());
        }
        let mut drop_acc = Setsum::default();
        if let Some(to_split) = snapshots_to_split.pop() {
            let snapshots = snapshots_to_keep;
            let fragments = fragments_to_keep;
            let (action, setsum) = Box::pin(Self::replace_snapshot(
                storage,
                prefix,
                &to_split,
                throttle,
                snapshot_cache,
                first_to_keep,
            ))
            .await?;
            let GarbageAction::ReplaceSnapshot(mut replace) = action else {
                return Err(Box::new(ScrubError::Internal(
                    "replace snapshot failed to generate a replace snapshot".to_string(),
                ))
                .into());
            };
            drop_acc += setsum;
            replace.new_snapshot.setsum -= setsum;
            replace.new_snapshot.path = unprefixed_snapshot_path(replace.new_snapshot.setsum);
            replace.new_snapshot.snapshots.extend(snapshots);
            replace.new_snapshot.fragments.extend(fragments);
            replace.new_snapshot.snapshots.sort_by_key(|s| s.start);
            replace.new_snapshot.fragments.sort_by_key(|f| f.start);
            for snap in snapshots_to_drop.iter() {
                let (action, setsum) =
                    Self::drop_snapshot(storage, prefix, snap, throttle, snapshot_cache).await?;
                replace.actions.push(action);
                drop_acc += setsum;
                replace.new_snapshot.setsum -= setsum;
            }
            for frag in fragments_to_drop.iter() {
                let (action, setsum) = Self::drop_fragment(frag)?;
                replace.actions.push(action);
                drop_acc += setsum;
                replace.new_snapshot.setsum -= setsum;
            }
            replace.new_snapshot.setsum = replace
                .new_snapshot
                .snapshots
                .iter()
                .map(|s| s.setsum)
                .fold(Setsum::default(), Setsum::add)
                + replace
                    .new_snapshot
                    .fragments
                    .iter()
                    .map(|f| f.setsum)
                    .fold(Setsum::default(), Setsum::add);
            let dropped = replace.scrub()?;
            eprintln!("dropped {}", dropped.hexdigest());
            eprintln!("drop_acc {}", drop_acc.hexdigest());
            eprintln!("new_snap {}", replace.new_snapshot.setsum.hexdigest());
            eprintln!(
                "new_snap-input {}",
                (replace.new_snapshot.setsum - ptr.setsum).hexdigest()
            );
            eprintln!(
                "input-new_snap {}",
                (ptr.setsum - replace.new_snapshot.setsum).hexdigest()
            );
            if replace.new_snapshot.setsum + drop_acc == ptr.setsum {
                eprintln!("SUCCESS");
                Ok((GarbageAction::ReplaceSnapshot(replace), drop_acc))
            } else {
                Err(Box::new(ScrubError::CorruptSnapshotReplace {
                    old_snapshot_setsum: ptr.setsum,
                    new_snapshot_setsum: replace.new_snapshot.setsum,
                    dropped: drop_acc,
                })
                .into())
            }
        } else {
            let snapshots = snapshots_to_keep;
            let fragments = fragments_to_keep;
            if fragments.is_empty() {
                return Err(Box::new(ScrubError::Internal(
                    "invalid state:  no fragments".to_string(),
                ))
                .into());
            }
            let depth = snapshots.iter().map(|s| s.depth).max().unwrap_or(0) + 1;
            let setsum = snapshots
                .iter()
                .map(|s| s.setsum)
                .fold(Setsum::default(), Setsum::add)
                + fragments
                    .iter()
                    .map(|f| f.setsum)
                    .fold(Setsum::default(), Setsum::add);
            let new_snapshot = Snapshot {
                path: unprefixed_snapshot_path(setsum),
                setsum,
                depth,
                snapshots,
                fragments,
                writer: "garbage collection".to_string(),
            };
            for f in fragments_to_drop.iter() {
                drop_acc += f.setsum;
            }
            for s in snapshots_to_drop.iter() {
                drop_acc += s.setsum;
            }
            let drop_snapshots = snapshots_to_drop
                .iter()
                .map(|s| DropSnapshotShallow {
                    path_to_snapshot: s.path_to_snapshot.clone(),
                    snapshot_setsum: s.setsum,
                })
                .collect::<Vec<_>>();
            let drop_fragments = fragments_to_drop
                .iter()
                .map(|f| DropFragment {
                    path_to_fragment: f.path.clone(),
                    fragment_setsum: f.setsum,
                })
                .collect::<Vec<_>>();
            if new_snapshot.setsum + drop_acc != ptr.setsum {
                return Err(Box::new(ScrubError::Internal(
                    "failed to replace snapshot:  setsum doesn't balance".to_string(),
                ))
                .into());
            }
            Ok((
                GarbageAction::ReplaceSnapshot(ReplaceSnapshot {
                    actions: vec![],
                    drop_snapshots,
                    drop_fragments,
                    new_snapshot,
                }),
                drop_acc,
            ))
        }
    }

    pub fn prefixed_paths(&self, prefix: &str) -> impl Iterator<Item = String> {
        fn prefixed_paths_for_action(
            prefix: &str,
            action: &GarbageAction,
        ) -> impl Iterator<Item = String> {
            let mut paths = vec![];
            match action {
                GarbageAction::DropFragment(DropFragment {
                    path_to_fragment,
                    fragment_setsum: _,
                }) => paths.push(format!("{prefix}/{path_to_fragment}")),
                GarbageAction::DropSnapshot(DropSnapshot {
                    path_to_snapshot,
                    snapshot_setsum: _,
                    children,
                }) => {
                    paths.push(format!("{prefix}/{path_to_snapshot}"));
                    for child in children {
                        paths.extend(prefixed_paths_for_action(prefix, child));
                    }
                }
                GarbageAction::ReplaceSnapshot(ReplaceSnapshot {
                    actions,
                    drop_snapshots,
                    drop_fragments,
                    new_snapshot: _,
                }) => {
                    for action in actions {
                        paths.extend(prefixed_paths_for_action(prefix, action));
                    }
                    for drop_snapshot in drop_snapshots.iter() {
                        paths.push(format!("{prefix}/{}", drop_snapshot.path_to_snapshot));
                    }
                    for drop_fragment in drop_fragments.iter() {
                        paths.push(format!("{prefix}/{}", drop_fragment.path_to_fragment));
                    }
                }
            };
            paths.into_iter()
        }
        let mut paths = vec![];
        for action in self.actions.iter() {
            paths.extend(prefixed_paths_for_action(prefix, action));
        }
        paths.into_iter()
    }

    pub async fn install_new_snapshots(
        &self,
        storage: &Storage,
        prefix: &str,
        throttle: &ThrottleOptions,
    ) -> Result<(), Error> {
        for action in self.actions.iter() {
            self.install_new_snapshots_from_action(storage, prefix, throttle, action)
                .await?;
        }
        Ok(())
    }

    async fn install_new_snapshots_from_action(
        &self,
        storage: &Storage,
        prefix: &str,
        throttle: &ThrottleOptions,
        action: &GarbageAction,
    ) -> Result<(), Error> {
        match action {
            GarbageAction::DropSnapshot { .. } | GarbageAction::DropFragment { .. } => {
                // NOTE(rescrv):  Because each snapshot and fragment has exactly one path from the
                // root, there is no need to process any of this snapshot's snapshots or fragments;
                // they are all dropped.
                Ok(())
            }
            GarbageAction::ReplaceSnapshot(replace) => {
                replace
                    .new_snapshot
                    .install(throttle, storage, prefix)
                    .await?;
                Ok(())
            }
        }
    }
}

/////////////////////////////////////////// GarbageAction //////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DropFragment {
    pub path_to_fragment: String,
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub fragment_setsum: Setsum,
}

impl DropFragment {
    pub fn scrub(&self) -> Result<Setsum, Error> {
        Ok(self.fragment_setsum)
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DropSnapshot {
    pub path_to_snapshot: String,
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub snapshot_setsum: Setsum,
    pub children: Vec<GarbageAction>,
}

impl DropSnapshot {
    pub fn scrub(&self) -> Result<Setsum, Error> {
        let to_drop = self
            .children
            .iter()
            .map(|x| x.scrub())
            .collect::<Result<Vec<_>, Error>>()?;
        let dropped_setsum = to_drop.into_iter().fold(Setsum::default(), Setsum::add);
        if dropped_setsum != self.snapshot_setsum {
            return Err(Box::new(ScrubError::CorruptGarbage {
                expected_setsum: self.snapshot_setsum,
                returned_setsum: dropped_setsum,
            })
            .into());
        }
        Ok(dropped_setsum)
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DropSnapshotShallow {
    pub path_to_snapshot: String,
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub snapshot_setsum: Setsum,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ReplaceSnapshot {
    pub actions: Vec<GarbageAction>,
    pub drop_snapshots: Vec<DropSnapshotShallow>,
    pub drop_fragments: Vec<DropFragment>,
    pub new_snapshot: Snapshot,
}

impl ReplaceSnapshot {
    pub fn scrub(&self) -> Result<Setsum, Error> {
        let actions = self
            .actions
            .iter()
            .map(|a| a.scrub())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(actions.into_iter().fold(Setsum::default(), Setsum::add)
            + self
                .drop_snapshots
                .iter()
                .map(|s| s.snapshot_setsum)
                .fold(Setsum::default(), Setsum::add)
            + self
                .drop_fragments
                .iter()
                .map(|f| f.fragment_setsum)
                .fold(Setsum::default(), Setsum::add))
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GarbageAction {
    DropSnapshot(DropSnapshot),
    DropFragment(DropFragment),
    ReplaceSnapshot(ReplaceSnapshot),
}

impl GarbageAction {
    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, Error> {
        match self {
            Self::DropFragment(f) => f.scrub(),
            Self::DropSnapshot(s) => s.scrub(),
            Self::ReplaceSnapshot(r) => r.scrub(),
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use setsum::Setsum;

    use chroma_storage::s3_client_for_test_with_new_bucket;

    use super::*;
    use crate::{FragmentSeqNo, LogPosition, SnapshotPointer};

    // Mock implementations for testing
    #[derive(Default)]
    struct MockSnapshotCache {
        snapshots: Mutex<Vec<Snapshot>>,
    }

    #[async_trait::async_trait]
    impl SnapshotCache for MockSnapshotCache {
        async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
            let snapshots = self.snapshots.lock().unwrap();
            Ok(snapshots
                .iter()
                .find(|s| s.setsum == ptr.setsum && s.path == ptr.path_to_snapshot)
                .cloned())
        }

        async fn put(&self, _: &SnapshotPointer, snap: &Snapshot) -> Result<(), Error> {
            let mut snapshots = self.snapshots.lock().unwrap();
            snapshots.push(snap.clone());
            Ok(())
        }
    }

    /// Test helper to create a fragment
    fn create_fragment(start: u64, limit: u64, seq_no: FragmentSeqNo, setsum: Setsum) -> Fragment {
        Fragment {
            start: LogPosition::from_offset(start),
            limit: LogPosition::from_offset(limit),
            path: format!("fragment_{start}_{limit}"),
            setsum,
            seq_no,
            num_bytes: 42,
        }
    }

    /// Test helper to create a snapshot with nested snapshots to trigger to_split case
    fn create_snapshot_for_split_test() -> (SnapshotPointer, Snapshot, MockSnapshotCache) {
        let cache = MockSnapshotCache::default();
        let overall_setsum = Setsum::from_hexdigest(
            "00000000aaaaaaaabbbbbbbb0000000000000000000000000000000000000000",
        )
        .unwrap();
        let nested_snapshot = Snapshot {
            path: unprefixed_snapshot_path(overall_setsum),
            setsum: overall_setsum,
            depth: 1,
            snapshots: vec![],
            fragments: vec![
                create_fragment(
                    5,
                    8,
                    FragmentSeqNo(1),
                    Setsum::from_hexdigest(
                        "00000000aaaaaaaa000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                ),
                create_fragment(
                    8,
                    15,
                    FragmentSeqNo(2),
                    Setsum::from_hexdigest(
                        "0000000000000000bbbbbbbb0000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                ),
            ],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(nested_snapshot.clone());
        (nested_snapshot.to_pointer(), nested_snapshot, cache)
    }

    #[tokio::test]
    async fn replace_snapshot_triggers_to_split_case_one_level() {
        // Set up test data that will trigger the to_split case
        let (parent_ptr, _parent_snapshot, cache) = create_snapshot_for_split_test();

        // Set cutoff at position 10, which should trigger splitting the nested snapshot
        // that spans from 8 to 15
        let first_to_keep = LogPosition::from_offset(10);

        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

        // This should trigger the to_split case in replace_snapshot
        let (action, setsum) = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        let GarbageAction::ReplaceSnapshot(replace) = action else {
            panic!("did not get a replace snapshot");
        };

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            setsum
        );
        assert!(replace.drop_snapshots.is_empty());
        assert_eq!(
            Snapshot {
                path: "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                    .to_string(),
                setsum: Setsum::from_hexdigest(
                    "0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                )
                .unwrap(),
                depth: 1,
                snapshots: vec![],
                fragments: vec![create_fragment(
                    8,
                    15,
                    FragmentSeqNo(2),
                    Setsum::from_hexdigest(
                        "0000000000000000bbbbbbbb0000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                ),],
                writer: "garbage collection".to_string(),
            },
            replace.new_snapshot
        );
    }

    fn create_nested_snapshot_for_split_test(
        depth: usize,
    ) -> (SnapshotPointer, Snapshot, MockSnapshotCache) {
        if depth == 0 {
            create_snapshot_for_split_test()
        } else {
            let (ptr, snap, cache) = create_nested_snapshot_for_split_test(depth.saturating_sub(1));
            let parent_snapshot = Snapshot {
                path: unprefixed_snapshot_path(snap.setsum),
                setsum: snap.setsum,
                depth: snap.depth + 1,
                snapshots: vec![ptr],
                fragments: vec![],
                writer: "test".to_string(),
            };
            cache
                .snapshots
                .lock()
                .unwrap()
                .push(parent_snapshot.clone());
            (parent_snapshot.to_pointer(), parent_snapshot, cache)
        }
    }

    #[tokio::test]
    async fn replace_snapshot_triggers_to_split_case_two_level() {
        // Set up test data that will trigger the to_split case
        let (parent_ptr, _parent_snapshot, cache) = create_nested_snapshot_for_split_test(1);

        // Set cutoff at position 10, which should trigger splitting the nested snapshot
        // that spans from 8 to 15
        let first_to_keep = LogPosition::from_offset(10);

        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

        // This should trigger the to_split case in replace_snapshot
        let (action, setsum) = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        let GarbageAction::ReplaceSnapshot(replace) = action else {
            panic!("did not get a replace snapshot");
        };

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            setsum
        );
        assert!(replace.drop_snapshots.is_empty());
        assert_eq!(
            Snapshot {
                path: "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                    .to_string(),
                setsum: Setsum::from_hexdigest(
                    "0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                )
                .unwrap(),
                depth: 1,
                snapshots: vec![],
                fragments: vec![create_fragment(
                    8,
                    15,
                    FragmentSeqNo(2),
                    Setsum::from_hexdigest(
                        "0000000000000000bbbbbbbb0000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                ),],
                writer: "garbage collection".to_string(),
            },
            replace.new_snapshot
        );
    }

    #[tokio::test]
    async fn replace_snapshot_triggers_to_split_case_three_level() {
        // Set up test data that will trigger the to_split case
        let (parent_ptr, _parent_snapshot, cache) = create_nested_snapshot_for_split_test(2);

        // Set cutoff at position 10, which should trigger splitting the nested snapshot
        // that spans from 8 to 15
        let first_to_keep = LogPosition::from_offset(10);

        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

        // This should trigger the to_split case in replace_snapshot
        let (action, setsum) = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        let GarbageAction::ReplaceSnapshot(replace) = action else {
            panic!("did not get a replace snapshot");
        };

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            setsum
        );
        assert!(replace.drop_snapshots.is_empty());
        assert_eq!(
            Snapshot {
                path: "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                    .to_string(),
                setsum: Setsum::from_hexdigest(
                    "0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
                )
                .unwrap(),
                depth: 1,
                snapshots: vec![],
                fragments: vec![create_fragment(
                    8,
                    15,
                    FragmentSeqNo(2),
                    Setsum::from_hexdigest(
                        "0000000000000000bbbbbbbb0000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                ),],
                writer: "garbage collection".to_string(),
            },
            replace.new_snapshot
        );
    }

    #[test]
    fn drop_frag() {
        let setsum = Setsum::from_hexdigest(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let fragment = create_fragment(10, 20, FragmentSeqNo(1), setsum);

        let (action, returned_setsum) = Garbage::drop_fragment(&fragment).unwrap();

        // Should return the same setsum
        assert_eq!(returned_setsum, setsum);

        // Should create a DropFragment action
        let GarbageAction::DropFragment(drop_frag) = action else {
            panic!("Expected DropFragment action");
        };

        assert_eq!(drop_frag.path_to_fragment, fragment.path);
        assert_eq!(drop_frag.fragment_setsum, fragment.setsum);

        // Test scrub on the created action
        assert_eq!(drop_frag.scrub().unwrap(), setsum);
    }

    #[tokio::test]
    async fn drop_snapshot() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let cache = MockSnapshotCache::default();

        // Create a snapshot with nested snapshots and fragments
        let frag1_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let frag2_setsum = Setsum::from_hexdigest(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let total_setsum = frag1_setsum + frag2_setsum;

        let fragment1 = create_fragment(10, 20, FragmentSeqNo(1), frag1_setsum);
        let fragment2 = create_fragment(20, 30, FragmentSeqNo(2), frag2_setsum);

        // Create nested snapshot with fragment1
        let nested_snapshot = Snapshot {
            path: unprefixed_snapshot_path(frag1_setsum),
            setsum: frag1_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment1.clone()],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(nested_snapshot.clone());

        // Create main snapshot with fragment2
        let main_snapshot = Snapshot {
            path: unprefixed_snapshot_path(total_setsum),
            setsum: total_setsum,
            depth: 1,
            snapshots: vec![nested_snapshot.to_pointer()],
            fragments: vec![fragment2.clone()],
            writer: "test".to_string(),
        };
        cache.snapshots.lock().unwrap().push(main_snapshot.clone());

        let snapshot_ptr = main_snapshot.to_pointer();

        let (action, returned_setsum) = Garbage::drop_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
        )
        .await
        .unwrap();

        // Should return the total setsum
        assert_eq!(returned_setsum, total_setsum);

        // Should create a DropSnapshot action
        let GarbageAction::DropSnapshot(drop_snapshot) = action else {
            panic!("Expected DropSnapshot action");
        };

        assert_eq!(drop_snapshot.path_to_snapshot, main_snapshot.path);
        assert_eq!(drop_snapshot.snapshot_setsum, main_snapshot.setsum);
        assert_eq!(drop_snapshot.children.len(), 2); // 1 fragment + 1 nested snapshot

        // Test scrub on the created action
        assert_eq!(drop_snapshot.scrub().unwrap(), total_setsum);
    }

    #[tokio::test]
    async fn replace_snapshot() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let cache = MockSnapshotCache::default();

        // Create fragments with different ranges
        let frag1_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let frag2_setsum = Setsum::from_hexdigest(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let frag3_setsum = Setsum::from_hexdigest(
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();

        let fragment1 = create_fragment(5, 10, FragmentSeqNo(1), frag1_setsum); // Will be dropped
        let fragment2 = create_fragment(10, 20, FragmentSeqNo(2), frag2_setsum); // Will be kept
        let fragment3 = create_fragment(20, 30, FragmentSeqNo(3), frag3_setsum); // Will be kept

        let total_setsum = frag1_setsum + frag2_setsum + frag3_setsum;

        // Create snapshot that spans across the cutoff point
        let snapshot = Snapshot {
            path: unprefixed_snapshot_path(total_setsum),
            setsum: total_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone(), fragment3.clone()],
            writer: "test".to_string(),
        };
        cache.snapshots.lock().unwrap().push(snapshot.clone());

        let snapshot_ptr = snapshot.to_pointer();
        let first_to_keep = LogPosition::from_offset(10); // Keep fragments starting from offset 10

        let (action, returned_setsum) = Garbage::replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        // Should return the setsum of the dropped fragment
        assert_eq!(returned_setsum, frag1_setsum);

        // Should create a ReplaceSnapshot action
        let GarbageAction::ReplaceSnapshot(replace_snapshot) = action else {
            panic!("Expected ReplaceSnapshot action");
        };

        // New snapshot should contain only the kept fragments
        assert_eq!(replace_snapshot.new_snapshot.fragments.len(), 2);
        assert_eq!(
            replace_snapshot.new_snapshot.setsum,
            frag2_setsum + frag3_setsum
        );

        // Should have dropped fragments
        assert_eq!(replace_snapshot.drop_fragments.len(), 1);
        assert_eq!(
            replace_snapshot.drop_fragments[0].fragment_setsum,
            frag1_setsum
        );

        // Test scrub on the created action
        assert_eq!(replace_snapshot.scrub().unwrap(), frag1_setsum);
    }

    #[tokio::test]
    async fn replace_snapshot_drops_snapshots_prior_to_cutoff() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let cache = MockSnapshotCache::default();

        // Create two child snapshots: one before cutoff (to be dropped), one after (to be kept)
        let frag1_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let frag2_setsum = Setsum::from_hexdigest(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let frag3_setsum = Setsum::from_hexdigest(
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();

        let fragment1 = create_fragment(5, 10, FragmentSeqNo(1), frag1_setsum);
        let fragment2 = create_fragment(15, 20, FragmentSeqNo(2), frag2_setsum);
        let fragment3 = create_fragment(25, 30, FragmentSeqNo(3), frag3_setsum); // Additional fragment for parent

        // Child snapshot before cutoff (will be dropped)
        let child_snapshot1 = Snapshot {
            path: unprefixed_snapshot_path(frag1_setsum),
            setsum: frag1_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment1.clone()],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(child_snapshot1.clone());

        // Child snapshot after cutoff (will be kept)
        let child_snapshot2 = Snapshot {
            path: unprefixed_snapshot_path(frag2_setsum),
            setsum: frag2_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment2.clone()],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(child_snapshot2.clone());

        let total_setsum = frag1_setsum + frag2_setsum + frag3_setsum;

        // Parent snapshot containing both child snapshots and an additional fragment
        let parent_snapshot = Snapshot {
            path: unprefixed_snapshot_path(total_setsum),
            setsum: total_setsum,
            depth: 1,
            snapshots: vec![child_snapshot1.to_pointer(), child_snapshot2.to_pointer()],
            fragments: vec![fragment3.clone()],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(parent_snapshot.clone());

        let snapshot_ptr = parent_snapshot.to_pointer();
        let first_to_keep = LogPosition::from_offset(12); // Keep snapshots starting from offset 12

        let (action, returned_setsum) = Garbage::replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        // Should return the setsum of the dropped snapshot
        assert_eq!(returned_setsum, frag1_setsum);

        // Should create a ReplaceSnapshot action
        let GarbageAction::ReplaceSnapshot(replace_snapshot) = action else {
            panic!("Expected ReplaceSnapshot action");
        };

        // New snapshot should contain the kept child snapshot and parent fragment
        assert_eq!(replace_snapshot.new_snapshot.snapshots.len(), 1);
        assert_eq!(replace_snapshot.new_snapshot.fragments.len(), 1);
        assert_eq!(
            replace_snapshot.new_snapshot.snapshots[0].setsum,
            frag2_setsum
        );
        assert_eq!(
            replace_snapshot.new_snapshot.setsum,
            frag2_setsum + frag3_setsum
        );

        // Should have dropped snapshots
        assert_eq!(replace_snapshot.drop_snapshots.len(), 1);
        assert_eq!(
            replace_snapshot.drop_snapshots[0].snapshot_setsum,
            frag1_setsum
        );

        // Test scrub on the created action
        assert_eq!(replace_snapshot.scrub().unwrap(), frag1_setsum);
    }

    #[tokio::test]
    async fn replace_snapshot_drops_fragments_prior_to_cutoff() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let cache = MockSnapshotCache::default();

        // Create fragments: some before cutoff (to be dropped), some after (to be kept)
        let frag1_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let frag2_setsum = Setsum::from_hexdigest(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let frag3_setsum = Setsum::from_hexdigest(
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();

        let fragment1 = create_fragment(5, 8, FragmentSeqNo(1), frag1_setsum); // Will be dropped
        let fragment2 = create_fragment(8, 10, FragmentSeqNo(2), frag2_setsum); // Will be dropped
        let fragment3 = create_fragment(15, 20, FragmentSeqNo(3), frag3_setsum); // Will be kept

        let total_setsum = frag1_setsum + frag2_setsum + frag3_setsum;

        // Snapshot containing fragments that span across the cutoff
        let snapshot = Snapshot {
            path: unprefixed_snapshot_path(total_setsum),
            setsum: total_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone(), fragment3.clone()],
            writer: "test".to_string(),
        };
        cache.snapshots.lock().unwrap().push(snapshot.clone());

        let snapshot_ptr = snapshot.to_pointer();
        let first_to_keep = LogPosition::from_offset(12); // Keep fragments starting from offset 12

        let (action, returned_setsum) = Garbage::replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        // Should return the setsum of the dropped fragments
        assert_eq!(returned_setsum, frag1_setsum + frag2_setsum);

        // Should create a ReplaceSnapshot action
        let GarbageAction::ReplaceSnapshot(replace_snapshot) = action else {
            panic!("Expected ReplaceSnapshot action");
        };

        // New snapshot should contain only the kept fragment
        assert_eq!(replace_snapshot.new_snapshot.fragments.len(), 1);
        assert_eq!(
            replace_snapshot.new_snapshot.fragments[0].setsum,
            frag3_setsum
        );
        assert_eq!(replace_snapshot.new_snapshot.setsum, frag3_setsum);

        // Should have dropped fragments
        assert_eq!(replace_snapshot.drop_fragments.len(), 2);
        let dropped_fragment_setsums: Vec<_> = replace_snapshot
            .drop_fragments
            .iter()
            .map(|f| f.fragment_setsum)
            .collect();
        assert!(dropped_fragment_setsums.contains(&frag1_setsum));
        assert!(dropped_fragment_setsums.contains(&frag2_setsum));

        // Test scrub on the created action
        assert_eq!(
            replace_snapshot.scrub().unwrap(),
            frag1_setsum + frag2_setsum
        );
    }

    #[tokio::test]
    async fn replace_snapshot_two_levels_rightmost_leaf() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let cache = MockSnapshotCache::default();

        // Create fragments for leaf snapshots
        let frag1_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let frag2_setsum = Setsum::from_hexdigest(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let frag3_setsum = Setsum::from_hexdigest(
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();

        let fragment1 = create_fragment(5, 10, FragmentSeqNo(1), frag1_setsum);
        let fragment2 = create_fragment(15, 20, FragmentSeqNo(2), frag2_setsum);
        let fragment3 = create_fragment(25, 30, FragmentSeqNo(3), frag3_setsum); // Additional fragment for interior

        // Left leaf snapshot (will be dropped entirely)
        let left_leaf = Snapshot {
            path: unprefixed_snapshot_path(frag1_setsum),
            setsum: frag1_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment1.clone()],
            writer: "test".to_string(),
        };
        cache.snapshots.lock().unwrap().push(left_leaf.clone());

        // Right leaf snapshot (will be kept)
        let right_leaf = Snapshot {
            path: unprefixed_snapshot_path(frag2_setsum),
            setsum: frag2_setsum,
            depth: 0,
            snapshots: vec![],
            fragments: vec![fragment2.clone()],
            writer: "test".to_string(),
        };
        cache.snapshots.lock().unwrap().push(right_leaf.clone());

        let total_setsum = frag1_setsum + frag2_setsum + frag3_setsum;

        // Interior node containing both leaf snapshots (right-most is the one we keep) and an additional fragment
        let interior_snapshot = Snapshot {
            path: unprefixed_snapshot_path(total_setsum),
            setsum: total_setsum,
            depth: 1,
            snapshots: vec![left_leaf.to_pointer(), right_leaf.to_pointer()],
            fragments: vec![fragment3.clone()],
            writer: "test".to_string(),
        };
        cache
            .snapshots
            .lock()
            .unwrap()
            .push(interior_snapshot.clone());

        let snapshot_ptr = interior_snapshot.to_pointer();
        let first_to_keep = LogPosition::from_offset(12); // Keep snapshots starting from offset 12

        let (action, returned_setsum) = Garbage::replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        // Should return the setsum of the dropped left leaf
        assert_eq!(returned_setsum, frag1_setsum);

        // Should create a ReplaceSnapshot action
        let GarbageAction::ReplaceSnapshot(replace_snapshot) = action else {
            panic!("Expected ReplaceSnapshot action");
        };

        // New snapshot should contain the right-most (kept) leaf snapshot and interior fragment
        assert_eq!(replace_snapshot.new_snapshot.snapshots.len(), 1);
        assert_eq!(replace_snapshot.new_snapshot.fragments.len(), 1);
        assert_eq!(
            replace_snapshot.new_snapshot.snapshots[0].setsum,
            frag2_setsum
        );
        assert_eq!(
            replace_snapshot.new_snapshot.setsum,
            frag2_setsum + frag3_setsum
        );

        // Should have dropped the left snapshot
        assert_eq!(replace_snapshot.drop_snapshots.len(), 1);
        assert_eq!(
            replace_snapshot.drop_snapshots[0].snapshot_setsum,
            frag1_setsum
        );

        // Test scrub on the created action
        assert_eq!(replace_snapshot.scrub().unwrap(), frag1_setsum);
    }
}
