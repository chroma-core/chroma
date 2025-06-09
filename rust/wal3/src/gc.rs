use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use setsum::Setsum;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};

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
        for frag in dropped_fragments {
            actions.push(Self::drop_fragment(frag, &mut drop_acc)?);
        }
        for snap in dropped_snapshots {
            actions.push(
                Self::drop_snapshot(storage, prefix, snap, throttle, snapshots, &mut drop_acc)
                    .await?,
            );
        }
        for snap in replaced_snapshots {
            actions.push(
                Self::replace_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshots,
                    first_to_keep,
                    &mut drop_acc,
                )
                .await?,
            );
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
        scrub(&self.actions, self.dropped_setsum)
    }

    #[allow(clippy::result_large_err)]
    fn drop_fragment(frag: &Fragment, drop_acc: &mut Setsum) -> Result<GarbageAction, Error> {
        *drop_acc += frag.setsum;
        Ok(GarbageAction::DropFragment {
            path_to_fragment: frag.path.clone(),
            fragment_setsum: frag.setsum,
        })
    }

    #[allow(clippy::result_large_err)]
    async fn drop_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshots: &dyn SnapshotCache,
        drop_acc: &mut Setsum,
    ) -> Result<GarbageAction, Error> {
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
        *drop_acc += snapshot.setsum;
        let mut children = vec![];
        // NOTE(rescrv):  Because of our tree structure, no snapshot will have two parents.  This
        // is critical because it means we can just drop all our children.  The setsum of the
        // snapshot includes everything dropped, so we don't need to drop individually.  For that
        // reason, provide a dummy drop_acc;
        for frag in snapshot.fragments.iter() {
            children.push(Self::drop_fragment(frag, &mut Setsum::default())?);
        }
        for snap in snapshot.snapshots.iter() {
            children.push(
                Box::pin(Self::drop_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshots,
                    &mut Setsum::default(),
                ))
                .await?,
            );
        }
        Ok(GarbageAction::DropSnapshot {
            path_to_snapshot: snapshot.path.clone(),
            snapshot_setsum: snapshot.setsum,
            children,
        })
    }

    #[allow(clippy::result_large_err)]
    async fn replace_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshots: &dyn SnapshotCache,
        first_to_keep: LogPosition,
        drop_acc: &mut Setsum,
    ) -> Result<GarbageAction, Error> {
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
        let mut ret_snapshot = snapshot.clone();
        let mut ret_children = vec![];
        for frag in std::mem::take(&mut ret_snapshot.fragments).into_iter() {
            if frag.limit <= first_to_keep {
                ret_children.push(Self::drop_fragment(&frag, drop_acc)?);
                ret_snapshot.setsum -= frag.setsum;
            } else {
                ret_snapshot.fragments.push(frag);
            }
        }
        for snap in std::mem::take(&mut ret_snapshot.snapshots).into_iter() {
            if snap.limit <= first_to_keep {
                ret_children.push(
                    Self::drop_snapshot(storage, prefix, &snap, throttle, snapshots, drop_acc)
                        .await?,
                );
                ret_snapshot.setsum -= snap.setsum;
            } else if (snap.start..snap.limit).contains(&first_to_keep) {
                let drop_acc_preserved = *drop_acc;
                let GarbageAction::ReplaceSnapshot {
                    old_path_to_snapshot,
                    old_snapshot_setsum,
                    new_snapshot,
                    children,
                } = Box::pin(Self::replace_snapshot(
                    storage,
                    prefix,
                    &snap,
                    throttle,
                    snapshots,
                    first_to_keep,
                    drop_acc,
                ))
                .await?
                else {
                    return Err(Box::new(ScrubError::Internal(
                        "replace snapshot failed to generate a replace snapshot".to_string(),
                    ))
                    .into());
                };
                if *drop_acc - drop_acc_preserved != new_snapshot.setsum - old_snapshot_setsum {
                    return Err(Box::new(ScrubError::CorruptSnapshotReplace {
                        lhs_before: drop_acc_preserved,
                        lhs_after: *drop_acc,
                        rhs_before: old_snapshot_setsum,
                        rhs_after: new_snapshot.setsum,
                    })
                    .into());
                }
                ret_children.push(GarbageAction::ReplaceSnapshot {
                    old_path_to_snapshot,
                    old_snapshot_setsum,
                    new_snapshot,
                    children,
                });
                ret_snapshot.setsum -= *drop_acc - drop_acc_preserved;
            } else {
                ret_snapshot.snapshots.push(snap);
            }
        }
        Ok(GarbageAction::ReplaceSnapshot {
            old_path_to_snapshot: ptr.path_to_snapshot.clone(),
            old_snapshot_setsum: ptr.setsum,
            new_snapshot: ret_snapshot,
            children: ret_children,
        })
    }

    pub fn prefixed_paths(&self, prefix: &str) -> impl Iterator<Item = String> {
        fn prefixed_paths_for_action(
            prefix: &str,
            action: &GarbageAction,
        ) -> impl Iterator<Item = String> {
            let mut paths = vec![];
            match action {
                GarbageAction::DropFragment {
                    path_to_fragment,
                    fragment_setsum: _,
                } => paths.push(format!("{prefix}/{path_to_fragment}")),
                GarbageAction::DropSnapshot {
                    path_to_snapshot,
                    snapshot_setsum: _,
                    children,
                } => {
                    paths.push(format!("{prefix}/{path_to_snapshot}"));
                    for child in children {
                        paths.extend(prefixed_paths_for_action(prefix, child));
                    }
                }
                GarbageAction::ReplaceSnapshot {
                    old_path_to_snapshot,
                    old_snapshot_setsum: _,
                    new_snapshot: _,
                    children,
                } => {
                    paths.push(format!("{prefix}/{old_path_to_snapshot}"));
                    for child in children {
                        paths.extend(prefixed_paths_for_action(prefix, child));
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
            GarbageAction::ReplaceSnapshot {
                new_snapshot,
                children,
                ..
            } => {
                new_snapshot.install(throttle, storage, prefix).await?;
                for child in children {
                    Box::pin(
                        self.install_new_snapshots_from_action(storage, prefix, throttle, child),
                    )
                    .await?;
                }
                Ok(())
            }
        }
    }
}

/////////////////////////////////////////// GarbageAction //////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GarbageAction {
    DropSnapshot {
        path_to_snapshot: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        snapshot_setsum: Setsum,
        children: Vec<GarbageAction>,
    },
    ReplaceSnapshot {
        old_path_to_snapshot: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        old_snapshot_setsum: Setsum,
        new_snapshot: Snapshot,
        children: Vec<GarbageAction>,
    },
    DropFragment {
        path_to_fragment: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        fragment_setsum: Setsum,
    },
}

impl GarbageAction {
    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, Error> {
        match self {
            Self::DropFragment {
                fragment_setsum,
                path_to_fragment: _,
            } => Ok(*fragment_setsum),
            Self::DropSnapshot {
                snapshot_setsum,
                children,
                path_to_snapshot: _,
            } => scrub(children, *snapshot_setsum),
            Self::ReplaceSnapshot {
                old_path_to_snapshot: _,
                old_snapshot_setsum,
                new_snapshot,
                children,
            } => scrub(children, new_snapshot.setsum - *old_snapshot_setsum),
        }
    }
}

/////////////////////////////////////////////// util ///////////////////////////////////////////////

#[allow(clippy::result_large_err)]
fn scrub(actions: &[GarbageAction], expected_setsum: Setsum) -> Result<Setsum, Error> {
    let to_drop = actions
        .iter()
        .map(GarbageAction::scrub)
        .collect::<Result<Vec<_>, Error>>()?;
    let dropped_setsum = to_drop.into_iter().fold(Setsum::default(), Setsum::add);
    if dropped_setsum != expected_setsum {
        return Err(Box::new(ScrubError::CorruptGarbage {
            expected_setsum,
            returned_setsum: dropped_setsum,
        })
        .into());
    }
    Ok(dropped_setsum)
}
