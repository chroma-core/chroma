use std::ops::Add;

use setsum::Setsum;

use crate::{
    deserialize_setsum, serialize_setsum, Fragment, LogPosition, Manifest, ScrubError, Snapshot,
    SnapshotPointer,
};

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct Garbage {
    pub dropped_setsum: Setsum,
    pub actions: Vec<GarbageAction>,
    pub cutoff: LogPosition,
}

impl Garbage {
    #[allow(clippy::result_large_err)]
    pub fn new(
        manifest: &Manifest,
        snapshots: &[Snapshot],
        first_to_keep: LogPosition,
    ) -> Result<Self, ScrubError> {
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
            actions.push(Self::drop_snapshot(snap, snapshots, &mut drop_acc)?);
        }
        for snap in replaced_snapshots {
            actions.push(Self::replace_snapshot(
                snap,
                snapshots,
                first_to_keep,
                &mut drop_acc,
            )?);
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

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
        scrub(&self.actions, self.dropped_setsum)
    }

    #[allow(clippy::result_large_err)]
    fn drop_fragment(frag: &Fragment, drop_acc: &mut Setsum) -> Result<GarbageAction, ScrubError> {
        *drop_acc += frag.setsum;
        Ok(GarbageAction::DropFragment {
            path_to_fragment: frag.path.clone(),
            fragment_setsum: frag.setsum,
        })
    }

    #[allow(clippy::result_large_err)]
    fn drop_snapshot(
        ptr: &SnapshotPointer,
        snapshots: &[Snapshot],
        drop_acc: &mut Setsum,
    ) -> Result<GarbageAction, ScrubError> {
        let Some(snapshot) = snapshots.iter().find(|snap| snap.setsum == ptr.setsum) else {
            return Err(ScrubError::MissingSnapshot {
                reference: ptr.clone(),
            });
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
            children.push(Self::drop_snapshot(
                snap,
                snapshots,
                &mut Setsum::default(),
            )?);
        }
        Ok(GarbageAction::DropSnapshot {
            path_to_snapshot: snapshot.path.clone(),
            snapshot_setsum: snapshot.setsum,
            children,
        })
    }

    #[allow(clippy::result_large_err)]
    fn replace_snapshot(
        ptr: &SnapshotPointer,
        snapshots: &[Snapshot],
        first_to_keep: LogPosition,
        drop_acc: &mut Setsum,
    ) -> Result<GarbageAction, ScrubError> {
        let Some(snapshot) = snapshots.iter().find(|snap| snap.setsum == ptr.setsum) else {
            return Err(ScrubError::MissingSnapshot {
                reference: ptr.clone(),
            });
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
                ret_children.push(Self::drop_snapshot(&snap, snapshots, drop_acc)?);
                ret_snapshot.setsum -= snap.setsum;
            } else if (snap.start..snap.limit).contains(&first_to_keep) {
                let drop_acc_preserved = *drop_acc;
                let GarbageAction::ReplaceSnapshot {
                    old_path_to_snapshot,
                    old_snapshot_setsum,
                    new_snapshot,
                    children,
                } = Self::replace_snapshot(&snap, snapshots, first_to_keep, drop_acc)?
                else {
                    return Err(ScrubError::Internal(
                        "replace snapshot failed to generate a replace snapshot".to_string(),
                    ));
                };
                if *drop_acc - drop_acc_preserved != new_snapshot.setsum - old_snapshot_setsum {
                    return Err(ScrubError::CorruptSnapshotReplace {
                        lhs_before: drop_acc_preserved,
                        lhs_after: *drop_acc,
                        rhs_before: old_snapshot_setsum,
                        rhs_after: new_snapshot.setsum,
                    });
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
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
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
fn scrub(actions: &[GarbageAction], expected_setsum: Setsum) -> Result<Setsum, ScrubError> {
    let to_drop = actions
        .iter()
        .map(GarbageAction::scrub)
        .collect::<Result<Vec<_>, ScrubError>>()?;
    let dropped_setsum = to_drop.into_iter().fold(Setsum::default(), Setsum::add);
    if dropped_setsum != expected_setsum {
        return Err(ScrubError::CorruptGarbage {
            expected_setsum,
            returned_setsum: dropped_setsum,
        });
    }
    Ok(dropped_setsum)
}
