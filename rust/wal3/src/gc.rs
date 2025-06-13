use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use setsum::Setsum;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, PutOptions, Storage,
    StorageError,
};

use crate::manifest::unprefixed_snapshot_path;
use crate::{
    deserialize_setsum, serialize_setsum, unprefixed_fragment_path, Error, Fragment, FragmentSeqNo,
    LogPosition, Manifest, ScrubError, Snapshot, SnapshotCache, SnapshotPointer, ThrottleOptions,
};

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Garbage {
    pub snapshots_to_drop: Vec<SnapshotPointer>,
    pub snapshots_to_make: Vec<Snapshot>,
    pub snapshot_for_root: Option<SnapshotPointer>,
    pub fragments_to_drop_start: FragmentSeqNo,
    pub fragments_to_drop_limit: FragmentSeqNo,
    #[serde(
        deserialize_with = "deserialize_setsum",
        serialize_with = "serialize_setsum"
    )]
    pub setsum_to_discard: Setsum,
    pub first_to_keep: LogPosition,
}

impl Garbage {
    pub fn path(prefix: &str) -> String {
        format!("{}/gc/GARBAGE", prefix)
    }

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
        if replaced_snapshots.len() > 1 {
            return Err(Error::CorruptGarbage(
                "More than one snapshot needs replacing".to_string(),
            ));
        }
        let mut ret = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(0),
            fragments_to_drop_limit: FragmentSeqNo(0),
            setsum_to_discard: Setsum::default(),
            first_to_keep,
        };
        let mut first = true;
        let mut drop_acc = Setsum::default();
        for snap in dropped_snapshots {
            drop_acc += ret
                .drop_snapshot(storage, prefix, snap, throttle, snapshots, &mut first)
                .await?;
        }
        for frag in dropped_fragments {
            drop_acc += ret.drop_fragment(frag, &mut first)?;
        }
        for snap in replaced_snapshots {
            drop_acc += ret
                .replace_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshots,
                    first_to_keep,
                    &mut first,
                )
                .await?;
        }
        if drop_acc != ret.setsum_to_discard {
            return Err(Error::ScrubError(Box::new(ScrubError::CorruptGarbage(
                "setsums don't balance".to_string(),
            ))));
        }
        Ok(ret)
    }

    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<Option<(Garbage, Option<ETag>)>, Error> {
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
                Ok((ref garbage, e_tag)) => {
                    let garbage: Garbage = serde_json::from_slice(garbage).map_err(|e| {
                        Error::CorruptGarbage(format!("could not decode JSON garbage: {e:?}"))
                    })?;
                    return Ok(Some((garbage, e_tag)));
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
    ) -> Result<Option<ETag>, Error> {
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
                Ok(e_tag) => return Ok(e_tag),
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

    pub fn prefixed_paths_to_delete(&self, prefix: &str) -> impl Iterator<Item = String> {
        let mut paths = vec![];
        for snap in self.snapshots_to_drop.iter() {
            paths.push(format!("{}/{}", prefix, snap.path_to_snapshot));
        }
        // TODO(rescrv):  When Step stabilizes, revisit this ugliness.
        for seq_no in self.fragments_to_drop_start.0..self.fragments_to_drop_limit.0 {
            paths.push(format!(
                "{}/{}",
                prefix,
                unprefixed_fragment_path(FragmentSeqNo(seq_no))
            ));
        }
        paths.into_iter()
    }

    pub async fn install_new_snapshots(
        &self,
        storage: &Storage,
        prefix: &str,
        throttle: &ThrottleOptions,
    ) -> Result<(), Error> {
        for snap in self.snapshots_to_make.iter() {
            snap.install(throttle, storage, prefix).await?;
        }
        Ok(())
    }

    pub fn drop_fragment(&mut self, frag: &Fragment, first: &mut bool) -> Result<Setsum, Error> {
        if self.fragments_to_drop_limit != frag.seq_no && !*first {
            return Err(Error::ScrubError(Box::new(ScrubError::Internal(
                "fragment sequence numbers collected out of order".to_string(),
            ))));
        }
        if *first {
            self.fragments_to_drop_start = frag.seq_no;
        }
        self.fragments_to_drop_limit = frag.seq_no + 1;
        self.setsum_to_discard += frag.setsum;
        *first = false;
        Ok(frag.setsum)
    }

    pub async fn drop_snapshot(
        &mut self,
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
        first: &mut bool,
    ) -> Result<Setsum, Error> {
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match Snapshot::load(throttle, storage, prefix, ptr).await? {
                Some(snapshot) => snapshot,
                None => {
                    return Err(Box::new(ScrubError::MissingSnapshot {
                        reference: ptr.clone(),
                    })
                    .into());
                }
            },
        };
        let mut drop_acc = Setsum::default();
        for snap in snapshot.snapshots.iter() {
            drop_acc += Box::pin(self.drop_snapshot(
                storage,
                prefix,
                snap,
                throttle,
                snapshot_cache,
                first,
            ))
            .await?;
        }
        for frag in snapshot.fragments.iter() {
            drop_acc += self.drop_fragment(frag, first)?;
        }
        if drop_acc == snapshot.setsum {
            self.snapshots_to_drop.push(ptr.clone());
            Ok(snapshot.setsum)
        } else {
            Err(Error::ScrubError(Box::new(
                ScrubError::CorruptSnapshotDrop {
                    lhs: snapshot.setsum,
                    rhs: drop_acc,
                },
            )))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn replace_snapshot(
        &mut self,
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
        first_to_keep: LogPosition,
        first: &mut bool,
    ) -> Result<Setsum, Error> {
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match Snapshot::load(throttle, storage, prefix, ptr).await? {
                Some(snapshot) => snapshot,
                None => {
                    return Err(Box::new(ScrubError::MissingSnapshot {
                        reference: ptr.clone(),
                    })
                    .into());
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
        // NOTE(rescrv):  Must process snaps, splits, frags in that order to ensure that fragments
        // get rolled up/collected in order.
        let mut drop_acc = Setsum::default();
        for snap in snapshots_to_drop.iter() {
            drop_acc += self
                .drop_snapshot(storage, prefix, snap, throttle, snapshot_cache, first)
                .await?;
        }
        let mut different = false;
        // SAFETY(rescrv):  This has 0 or 1 elements by the snapshot balance check above.
        if let Some(to_split) = snapshots_to_split.pop() {
            drop_acc += Box::pin(self.replace_snapshot(
                storage,
                prefix,
                &to_split,
                throttle,
                snapshot_cache,
                first_to_keep,
                first,
            ))
            .await?;
            if let Some(child) = self.snapshot_for_root.take() {
                different = child != to_split;
                snapshots_to_keep.insert(0, child);
            }
        }
        if different
            || !fragments_to_keep.is_empty()
            || snapshots_to_keep.len() + fragments_to_keep.len() > 1
        {
            if different || !fragments_to_drop.is_empty() || !snapshots_to_drop.is_empty() {
                let snapshots = snapshots_to_keep;
                let fragments = fragments_to_keep;
                let setsum = snapshots
                    .iter()
                    .map(|s| s.setsum)
                    .fold(Setsum::default(), Setsum::add)
                    + fragments
                        .iter()
                        .map(|f| f.setsum)
                        .fold(Setsum::default(), Setsum::add);
                let path = unprefixed_snapshot_path(setsum);
                let depth = snapshots.iter().map(|s| s.depth).max().unwrap_or(0) + 1;
                let snapshot = Snapshot {
                    path,
                    setsum,
                    depth,
                    snapshots,
                    fragments,
                    writer: "garbage collection".to_string(),
                };
                self.snapshots_to_drop.push(ptr.clone());
                self.snapshot_for_root = Some(snapshot.to_pointer());
                self.snapshots_to_make.push(snapshot);
            } else {
                self.snapshot_for_root = Some(ptr.clone());
            }
        } else if let Some(snap) = snapshots_to_keep.last() {
            assert_eq!(
                1,
                snapshots_to_keep.len(),
                "snapshots_to_keep.len() > 1 would trip above condition"
            );
            assert!(fragments_to_keep.is_empty(), "ensured by first condition");
            self.snapshots_to_drop.push(ptr.clone());
            self.snapshot_for_root = Some(snap.clone());
        } else {
            assert!(
                snapshots_to_keep.is_empty() && fragments_to_keep.is_empty(),
                "guaranteed by first condition of block"
            );
        }
        for frag in fragments_to_drop.iter() {
            drop_acc += self.drop_fragment(frag, first)?;
        }
        Ok(drop_acc)
    }
}
