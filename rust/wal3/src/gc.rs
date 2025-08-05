use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use setsum::Setsum;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, PutOptions, Storage,
    StorageError,
};

use crate::manifest::unprefixed_snapshot_path;
use crate::writer::OnceLogWriter;
use crate::{
    deserialize_setsum, prefixed_fragment_path, serialize_setsum, Error, Fragment, FragmentSeqNo,
    GarbageCollectionOptions, LogPosition, LogWriterOptions, Manifest, ScrubError, Snapshot,
    SnapshotCache, SnapshotPointer, ThrottleOptions,
};

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
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
    pub fn empty() -> Self {
        Garbage {
            snapshots_to_drop: Vec::new(),
            snapshots_to_make: Vec::new(),
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(0),
            fragments_to_drop_limit: FragmentSeqNo(0),
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }

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
        mut first_to_keep: LogPosition,
    ) -> Result<Option<Self>, Error> {
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
                .drop_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshots,
                    &mut first,
                    &mut first_to_keep,
                )
                .await?;
        }
        for frag in dropped_fragments {
            drop_acc += ret.drop_fragment(frag, &mut first, &mut first_to_keep)?;
        }
        for snap in replaced_snapshots {
            let (drop_delta, root) = ret
                .replace_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshots,
                    &mut first_to_keep,
                    &mut first,
                )
                .await?;
            drop_acc += drop_delta;
            ret.snapshot_for_root = root;
        }
        if drop_acc != ret.setsum_to_discard {
            return Err(Error::ScrubError(Box::new(ScrubError::CorruptGarbage(
                "setsums don't balance".to_string(),
            ))));
        }
        ret.first_to_keep = first_to_keep;
        if !first {
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(storage))]
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

    #[tracing::instrument(skip(self, storage))]
    pub async fn install(
        &self,
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        existing: Option<&ETag>,
    ) -> Result<Option<ETag>, Error> {
        self.install_new_snapshots(storage, prefix, options).await?;
        Self::transition(options, storage, prefix, existing, self).await
    }

    #[tracing::instrument(skip(self, storage))]
    pub async fn reset(
        &self,
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        existing: &ETag,
    ) -> Result<Option<ETag>, Error> {
        match Self::transition(options, storage, prefix, Some(existing), &Self::empty()).await {
            Ok(e_tag) => Ok(e_tag),
            Err(Error::LogContentionFailure) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn transition(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        existing: Option<&ETag>,
        replacement: &Self,
    ) -> Result<Option<ETag>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retry_count = 0;
        loop {
            let path = Self::path(prefix);
            let payload = serde_json::to_string(replacement)
                .map_err(|e| {
                    Error::CorruptManifest(format!("could not encode JSON garbage: {e:?}"))
                })?
                .into_bytes();
            let options = if let Some(e_tag) = existing {
                PutOptions::if_matches(e_tag, StorageRequestPriority::P0)
            } else {
                PutOptions::if_not_exists(StorageRequestPriority::P0)
            };
            match storage.put_bytes(&path, payload, options).await {
                Ok(e_tag) => return Ok(e_tag),
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    // NOTE(rescrv):  We know that we put the file.  The e_tag no longer matches.
                    // Therefore, we know someone else transitioned the file and our reset should
                    // be a NOP.
                    return Err(Error::LogContentionFailure);
                }
                Err(e) => {
                    tracing::error!("error uploading garbage: {e:?}");
                    let backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(60) || retry_count >= 3 {
                        return Err(Arc::new(e).into());
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
            retry_count += 1;
        }
    }

    pub fn prefixed_paths_to_delete(&self, prefix: &str) -> impl Iterator<Item = String> {
        let prefix = Arc::new(prefix.to_string());
        let mut paths = vec![];
        for snap in self.snapshots_to_drop.iter() {
            paths.push(format!("{}/{}", prefix, snap.path_to_snapshot));
        }
        paths.into_iter().chain(
            (self.fragments_to_drop_start.0..self.fragments_to_drop_limit.0)
                .map(move |seq_no| (seq_no, Arc::clone(&prefix)))
                .map(|(seq_no, prefix)| prefixed_fragment_path(&prefix, FragmentSeqNo(seq_no))),
        )
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

    pub fn drop_fragment(
        &mut self,
        frag: &Fragment,
        first: &mut bool,
        first_to_keep: &mut LogPosition,
    ) -> Result<Setsum, Error> {
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
        *first_to_keep = frag.limit;
        Ok(frag.setsum)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn drop_snapshot(
        &mut self,
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
        first: &mut bool,
        first_to_keep: &mut LogPosition,
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
                first_to_keep,
            ))
            .await?;
        }
        for frag in snapshot.fragments.iter() {
            drop_acc += self.drop_fragment(frag, first, first_to_keep)?;
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
        first_to_keep: &mut LogPosition,
        first: &mut bool,
    ) -> Result<(Setsum, Option<SnapshotPointer>), Error> {
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
            if frag.limit <= *first_to_keep {
                fragments_to_drop.push(frag);
            } else {
                fragments_to_keep.push(frag.clone());
            }
        }
        for snap in snapshot.snapshots.iter() {
            if snap.limit <= *first_to_keep {
                snapshots_to_drop.push(snap);
            } else if (snap.start..snap.limit).contains(first_to_keep) {
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
                .drop_snapshot(
                    storage,
                    prefix,
                    snap,
                    throttle,
                    snapshot_cache,
                    first,
                    first_to_keep,
                )
                .await?;
        }
        let new_snapshot_pointer;
        // SAFETY(rescrv):  This has 0 or 1 elements by the snapshot balance check above.
        if let Some(to_split) = snapshots_to_split.pop() {
            let (drop_delta, new_child) = Box::pin(self.replace_snapshot(
                storage,
                prefix,
                &to_split,
                throttle,
                snapshot_cache,
                first_to_keep,
                first,
            ))
            .await?;
            drop_acc += drop_delta;
            if let Some(new_child) = new_child {
                snapshots_to_keep.insert(0, new_child);
            }
        }
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
        let new_snapshot = Snapshot {
            path,
            setsum,
            depth,
            snapshots,
            fragments,
            writer: "garbage collection".to_string(),
        };
        if new_snapshot.to_pointer() != *ptr
            && (!new_snapshot.fragments.is_empty() || !new_snapshot.snapshots.is_empty())
        {
            new_snapshot_pointer = Some(new_snapshot.to_pointer());
            self.snapshots_to_drop.push(ptr.clone());
            self.snapshots_to_make.push(new_snapshot);
        } else {
            // NOTE(rescrv):  The pointer is the same as the new snapshot or they differ and both
            // fragments and snapshots were empty.  I can reason to say that the latter case is
            // impossible (a half open interval necessary to make that happen will never match the
            // replace condition), so the only case that can happen is the former.
            new_snapshot_pointer = Some(ptr.clone());
        }
        for frag in fragments_to_drop.iter() {
            drop_acc += self.drop_fragment(frag, first, first_to_keep)?;
        }
        Ok((drop_acc, new_snapshot_pointer))
    }

    /// Only call this function if you know what bug you are fixing.  The code documents the bug,
    /// but it is omitted from the documentation.
    // NOTE(rescrv):
    // - The bug:  Delete the data before updating the manifest.
    // - The fallout:  The manifest refers to a snapshot that doesn't exist; the next pass fails.
    // - The fix:  Generate a garbage file that erases the snapshots.
    //
    // manifest is the manifest to use for getting snapshots to drop.
    // seq_no is the seq_no of the first fragment to keep.
    // offset is the log position of the first record to keep.
    //
    // To determine these values, find the first snapshot that is in the manifest that wasn't
    // erased.  It will give you the offset as its start.  Follow the left-most snapshot from that
    // snapshot, recursively, until you find the first fragment.  That's your seq_no.
    pub fn bug_patch_construct_garbage_from_manifest(
        manifest: &Manifest,
        seq_no: FragmentSeqNo,
        offset: LogPosition,
    ) -> Garbage {
        let mut garbage = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: seq_no,
            fragments_to_drop_limit: seq_no,
            setsum_to_discard: Setsum::default(),
            first_to_keep: offset,
        };
        for snapshot in manifest.snapshots.iter() {
            if snapshot.limit <= garbage.first_to_keep {
                garbage.snapshots_to_drop.push(snapshot.clone());
                garbage.setsum_to_discard += snapshot.setsum;
            }
        }
        garbage
    }
}

///////////////////////////////////////// GarbageCollector /////////////////////////////////////////

pub struct GarbageCollector {
    log: Arc<OnceLogWriter>,
}

impl GarbageCollector {
    /// Open the log into a state where it can be garbage collected.
    pub async fn open(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: &str,
        writer: &str,
    ) -> Result<Self, Error> {
        let log = OnceLogWriter::open_for_read_only_and_stale_ops(
            options.clone(),
            Arc::clone(&storage),
            prefix.to_string(),
            writer.to_string(),
            Arc::new(()),
        )
        .await?;
        Ok(Self { log })
    }

    pub async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<bool, Error> {
        self.log
            .garbage_collect_phase1_compute_garbage(options, keep_at_least)
            .await
    }

    pub async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
    ) -> Result<(), Error> {
        self.log
            .garbage_collect_phase3_delete_garbage(options)
            .await
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_seen_in_the_wild() {
        let manifest_json =
            include_str!("../tests/test_k8s_integration_AA_construct_garbage/MANIFEST");
        let manifest: Manifest = serde_json::from_str(manifest_json).unwrap();
        let output = Garbage::bug_patch_construct_garbage_from_manifest(
            &manifest,
            FragmentSeqNo(806913),
            LogPosition::from_offset(900883),
        );
        assert_eq!(output.fragments_to_drop_start, FragmentSeqNo(806913));
        assert_eq!(output.fragments_to_drop_limit, FragmentSeqNo(806913));
        assert_eq!(
            output.setsum_to_discard.hexdigest(),
            "c921d21a0820be5d3b6f2d90942648f2853188bb0e3c6a22fe3dbd81c1e1c380"
        );
        assert_eq!(output.first_to_keep, LogPosition::from_offset(900883));
        for snapshot in output.snapshots_to_drop.iter() {
            assert!(snapshot.limit <= output.first_to_keep);
        }
    }
}
