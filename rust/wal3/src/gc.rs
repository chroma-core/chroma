use std::ops::Add;
use std::sync::Arc;

use setsum::Setsum;

use chroma_storage::{ETag, StorageError};

use crate::interfaces::{
    FragmentManagerFactory, FragmentPointer, FragmentPublisher, ManifestManagerFactory,
    ManifestPublisher,
};
use crate::manifest::unprefixed_snapshot_path;
use crate::writer::OnceLogWriter;
use crate::{
    deserialize_setsum, prefixed_fragment_path, serialize_setsum, Error, Fragment,
    FragmentIdentifier, FragmentSeqNo, GarbageCollectionOptions, LogPosition, LogWriterOptions,
    Manifest, ScrubError, Snapshot, SnapshotCache, SnapshotPointer, ThrottleOptions,
};

const GARBAGE_PATH: &str = "gc/GARBAGE";

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Garbage {
    pub snapshots_to_drop: Vec<SnapshotPointer>,
    pub snapshots_to_make: Vec<Snapshot>,
    pub snapshot_for_root: Option<SnapshotPointer>,
    pub fragments_to_drop_start: FragmentSeqNo,
    pub fragments_to_drop_limit: FragmentSeqNo,
    #[serde(default)]
    pub fragments_are_uuids: bool,
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
            fragments_to_drop_start: FragmentSeqNo::ZERO,
            fragments_to_drop_limit: FragmentSeqNo::ZERO,
            fragments_are_uuids: false,
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        }
    }

    pub fn check_invariants_for_repl(&self) -> Result<(), Error> {
        // TODO(rescrv, mcmr):  Scrub more.
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }

    pub fn path(prefix: &str) -> String {
        format!("{}/{}", prefix, GARBAGE_PATH)
    }

    #[allow(clippy::result_large_err)]
    pub async fn new<P: FragmentPointer>(
        manifest: &Manifest,
        snapshots: &dyn SnapshotCache,
        manifest_publisher: &dyn ManifestPublisher<P>,
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
            fragments_to_drop_start: FragmentSeqNo::ZERO,
            fragments_to_drop_limit: FragmentSeqNo::ZERO,
            setsum_to_discard: Setsum::default(),
            fragments_are_uuids: manifest
                .fragments
                .iter()
                .all(|f| matches!(f.seq_no, FragmentIdentifier::Uuid(_))),
            first_to_keep,
        };
        let mut first = true;
        let mut drop_acc = Setsum::default();
        for snap in dropped_snapshots {
            drop_acc += ret
                .drop_snapshot(
                    snap,
                    snapshots,
                    manifest_publisher,
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
                    snap,
                    snapshots,
                    manifest_publisher,
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

    #[tracing::instrument(skip(fragment_publisher))]
    pub async fn load<FP: FragmentPointer>(
        options: &ThrottleOptions,
        fragment_publisher: &dyn FragmentPublisher<FragmentPointer = FP>,
    ) -> Result<Option<(Garbage, Option<ETag>)>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retries = 0;
        loop {
            match fragment_publisher.read_json_file(GARBAGE_PATH).await {
                Ok((ref garbage, e_tag)) => {
                    let garbage: Garbage = serde_json::from_slice(garbage).map_err(|e| {
                        Error::CorruptGarbage(format!("could not decode JSON garbage: {e:?}"))
                    })?;
                    return Ok(Some((garbage, e_tag)));
                }
                Err(err) => {
                    if let Error::StorageError(e) = &err {
                        if matches!(&**e, StorageError::NotFound { path: _, source: _ }) {
                            return Ok(None);
                        }
                    }
                    let backoff = exp_backoff.next();
                    tokio::time::sleep(backoff).await;
                    if retries >= 3 {
                        return Err(err);
                    }
                    retries += 1;
                }
            }
        }
    }

    #[tracing::instrument(skip(self, manifest_publisher, fragment_publisher))]
    pub async fn install<FP: FragmentPointer>(
        &self,
        manifest_publisher: &dyn ManifestPublisher<FP>,
        fragment_publisher: &(dyn FragmentPublisher<FragmentPointer = FP> + Sync),
        options: &ThrottleOptions,
        existing: Option<&ETag>,
    ) -> Result<Option<ETag>, Error> {
        self.install_new_snapshots(manifest_publisher).await?;
        fragment_publisher
            .write_garbage(options, existing, self)
            .await
    }

    pub fn prefixed_paths_to_delete(&self, prefix: &str) -> impl Iterator<Item = String> {
        let prefix = Arc::new(prefix.to_string());
        let mut paths = vec![];
        for snap in self.snapshots_to_drop.iter() {
            paths.push(format!("{}/{}", prefix, snap.path_to_snapshot));
        }
        let start = self.fragments_to_drop_start.as_u64();
        let limit = self.fragments_to_drop_limit.as_u64();
        paths.into_iter().chain((start..limit).map(move |seq_no| {
            prefixed_fragment_path(
                &prefix,
                FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(seq_no)),
            )
        }))
    }

    pub async fn install_new_snapshots<P: FragmentPointer>(
        &self,
        manifest_publisher: &dyn ManifestPublisher<P>,
    ) -> Result<(), Error> {
        for snap in self.snapshots_to_make.iter() {
            manifest_publisher.snapshot_install(snap).await?;
        }
        Ok(())
    }

    pub fn drop_fragment(
        &mut self,
        frag: &Fragment,
        first: &mut bool,
        first_to_keep: &mut LogPosition,
    ) -> Result<Setsum, Error> {
        if let Some(seq_no) = frag.seq_no.as_seq_no() {
            if FragmentIdentifier::from(self.fragments_to_drop_limit) != frag.seq_no && !*first {
                return Err(Error::ScrubError(Box::new(ScrubError::Internal(
                    "fragment sequence numbers collected out of order".to_string(),
                ))));
            }
            if *first {
                self.fragments_to_drop_start = seq_no;
            }
            self.fragments_to_drop_limit = seq_no.successor().ok_or_else(|| {
                Error::ScrubError(Box::new(ScrubError::Internal(
                    "fragment sequence number has no successor".to_string(),
                )))
            })?;
        }
        self.setsum_to_discard += frag.setsum;
        *first = false;
        *first_to_keep = frag.limit;
        Ok(frag.setsum)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn drop_snapshot<P: FragmentPointer>(
        &mut self,
        ptr: &SnapshotPointer,
        snapshot_cache: &dyn SnapshotCache,
        manifest_publisher: &dyn ManifestPublisher<P>,
        first: &mut bool,
        first_to_keep: &mut LogPosition,
    ) -> Result<Setsum, Error> {
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match manifest_publisher.snapshot_load(ptr).await? {
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
                snap,
                snapshot_cache,
                manifest_publisher,
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
    pub async fn replace_snapshot<P: FragmentPointer>(
        &mut self,
        ptr: &SnapshotPointer,
        snapshot_cache: &dyn SnapshotCache,
        manifest_publisher: &dyn ManifestPublisher<P>,
        first_to_keep: &mut LogPosition,
        first: &mut bool,
    ) -> Result<(Setsum, Option<SnapshotPointer>), Error> {
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match manifest_publisher.snapshot_load(ptr).await? {
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
                    snap,
                    snapshot_cache,
                    manifest_publisher,
                    first,
                    first_to_keep,
                )
                .await?;
        }
        let new_snapshot_pointer;
        // SAFETY(rescrv):  This has 0 or 1 elements by the snapshot balance check above.
        if let Some(to_split) = snapshots_to_split.pop() {
            let (drop_delta, new_child) = Box::pin(self.replace_snapshot(
                &to_split,
                snapshot_cache,
                manifest_publisher,
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
            fragments_are_uuids: false,
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

pub struct GarbageCollector<
    P: FragmentPointer,
    FP: FragmentManagerFactory<FragmentPointer = P>,
    MP: ManifestManagerFactory<FragmentPointer = P>,
> {
    log: Arc<OnceLogWriter<P, FP::Publisher, MP::Publisher>>,
}

impl<
        P: FragmentPointer,
        FP: FragmentManagerFactory<FragmentPointer = P>,
        MP: ManifestManagerFactory<FragmentPointer = P>,
    > GarbageCollector<P, FP, MP>
{
    /// Open the log into a state where it can be garbage collected.
    pub async fn open(
        options: LogWriterOptions,
        new_fragment_publisher: FP,
        new_manifest_publisher: MP,
    ) -> Result<Self, Error> {
        let batch_manager = new_fragment_publisher.make_publisher().await?;
        let manifest_manager = new_manifest_publisher.open_publisher().await?;
        let log = OnceLogWriter::open_for_read_only_and_stale_ops(
            options.clone(),
            batch_manager,
            manifest_manager,
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
        let manifest_json = include_str!("../tests/s3_AA_construct_garbage/MANIFEST");
        let manifest: Manifest = serde_json::from_str(manifest_json).unwrap();
        let output = Garbage::bug_patch_construct_garbage_from_manifest(
            &manifest,
            FragmentSeqNo::from_u64(806913),
            LogPosition::from_offset(900883),
        );
        assert_eq!(
            output.fragments_to_drop_start,
            FragmentSeqNo::from_u64(806913)
        );
        assert_eq!(
            output.fragments_to_drop_limit,
            FragmentSeqNo::from_u64(806913)
        );
        assert_eq!(
            output.setsum_to_discard.hexdigest(),
            "c921d21a0820be5d3b6f2d90942648f2853188bb0e3c6a22fe3dbd81c1e1c380"
        );
        assert_eq!(output.first_to_keep, LogPosition::from_offset(900883));
        for snapshot in output.snapshots_to_drop.iter() {
            assert!(snapshot.limit <= output.first_to_keep);
        }
    }

    /// Deserialize from JSON to verify the stable format of Garbage.
    #[test]
    fn deserialize_garbage_from_json() {
        let json = r#"{
            "snapshots_to_drop": [
                {
                    "setsum": "25bdf4e28c8079d3e0324db417fa28eede29f8d7cc34c71fac19c39ca4691792",
                    "path_to_snapshot": "snapshot/SNAPSHOT.25bdf4e28c8079d3e0324db417fa28eede29f8d7cc34c71fac19c39ca4691792",
                    "depth": 1,
                    "start": {"offset": 1},
                    "limit": {"offset": 2379},
                    "num_bytes": 3796051
                }
            ],
            "snapshots_to_make": [
                {
                    "path": "snapshot/SNAPSHOT.abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
                    "depth": 2,
                    "setsum": "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
                    "writer": "garbage collection",
                    "snapshots": [],
                    "fragments": [
                        {
                            "path": "log/0/1",
                            "seq_no": 1,
                            "start": {"offset": 1},
                            "limit": {"offset": 100},
                            "num_bytes": 1024,
                            "setsum": "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
                        }
                    ]
                }
            ],
            "snapshot_for_root": {
                "setsum": "1b5797c4029b6664a74ccebba552f95767a22731a375cdddc0538afd01e88116",
                "path_to_snapshot": "snapshot/SNAPSHOT.1b5797c4029b6664a74ccebba552f95767a22731a375cdddc0538afd01e88116",
                "depth": 1,
                "start": {"offset": 2379},
                "limit": {"offset": 4816},
                "num_bytes": 3694483
            },
            "fragments_to_drop_start": 100,
            "fragments_to_drop_limit": 200,
            "setsum_to_discard": "c921d21a0820be5d3b6f2d90942648f2853188bb0e3c6a22fe3dbd81c1e1c380",
            "first_to_keep": {"offset": 900883}
        }"#;

        let garbage: Garbage = serde_json::from_str(json).unwrap();

        // Verify snapshots_to_drop
        assert_eq!(garbage.snapshots_to_drop.len(), 1);
        assert_eq!(
            garbage.snapshots_to_drop[0].setsum.hexdigest(),
            "25bdf4e28c8079d3e0324db417fa28eede29f8d7cc34c71fac19c39ca4691792"
        );
        assert_eq!(
            garbage.snapshots_to_drop[0].path_to_snapshot,
            "snapshot/SNAPSHOT.25bdf4e28c8079d3e0324db417fa28eede29f8d7cc34c71fac19c39ca4691792"
        );
        assert_eq!(garbage.snapshots_to_drop[0].depth, 1);
        assert_eq!(
            garbage.snapshots_to_drop[0].start,
            LogPosition::from_offset(1)
        );
        assert_eq!(
            garbage.snapshots_to_drop[0].limit,
            LogPosition::from_offset(2379)
        );
        assert_eq!(garbage.snapshots_to_drop[0].num_bytes, 3796051);

        // Verify snapshots_to_make
        assert_eq!(garbage.snapshots_to_make.len(), 1);
        assert_eq!(
            garbage.snapshots_to_make[0].setsum.hexdigest(),
            "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
        );
        assert_eq!(garbage.snapshots_to_make[0].depth, 2);
        assert_eq!(garbage.snapshots_to_make[0].writer, "garbage collection");
        assert_eq!(garbage.snapshots_to_make[0].fragments.len(), 1);
        assert_eq!(
            garbage.snapshots_to_make[0].fragments[0].seq_no,
            FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1))
        );

        // Verify snapshot_for_root
        let root = garbage.snapshot_for_root.as_ref().unwrap();
        assert_eq!(
            root.setsum.hexdigest(),
            "1b5797c4029b6664a74ccebba552f95767a22731a375cdddc0538afd01e88116"
        );
        assert_eq!(root.start, LogPosition::from_offset(2379));
        assert_eq!(root.limit, LogPosition::from_offset(4816));

        // Verify fragment identifiers
        assert_eq!(
            garbage.fragments_to_drop_start,
            FragmentSeqNo::from_u64(100)
        );
        assert_eq!(
            garbage.fragments_to_drop_limit,
            FragmentSeqNo::from_u64(200)
        );

        // Verify setsum_to_discard
        assert_eq!(
            garbage.setsum_to_discard.hexdigest(),
            "c921d21a0820be5d3b6f2d90942648f2853188bb0e3c6a22fe3dbd81c1e1c380"
        );

        // Verify first_to_keep
        assert_eq!(garbage.first_to_keep, LogPosition::from_offset(900883));
    }

    #[test]
    fn garbage_path_includes_prefix_and_constant() {
        let path = Garbage::path("my-prefix");
        assert_eq!(path, "my-prefix/gc/GARBAGE");
        println!("garbage_path_includes_prefix_and_constant: path={}", path);
    }
}
