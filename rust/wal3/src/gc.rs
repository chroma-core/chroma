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
    pub actions: GarbageAction,
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
        let mut actions = GarbageAction::default();
        for snap in dropped_snapshots {
            let action = Self::drop_snapshot(storage, prefix, snap, throttle, snapshots).await?;
            actions.merge(action)?;
        }
        for snap in replaced_snapshots {
            let action =
                Self::replace_snapshot(storage, prefix, snap, throttle, snapshots, first_to_keep)
                    .await?;
            actions.merge(action)?;
        }
        for frag in dropped_fragments {
            let action = Self::drop_fragment(frag)?;
            actions.merge(action)?;
        }
        let cutoff = first_to_keep;
        let garbage = Garbage { actions, cutoff };
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
        self.actions.snapshots_to_drop.is_empty() && self.actions.fragments_to_drop.is_empty()
    }

    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, Error> {
        self.actions.scrub()?;
        Ok(self.actions.setsum_to_discard)
    }

    #[allow(clippy::result_large_err)]
    fn drop_fragment(frag: &Fragment) -> Result<GarbageAction, Error> {
        Ok(GarbageAction {
            snapshots_to_drop: vec![],
            fragments_to_drop: vec![frag.clone()],
            snapshots_to_make: vec![],
            setsum_to_discard: frag.setsum,
            root_snapshot_ptr: None,
        })
    }

    #[allow(clippy::result_large_err)]
    async fn drop_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
    ) -> Result<GarbageAction, Error> {
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
        let mut actions = GarbageAction::default();
        actions.drop_snapshot(snapshot.clone())?;
        for snap in snapshot.snapshots.iter() {
            let action = Box::pin(Self::drop_snapshot(
                storage,
                prefix,
                snap,
                throttle,
                snapshot_cache,
            ))
            .await?;
            actions.merge(action)?;
        }
        for frag in snapshot.fragments.iter() {
            let action = Self::drop_fragment(frag)?;
            actions.merge(action)?;
        }
        actions.scrub()?;
        Ok(actions)
    }

    #[allow(clippy::result_large_err)]
    async fn replace_snapshot(
        storage: &Storage,
        prefix: &str,
        ptr: &SnapshotPointer,
        throttle: &ThrottleOptions,
        snapshot_cache: &dyn SnapshotCache,
        first_to_keep: LogPosition,
    ) -> Result<GarbageAction, Error> {
        let snapshot = match snapshot_cache.get(ptr).await? {
            Some(snapshot) => snapshot,
            None => match Snapshot::load(throttle, storage, prefix, ptr).await? {
                Some(snapshot) => snapshot,
                None => {
                    eprintln!("FINDME {}:{}", file!(), line!());
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
        let mut actions = GarbageAction::default();
        for snap in snapshots_to_drop {
            let action = Box::pin(Self::drop_snapshot(
                storage,
                prefix,
                snap,
                throttle,
                snapshot_cache,
            ))
            .await?;
            actions.merge(action)?;
        }
        for frag in fragments_to_drop {
            let action = Self::drop_fragment(frag)?;
            actions.merge(action)?;
        }
        let ptr = if let Some(to_split) = snapshots_to_split.pop() {
            let action = Box::pin(Self::replace_snapshot(
                storage,
                prefix,
                &to_split,
                throttle,
                snapshot_cache,
                first_to_keep,
            ))
            .await?;
            let ptr = action.root_snapshot_ptr.clone();
            actions.merge(action)?;
            ptr
        } else {
            None
        };
        if ptr.is_some() && (!snapshots_to_keep.is_empty() || !fragments_to_keep.is_empty()) {
            let mut snapshots = Vec::with_capacity(snapshots_to_keep.len() + 1);
            if let Some(ptr) = ptr {
                snapshots.push(ptr);
            }
            snapshots.extend(snapshots_to_keep);
            let depth = snapshots.iter().map(|s| s.depth).max().unwrap_or(0) + 1;
            let setsum = snapshots
                .iter()
                .map(|s| s.setsum)
                .fold(Setsum::default(), Setsum::add);
            let path = unprefixed_snapshot_path(setsum);
            let fragments = fragments_to_keep;
            actions.add_snapshot(Snapshot {
                path,
                setsum,
                depth,
                snapshots,
                fragments,
                writer: "garbage collection".to_string(),
            })?;
        } else if ptr.is_some() {
            // pass
        } else if snapshots_to_keep.len() + fragments_to_keep.len() > 1
            || snapshots_to_keep.is_empty()
        {
            let setsum = snapshots_to_keep
                .iter()
                .map(|s| s.setsum)
                .fold(Setsum::default(), Setsum::add)
                + fragments_to_keep
                    .iter()
                    .map(|f| f.setsum)
                    .fold(Setsum::default(), Setsum::add);
            let path = unprefixed_snapshot_path(setsum);
            let depth = snapshots_to_keep.iter().map(|s| s.depth).max().unwrap_or(0) + 1;
            let snapshots = snapshots_to_keep;
            let fragments = fragments_to_keep;
            actions.add_snapshot(Snapshot {
                path,
                setsum,
                depth,
                snapshots,
                fragments,
                writer: "garbage collection".to_string(),
            })?;
        } else if !snapshots_to_keep.is_empty() {
            // SAFETY(rescrv):  Assured by the > 1 condition above.
            assert_eq!(1, snapshots_to_keep.len());
            actions.set_root_snapshot(snapshots_to_keep[0].clone())?;
        }
        actions.scrub()?;
        Ok(actions)
    }

    pub fn prefixed_paths(&self, prefix: &str) -> impl Iterator<Item = String> {
        let mut paths = vec![];
        for snap in self.actions.snapshots_to_drop.iter() {
            paths.push(format!("{prefix}/{}", snap.path));
        }
        for frag in self.actions.fragments_to_drop.iter() {
            paths.push(format!("{prefix}/{}", frag.path));
        }
        paths.into_iter()
    }

    pub async fn install_new_snapshots(
        &self,
        storage: &Storage,
        prefix: &str,
        throttle: &ThrottleOptions,
    ) -> Result<(), Error> {
        for snap in self.actions.snapshots_to_make.iter() {
            snap.install(throttle, storage, prefix).await?;
        }
        Ok(())
    }
}

/////////////////////////////////////////// GarbageAction //////////////////////////////////////////

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct GarbageAction {
    pub snapshots_to_drop: Vec<Snapshot>,
    pub fragments_to_drop: Vec<Fragment>,
    pub snapshots_to_make: Vec<Snapshot>,
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum_to_discard: Setsum,
    pub root_snapshot_ptr: Option<SnapshotPointer>,
}

impl GarbageAction {
    pub fn set_root_snapshot(&mut self, ptr: SnapshotPointer) -> Result<(), Box<ScrubError>> {
        if self.root_snapshot_ptr.is_some() {
            todo!();
        }
        self.root_snapshot_ptr = Some(ptr);
        Ok(())
    }

    pub fn add_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Box<ScrubError>> {
        if let Some(root_snapshot_ptr) = self.root_snapshot_ptr.as_ref() {
            if !snapshot.snapshots.iter().any(|s| s == root_snapshot_ptr) {
                todo!();
            }
        }
        self.root_snapshot_ptr = Some(snapshot.to_pointer());
        self.snapshots_to_make.push(snapshot);
        Ok(())
    }

    pub fn drop_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Box<ScrubError>> {
        self.scrub()?;
        self.snapshots_to_drop.push(snapshot);
        self.scrub()?;
        Ok(())
    }

    pub fn scrub(&self) -> Result<(), Box<ScrubError>> {
        // NOTE(rescrv):  There are a number of invariants we'd like to preserve, but the number
        // one invariant is that anything reachable from snapshots_to_drop must be in
        // snapshots_to_drop, fragments_to_drop, or referred to from a snapshot in
        // snapshots_to_make.
        for snapshot in self.snapshots_to_drop.iter() {
            for unrefed_snap in snapshot.snapshots.iter() {
                // TODO(rescrv): check it.
            }
            for unrefed_frag in snapshot.fragments.iter() {
                // TODO(rescrv): check it.
            }
        }
        for unrefed_frag in self.fragments_to_drop.iter() {
            // TODO(rescrv): check it.
        }
        // TODO(rescrv): More invariant scrubbing.
        Ok(())
    }

    pub fn merge(&mut self, other: GarbageAction) -> Result<(), Box<ScrubError>> {
        other.scrub()?;
        self.scrub()?;
        if self.root_snapshot_ptr.is_some() && other.root_snapshot_ptr.is_some() {
            todo!();
        }
        if self.root_snapshot_ptr.is_none() {
            self.root_snapshot_ptr = other.root_snapshot_ptr;
        }
        self.snapshots_to_drop.extend(other.snapshots_to_drop);
        self.snapshots_to_make.extend(other.snapshots_to_make);
        self.fragments_to_drop.extend(other.fragments_to_drop);
        self.setsum_to_discard += other.setsum_to_discard;
        self.scrub()?;
        Ok(())
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
        let (nested_ptr, nested_snapshot, cache) = create_snapshot_for_split_test();

        // Set cutoff at position 10, which should trigger splitting the nested snapshot
        // that spans from 8 to 15
        let first_to_keep = LogPosition::from_offset(10);

        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

        // This should trigger the to_split case in replace_snapshot
        let action = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &nested_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            action.setsum_to_discard,
        );
        assert!(action.snapshots_to_drop.is_empty());
        assert_eq!(1, action.fragments_to_drop.len());
        assert_eq!(
            create_fragment(
                5,
                8,
                FragmentSeqNo(1),
                Setsum::from_hexdigest(
                    "00000000aaaaaaaa000000000000000000000000000000000000000000000000",
                )
                .unwrap(),
            ),
            action.fragments_to_drop[0]
        );
        assert_eq!(1, action.snapshots_to_make.len());
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
            action.snapshots_to_make[0],
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
        let action = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            action.setsum_to_discard
        );
        assert!(action.snapshots_to_drop.is_empty());
        assert_eq!(1, action.fragments_to_drop.len());
        assert_eq!(1, action.snapshots_to_make.len());
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
            action.snapshots_to_make[0]
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
        let action = Garbage::replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            first_to_keep,
        )
        .await
        .unwrap();

        assert_eq!(
            Setsum::from_hexdigest(
                "00000000aaaaaaaa000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            action.setsum_to_discard
        );
        assert!(action.snapshots_to_drop.is_empty());
        assert_eq!(1, action.fragments_to_drop.len());
        assert_eq!(1, action.snapshots_to_make.len());
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
            action.snapshots_to_make[0]
        );
    }

    #[test]
    fn drop_frag() {
        let setsum = Setsum::from_hexdigest(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let fragment = create_fragment(10, 20, FragmentSeqNo(1), setsum);

        let action = Garbage::drop_fragment(&fragment).unwrap();

        // Should return the same setsum
        assert_eq!(action.setsum_to_discard, setsum);

        // Should have the fragment in fragments_to_drop
        assert_eq!(action.fragments_to_drop.len(), 1);
        assert_eq!(action.fragments_to_drop[0], fragment);
        assert_eq!(action.snapshots_to_drop.len(), 0);
        assert_eq!(action.snapshots_to_make.len(), 0);

        // Test scrub on the created action
        action.scrub().unwrap();
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

        let action = Garbage::drop_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
        )
        .await
        .unwrap();

        // Should return the total setsum
        assert_eq!(action.setsum_to_discard, total_setsum);

        // Debug: let's see what we actually get
        println!("snapshots_to_drop: {}", action.snapshots_to_drop.len());
        println!("fragments_to_drop: {}", action.fragments_to_drop.len());
        println!("snapshots_to_make: {}", action.snapshots_to_make.len());

        // Should have all snapshots and fragments in the drop lists
        assert_eq!(action.snapshots_to_drop.len(), 2); // main + nested
        assert_eq!(action.fragments_to_drop.len(), 2); // both fragments
        assert_eq!(action.snapshots_to_make.len(), 0);

        // Verify the main snapshot is in the drop list
        assert!(action.snapshots_to_drop.contains(&main_snapshot));
        assert!(action.snapshots_to_drop.contains(&nested_snapshot));
        assert!(action.fragments_to_drop.contains(&fragment1));
        assert!(action.fragments_to_drop.contains(&fragment2));

        // Test scrub on the created action
        action.scrub().unwrap();
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

        let action = Garbage::replace_snapshot(
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
        assert_eq!(action.setsum_to_discard, frag1_setsum);

        // Should have dropped the first fragment
        assert_eq!(action.fragments_to_drop.len(), 1);
        assert_eq!(action.fragments_to_drop[0], fragment1);

        // Should have created a new snapshot with the kept fragments
        assert_eq!(action.snapshots_to_make.len(), 1);
        let new_snapshot = &action.snapshots_to_make[0];
        assert_eq!(new_snapshot.fragments.len(), 2);
        assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);
        assert!(new_snapshot.fragments.contains(&fragment2));
        assert!(new_snapshot.fragments.contains(&fragment3));

        // Test scrub on the created action
        action.scrub().unwrap();
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

        let action = Garbage::replace_snapshot(
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
        assert_eq!(action.setsum_to_discard, frag1_setsum);

        // Debug: let's see what we actually get
        println!("replace_snapshot_drops_snapshots_prior_to_cutoff:");
        println!("snapshots_to_drop: {}", action.snapshots_to_drop.len());
        println!("fragments_to_drop: {}", action.fragments_to_drop.len());
        println!("snapshots_to_make: {}", action.snapshots_to_make.len());

        // Should have dropped the first child snapshot
        assert_eq!(action.snapshots_to_drop.len(), 1);
        assert_eq!(action.snapshots_to_drop[0], child_snapshot1);

        // Should have created a new snapshot with the kept child snapshot and parent fragment
        assert_eq!(action.snapshots_to_make.len(), 1);
        let new_snapshot = &action.snapshots_to_make[0];
        assert_eq!(new_snapshot.snapshots.len(), 1);
        assert_eq!(new_snapshot.fragments.len(), 1);
        assert_eq!(new_snapshot.snapshots[0].setsum, frag2_setsum);
        assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);

        // Test scrub on the created action
        action.scrub().unwrap();
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

        let action = Garbage::replace_snapshot(
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
        assert_eq!(action.setsum_to_discard, frag1_setsum + frag2_setsum);

        // Should have dropped the first two fragments
        assert_eq!(action.fragments_to_drop.len(), 2);
        assert!(action.fragments_to_drop.contains(&fragment1));
        assert!(action.fragments_to_drop.contains(&fragment2));

        // Should have created a new snapshot with only the kept fragment
        assert_eq!(action.snapshots_to_make.len(), 1);
        let new_snapshot = &action.snapshots_to_make[0];
        assert_eq!(new_snapshot.fragments.len(), 1);
        assert_eq!(new_snapshot.fragments[0].setsum, frag3_setsum);
        assert_eq!(new_snapshot.setsum, frag3_setsum);

        // Test scrub on the created action
        action.scrub().unwrap();
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

        let action = Garbage::replace_snapshot(
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
        assert_eq!(action.setsum_to_discard, frag1_setsum);

        // Debug: let's see what we actually get
        println!("replace_snapshot_two_levels_rightmost_leaf:");
        println!("snapshots_to_drop: {}", action.snapshots_to_drop.len());
        println!("fragments_to_drop: {}", action.fragments_to_drop.len());
        println!("snapshots_to_make: {}", action.snapshots_to_make.len());

        // Should have dropped the left leaf snapshot
        assert_eq!(action.snapshots_to_drop.len(), 1);
        assert_eq!(action.snapshots_to_drop[0], left_leaf);

        // Should have created a new snapshot with the right-most (kept) leaf snapshot and interior fragment
        assert_eq!(action.snapshots_to_make.len(), 1);
        let new_snapshot = &action.snapshots_to_make[0];
        assert_eq!(new_snapshot.snapshots.len(), 1);
        assert_eq!(new_snapshot.fragments.len(), 1);
        assert_eq!(new_snapshot.snapshots[0].setsum, frag2_setsum);
        assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);

        // Test scrub on the created action
        action.scrub().unwrap();
    }
}
