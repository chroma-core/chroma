mod mocks;

use std::sync::Arc;

use setsum::Setsum;

use proptest::prelude::{ProptestConfig, Strategy};
use rand::RngCore;

use wal3::{
    Error, Fragment, FragmentIdentifier, FragmentSeqNo, Garbage, LogPosition, Manifest, Snapshot,
    SnapshotOptions, SnapshotPointer,
};

use mocks::MockManifestPublisher;

/// A mutable snapshot cache for testing that supports interior mutability.
pub struct TestingSnapshotCache {
    snapshots: std::sync::Mutex<Vec<Snapshot>>,
}

impl Default for TestingSnapshotCache {
    fn default() -> Self {
        Self {
            snapshots: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl TestingSnapshotCache {
    pub fn with_snapshots(snapshots: Vec<Snapshot>) -> Self {
        Self {
            snapshots: std::sync::Mutex::new(snapshots),
        }
    }

    pub fn add_snapshots(&self, new_snapshots: Vec<Snapshot>) {
        self.snapshots.lock().unwrap().extend(new_snapshots);
    }
}

#[async_trait::async_trait]
impl wal3::SnapshotCache for TestingSnapshotCache {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        Ok(self
            .snapshots
            .lock()
            .unwrap()
            .iter()
            .find(|x| x.setsum == ptr.setsum)
            .cloned())
    }

    async fn put(&self, _: &SnapshotPointer, _: &Snapshot) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct FragmentDelta {
    pub num_bytes: u64,
    pub num_records: u64,
    pub setsum: Setsum,
}

impl FragmentDelta {
    fn arbitrary() -> impl Strategy<Value = Self> {
        (1..8_000_000u64, 1..1000u64).prop_map(|(num_bytes, num_records)| {
            let mut setsum = Setsum::default();
            let mut rng = rand::thread_rng();
            let mut bytes = [0u8; 24];
            rng.fill_bytes(&mut bytes);
            setsum.insert(&bytes);
            FragmentDelta {
                num_bytes,
                num_records,
                setsum,
            }
        })
    }
}

fn deltas_to_fragment_sequence(deltas: &[FragmentDelta]) -> Vec<Fragment> {
    let mut fragments: Vec<Fragment> = vec![];
    for delta in deltas.iter() {
        let fragment = if let Some(recent) = fragments.last() {
            let next_seq_no = recent.seq_no.successor().expect("seq_no overflow in test");
            Fragment {
                path: wal3::unprefixed_fragment_path(next_seq_no),
                num_bytes: delta.num_bytes,
                setsum: delta.setsum,
                seq_no: next_seq_no,
                start: recent.limit,
                limit: recent.limit + delta.num_records,
            }
        } else {
            Fragment {
                path: wal3::unprefixed_fragment_path(FragmentIdentifier::SeqNo(
                    FragmentSeqNo::from_u64(1),
                )),
                num_bytes: delta.num_bytes,
                setsum: delta.setsum,
                seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(1) + delta.num_records,
            }
        };
        fragments.push(fragment);
    }
    fragments
}

proptest::proptest! {
    #[test]
    fn manifests(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1000)) {
        let mut manifest = Manifest::new_empty("test");
        let fragments = deltas_to_fragment_sequence(&deltas);
        for fragment in fragments.into_iter() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
        }
    }
}

proptest::proptest! {
    #[test]
    fn test_k8s_integration_manifests_with_snapshots(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1000), snapshot_rollover_threshold in 2..100usize, fragment_rollover_threshold in 2..100usize) {
        let mut manifest = Manifest::new_empty("test");
        let fragments = deltas_to_fragment_sequence(&deltas);
        for fragment in fragments.into_iter() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
            if let Some(snapshot) = manifest.generate_snapshot(SnapshotOptions {
                snapshot_rollover_threshold,
                fragment_rollover_threshold,
            }, "test") {
                assert!(manifest.apply_snapshot(&snapshot).is_ok());
            }
        }
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 5, .. ProptestConfig::default()
    })]

    #[test]
    fn test_k8s_integration_manifests_garbage(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1..75)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut manifest = Manifest::new_empty("test");
        println!("deltas = {deltas:#?}");
        let fragments = deltas_to_fragment_sequence(&deltas);
        println!("fragments = {fragments:#?}");
        for fragment in fragments.into_iter() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
        }
        eprintln!("starting manifest = {manifest:#?}");
        let start = manifest.oldest_timestamp();
        let limit = manifest.next_write_timestamp();
        let cache = Arc::new(TestingSnapshotCache::default());
        let mock_publisher = MockManifestPublisher::new(Arc::clone(&cache));
        let mut count = 0;
        let mut last_limit = 0;
        for offset in start.offset()..=limit.offset() {
            let position = LogPosition::from_offset(offset);
            eprintln!("position = {position:?}");
            let Some(garbage) = rt.block_on(Garbage::new(&manifest, &*cache, &mock_publisher, position)).unwrap() else {
                continue;
            };
            eprintln!("garbage = {garbage:#?}");
            let dropped = garbage.setsum_to_discard;
            if garbage.is_empty() {
                continue;
            }
            let Some(new_manifest) = manifest.apply_garbage(garbage.clone()).unwrap() else {
                panic!("garbage fail {garbage:#?}");
            };
            eprintln!("manifest.setsum = {}", manifest.setsum.hexdigest());
            eprintln!("new_manifest.setsum = {}", new_manifest.setsum.hexdigest());
            eprintln!("dropped = {}", dropped.hexdigest());
            // NOTE(rescrv):  This looks wrong.  It is not.
            //
            // Reasoning:  Garbage collection only advances a prefix of collected.  It doesn't
            // affect the totality of data that has been written, which is what gets captured by
            // manifest.setsum.  The relationship is collected + active = manifest.setsum.
            assert_eq!(manifest.setsum, new_manifest.setsum, "manifest.setsum mismatch");
            assert_eq!(manifest.collected + dropped, new_manifest.collected, "manifest.collected mismatch");
            assert!(new_manifest.scrub().is_ok(), "scrub error");
            count += 1;
            last_limit = offset;
        }
        assert!(count >= 1);
        assert!(LogPosition::from_offset(last_limit) == manifest.next_write_timestamp());
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1, .. ProptestConfig::default()
    })]

    #[test]
    fn test_k8s_integration_manifests_with_snapshots_garbage(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1..100), snapshot_rollover_threshold in 2..3usize, fragment_rollover_threshold in 2..3usize) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut manifest = Manifest::new_empty("test");
        println!("deltas = {deltas:#?}");
        let fragments = deltas_to_fragment_sequence(&deltas);
        println!("fragments = {fragments:#?}");
        let mut snapshots = vec![];
        for fragment in fragments.iter().cloned() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
            if let Some(snapshot) = manifest.generate_snapshot(SnapshotOptions {
                snapshot_rollover_threshold,
                fragment_rollover_threshold,
            }, "test") {
                assert!(manifest.apply_snapshot(&snapshot).is_ok());
                snapshots.push(snapshot);
            }
        }
        eprintln!("starting manifest = {manifest:#?}");
        let start = manifest.oldest_timestamp();
        let limit = manifest.next_write_timestamp();
        let cache = Arc::new(TestingSnapshotCache::with_snapshots(snapshots.clone()));
        let mock_publisher = MockManifestPublisher::new(Arc::clone(&cache));
        eprintln!("[{:?}, {:?})", start, limit);
        let mut last_initial_seq_no = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(0));
        for offset in start.offset()..=limit.offset() {
            let position = LogPosition::from_offset(offset);
            eprintln!("position = {position:?}");
            let garbage = rt.block_on(Garbage::new(&manifest, &*cache, &mock_publisher, position)).unwrap();
            let Some(garbage) = garbage else {
                continue;
            };
            eprintln!("garbage = {garbage:#?}");
            let dropped = garbage.setsum_to_discard;
            cache.add_snapshots(garbage.snapshots_to_make.clone());
            if garbage.is_empty() {
                continue;
            }
            let new_manifest = manifest.apply_garbage(garbage.clone()).unwrap().unwrap();
            eprintln!("manifest.setsum = {}", manifest.setsum.hexdigest());
            eprintln!("new_manifest.setsum = {}", new_manifest.setsum.hexdigest());
            eprintln!("dropped = {}", dropped.hexdigest());
            eprintln!("dropped^1 = {}", (Setsum::default()- dropped).hexdigest());
            assert_eq!(manifest.setsum, new_manifest.setsum, "manifest.setsum mismatch");
            assert_eq!(manifest.collected + dropped, new_manifest.collected, "manifest.collected mismatch");
            assert!(new_manifest.scrub().is_ok(), "scrub error");
            assert!(new_manifest.initial_seq_no.is_some() || new_manifest.initial_offset.is_none());
            if new_manifest.initial_seq_no.is_some() {
                assert!(new_manifest.initial_seq_no.unwrap() >= last_initial_seq_no);
                last_initial_seq_no = new_manifest.initial_seq_no.unwrap();
            }
        }
        assert_eq!(
            last_initial_seq_no,
            fragments
                .last()
                .unwrap()
                .seq_no
                .successor()
                .expect("seq_no overflow in test")
        );
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100, .. ProptestConfig::default()
    })]

    #[test]
    fn test_k8s_integration_manifests_with_snapshots_that_collide(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 16..32), snapshot_rollover_threshold in 2..=2usize, fragment_rollover_threshold in 2..=2usize) {
        // NOTE(rescrv):
        // Consider a snapshot tree that gets pruned to:
        // [MANIFEST] -> [SNAP C] -> [SNAP B] -> [SNAP A] -> [ONE FRAG]
        // All three manifests will have the same setsum.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut manifest = Manifest::new_empty("test");
        println!("deltas = {deltas:#?}");
        let fragments = deltas_to_fragment_sequence(&deltas);
        println!("fragments = {fragments:#?}");
        let mut snapshots = vec![];
        for fragment in fragments.into_iter() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
            if let Some(snapshot) = manifest.generate_snapshot(SnapshotOptions {
                snapshot_rollover_threshold,
                fragment_rollover_threshold,
            }, "test") {
                assert!(manifest.apply_snapshot(&snapshot).is_ok());
                snapshots.push(snapshot);
            }
        }
        let cache = Arc::new(TestingSnapshotCache::with_snapshots(snapshots.clone()));
        let mock_publisher = MockManifestPublisher::new(Arc::clone(&cache));
        // Pick as victim the most recent snapshot and select so that we keep just one snapshot and
        // one frag within that snapshot.
        assert!(!manifest.snapshots.is_empty());
        let victim = &manifest.snapshots[manifest.snapshots.len() - 1];
        eprintln!("victim = {victim:?}");
        eprintln!("position = {:?}", victim.limit - 1);
        let garbage = rt.block_on(Garbage::new(&manifest, &*cache, &mock_publisher, victim.limit - 1)).unwrap();
        eprintln!("garbage = {garbage:?}");
    }
}
