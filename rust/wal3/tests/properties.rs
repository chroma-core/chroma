use chroma_storage::s3_client_for_test_with_new_bucket;

use setsum::Setsum;

use proptest::prelude::{ProptestConfig, Strategy};
use rand::RngCore;

use wal3::{
    Error, Fragment, FragmentSeqNo, Garbage, LogPosition, Manifest, Snapshot, SnapshotOptions,
    SnapshotPointer, ThrottleOptions,
};

#[derive(Default)]
pub struct TestingSnapshotCache {
    snapshots: Vec<Snapshot>,
}

#[async_trait::async_trait]
impl wal3::SnapshotCache for TestingSnapshotCache {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        Ok(self
            .snapshots
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
            Fragment {
                path: wal3::unprefixed_fragment_path(recent.seq_no + 1),
                num_bytes: delta.num_bytes,
                setsum: delta.setsum,
                seq_no: recent.seq_no + 1,
                start: recent.limit,
                limit: recent.limit + delta.num_records,
            }
        } else {
            Fragment {
                path: wal3::unprefixed_fragment_path(FragmentSeqNo(1)),
                num_bytes: delta.num_bytes,
                setsum: delta.setsum,
                seq_no: FragmentSeqNo(1),
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
    fn manifests_with_snapshots(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1000), snapshot_rollover_threshold in 2..100usize, fragment_rollover_threshold in 2..100usize) {
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
        cases: 1, .. ProptestConfig::default()
    })]

    #[test]
    fn manifests_garbage(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 75)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let storage = rt.block_on(s3_client_for_test_with_new_bucket());
        let throttle = ThrottleOptions::default();
        let mut manifest = Manifest::new_empty("test");
        println!("deltas = {deltas:#?}");
        let fragments = deltas_to_fragment_sequence(&deltas);
        println!("fragments = {fragments:#?}");
        for fragment in fragments.into_iter() {
            assert!(manifest.can_apply_fragment(&fragment));
            manifest.apply_fragment(fragment);
        }
        eprintln!("starting manifest = {manifest:#?}");
        let start = manifest.oldest_timestamp().unwrap();
        let limit = manifest.newest_timestamp().unwrap();
        let cache = TestingSnapshotCache::default();
        for offset in start.offset()..limit.offset() {
            let position = LogPosition::from_offset(offset);
            let garbage = rt.block_on(Garbage::new(&storage, "manifests_gargage", &manifest, &throttle, &cache, position)).unwrap();
            eprintln!("garbage = {garbage:#?}");
            let dropped = garbage.scrub().unwrap();
            assert!(garbage.is_empty() || !manifest.has_collected_garbage(&garbage));
            let new_manifest = manifest.apply_garbage(garbage.clone()).unwrap();
            assert!(new_manifest.has_collected_garbage(&garbage));
            eprintln!("manifest.setsum = {}", manifest.setsum.hexdigest());
            eprintln!("new_manifest.setsum = {}", new_manifest.setsum.hexdigest());
            eprintln!("dropped = {}", dropped.hexdigest());
            assert_eq!(manifest.setsum - dropped, new_manifest.setsum, "manifest.setsum mismatch");
            assert_eq!(manifest.collected + dropped, new_manifest.collected, "manifest.collected mismatch");
            assert!(new_manifest.scrub().is_ok(), "scrub error");
        }
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1, .. ProptestConfig::default()
    })]

    #[test]
    fn manifests_with_snapshots_garbage(deltas in proptest::collection::vec(FragmentDelta::arbitrary(), 1000), snapshot_rollover_threshold in 2..20usize, fragment_rollover_threshold in 2..20usize) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let storage = rt.block_on(s3_client_for_test_with_new_bucket());
        let throttle = ThrottleOptions::default();
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
        eprintln!("starting manifest = {manifest:#?}");
        let start = manifest.oldest_timestamp().unwrap();
        let limit = manifest.newest_timestamp().unwrap();
        let cache = TestingSnapshotCache {
            snapshots: snapshots.clone(),
        };
        for offset in start.offset()..limit.offset() {
            let position = LogPosition::from_offset(offset);
            let garbage = rt.block_on(Garbage::new(&storage, "manifests_with_snapshots_gargage", &manifest, &throttle, &cache, position)).unwrap();
            eprintln!("garbage = {garbage:#?}");
            let dropped = garbage.scrub().unwrap();
            assert!(garbage.is_empty() || !manifest.has_collected_garbage(&garbage));
            let new_manifest = manifest.apply_garbage(garbage.clone()).unwrap();
            assert!(new_manifest.has_collected_garbage(&garbage));
            eprintln!("manifest.setsum = {}", manifest.setsum.hexdigest());
            eprintln!("new_manifest.setsum = {}", new_manifest.setsum.hexdigest());
            eprintln!("dropped = {}", dropped.hexdigest());
            assert_eq!(manifest.setsum - dropped, new_manifest.setsum, "manifest.setsum mismatch");
            assert_eq!(manifest.collected + dropped, new_manifest.collected, "manifest.collected mismatch");
            assert!(new_manifest.scrub().is_ok(), "scrub error");
        }
    }
}
