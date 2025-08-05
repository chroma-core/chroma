use std::sync::{Arc, Mutex};

use setsum::Setsum;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    unprefixed_snapshot_path, Error, Fragment, FragmentSeqNo, Garbage, LogPosition, Snapshot,
    SnapshotCache, SnapshotPointer, ThrottleOptions,
};

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
    let overall_setsum =
        Setsum::from_hexdigest("00000000aaaaaaaabbbbbbbb0000000000000000000000000000000000000000")
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
async fn test_k8s_integration_replace_snapshot_triggers_to_split_case_one_level() {
    // Set up test data that will trigger the to_split case
    let (nested_ptr, _, cache) = create_snapshot_for_split_test();

    // Set cutoff at position 10, which should trigger splitting the nested snapshot
    // that spans from 8 to 15
    let mut first_to_keep = LogPosition::from_offset(10);

    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    // This should trigger the to_split case in replace_snapshot
    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "replace-snapshot",
            &nested_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    assert_eq!(
        Setsum::from_hexdigest("00000000aaaaaaaa000000000000000000000000000000000000000000000000")
            .unwrap(),
        dropped_setsum,
    );

    let paths_to_delete: Vec<String> = garbage
        .prefixed_paths_to_delete("replace-snapshot")
        .collect();
    let mut sorted_paths = paths_to_delete;
    sorted_paths.sort();
    assert_eq!(
        vec!["replace-snapshot/log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet", "replace-snapshot/snapshot/SNAPSHOT.00000000aaaaaaaabbbbbbbb0000000000000000000000000000000000000000"],
        sorted_paths
    );

    assert_eq!(1, garbage.snapshots_to_make.len());
    assert_eq!(
        Snapshot {
            path:
                "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
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
        garbage.snapshots_to_make[0].clone(),
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
async fn test_k8s_integration_replace_snapshot_triggers_to_split_case_two_level() {
    // Set up test data that will trigger the to_split case
    let (parent_ptr, _parent_snapshot, cache) = create_nested_snapshot_for_split_test(1);

    // Set cutoff at position 10, which should trigger splitting the nested snapshot
    // that spans from 8 to 15
    let mut first_to_keep = LogPosition::from_offset(10);

    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    // This should trigger the to_split case in replace_snapshot
    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    assert_eq!(
        Setsum::from_hexdigest("00000000aaaaaaaa000000000000000000000000000000000000000000000000")
            .unwrap(),
        dropped_setsum,
    );

    assert_eq!(1, garbage.snapshots_to_make.len());
    assert_eq!(
        Snapshot {
            path:
                "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
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
        garbage.snapshots_to_make[0].clone(),
    );

    let paths_to_delete: Vec<String> = garbage
        .prefixed_paths_to_delete("replace-snapshot")
        .collect();
    let mut sorted_paths = paths_to_delete;
    sorted_paths.sort();
    assert_eq!(
        vec!["replace-snapshot/log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet", "replace-snapshot/snapshot/SNAPSHOT.00000000aaaaaaaabbbbbbbb0000000000000000000000000000000000000000"],
        sorted_paths
    );
}

#[tokio::test]
async fn test_k8s_integration_replace_snapshot_triggers_to_split_case_three_level() {
    // Set up test data that will trigger the to_split case
    let (parent_ptr, _parent_snapshot, cache) = create_nested_snapshot_for_split_test(2);

    // Set cutoff at position 10, which should trigger splitting the nested snapshot
    // that spans from 8 to 15
    let mut first_to_keep = LogPosition::from_offset(10);

    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    // This should trigger the to_split case in replace_snapshot
    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "replace-snapshot",
            &parent_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    assert_eq!(
        Setsum::from_hexdigest("00000000aaaaaaaa000000000000000000000000000000000000000000000000")
            .unwrap(),
        dropped_setsum,
    );

    let paths_to_delete: Vec<String> = garbage
        .prefixed_paths_to_delete("replace-snapshot")
        .collect();
    let mut sorted_paths = paths_to_delete;
    sorted_paths.sort();
    assert_eq!(
        vec!["replace-snapshot/log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet", "replace-snapshot/snapshot/SNAPSHOT.00000000aaaaaaaabbbbbbbb0000000000000000000000000000000000000000"],
        sorted_paths
    );

    assert_eq!(1, garbage.snapshots_to_make.len());
    assert_eq!(
        Snapshot {
            path:
                "snapshot/SNAPSHOT.0000000000000000bbbbbbbb0000000000000000000000000000000000000000"
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
        garbage.snapshots_to_make[0].clone(),
    );
}

#[test]
fn test_k8s_integration_test_k8s_integration_drop_frag() {
    let setsum =
        Setsum::from_hexdigest("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .unwrap();
    let fragment = create_fragment(10, 20, FragmentSeqNo(1), setsum);

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep: LogPosition::from_offset(10),
    };

    let dropped_setsum = garbage
        .drop_fragment(&fragment, &mut true, &mut LogPosition::default())
        .unwrap();

    // Should return the same setsum
    assert_eq!(setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(2));
    assert_eq!(garbage.setsum_to_discard, setsum);
}

#[tokio::test]
async fn test_k8s_integration_drop_snapshot() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let cache = MockSnapshotCache::default();

    // Create a snapshot with nested snapshots and fragments
    let frag1_setsum =
        Setsum::from_hexdigest("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let frag2_setsum =
        Setsum::from_hexdigest("2222222222222222222222222222222222222222222222222222222222222222")
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

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep: LogPosition::from_offset(35),
    };

    let dropped_setsum = garbage
        .drop_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut true,
            &mut LogPosition::default(),
        )
        .await
        .unwrap();

    // Should return the total setsum
    assert_eq!(total_setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(3));
    assert_eq!(garbage.snapshots_to_drop.len(), 2);
    assert_eq!(garbage.snapshots_to_drop[0], nested_snapshot.to_pointer());
    assert_eq!(garbage.snapshots_to_drop[1], snapshot_ptr);
}

#[tokio::test]
async fn test_k8s_integration_replace_snapshot_flat() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let cache = MockSnapshotCache::default();

    // Create fragments with different ranges
    let frag1_setsum =
        Setsum::from_hexdigest("1111111100000000000000000000000000000000000000000000000000000000")
            .unwrap();
    let frag2_setsum =
        Setsum::from_hexdigest("0000000022222222000000000000000000000000000000000000000000000000")
            .unwrap();
    let frag3_setsum =
        Setsum::from_hexdigest("0000000000000000333333330000000000000000000000000000000000000000")
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
    let mut first_to_keep = LogPosition::from_offset(10); // Keep fragments starting from offset 10

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    // Should return the setsum of the dropped fragment
    assert_eq!(frag1_setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(2));
    assert_eq!(garbage.snapshots_to_make.len(), 1);

    // Check the new snapshot contains the kept fragments
    let new_snapshot = &garbage.snapshots_to_make[0];
    assert_eq!(new_snapshot.fragments.len(), 2);
    assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);
    assert!(new_snapshot.fragments.contains(&fragment2));
    assert!(new_snapshot.fragments.contains(&fragment3));
}

#[tokio::test]
async fn test_k8s_integration_replace_snapshot_drops_snapshots_prior_to_cutoff() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let cache = MockSnapshotCache::default();

    // Create two child snapshots: one before cutoff (to be dropped), one after (to be kept)
    let frag1_setsum =
        Setsum::from_hexdigest("1111111100000000000000000000000000000000000000000000000000000000")
            .unwrap();
    let frag2_setsum =
        Setsum::from_hexdigest("0000000022222222000000000000000000000000000000000000000000000000")
            .unwrap();
    let frag3_setsum =
        Setsum::from_hexdigest("0000000000000000333333330000000000000000000000000000000000000000")
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
    let mut first_to_keep = LogPosition::from_offset(12); // Keep snapshots starting from offset 12

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    // Should return the setsum of the dropped snapshot
    assert_eq!(frag1_setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(2));
    assert_eq!(garbage.snapshots_to_make.len(), 1);

    // Check the new snapshot contains the kept child snapshot and parent fragment
    let new_snapshot = &garbage.snapshots_to_make[0];
    assert_eq!(new_snapshot.snapshots.len(), 1);
    assert_eq!(new_snapshot.fragments.len(), 1);
    assert_eq!(new_snapshot.snapshots[0].setsum, frag2_setsum);
    assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);
}

#[tokio::test]
async fn test_k8s_integration_replace_snapshot_drops_fragments_prior_to_cutoff() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let cache = MockSnapshotCache::default();

    // Create fragments: some before cutoff (to be dropped), some after (to be kept)
    let frag1_setsum =
        Setsum::from_hexdigest("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let frag2_setsum =
        Setsum::from_hexdigest("2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();
    let frag3_setsum =
        Setsum::from_hexdigest("3333333333333333333333333333333333333333333333333333333333333333")
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
    let mut first_to_keep = LogPosition::from_offset(12); // Keep fragments starting from offset 12

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    // Should return the setsum of the dropped fragments
    assert_eq!(frag1_setsum + frag2_setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(3));
    assert_eq!(garbage.snapshots_to_make.len(), 1);

    // Check the new snapshot contains only the kept fragment
    let new_snapshot = &garbage.snapshots_to_make[0];
    assert_eq!(new_snapshot.fragments.len(), 1);
    assert_eq!(new_snapshot.fragments[0].setsum, frag3_setsum);
    assert_eq!(new_snapshot.setsum, frag3_setsum);
}

#[tokio::test]
async fn test_k8s_integration_replace_snapshot_two_levels_rightmost_leaf() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let cache = MockSnapshotCache::default();

    // Create fragments for leaf snapshots
    let frag1_setsum =
        Setsum::from_hexdigest("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let frag2_setsum =
        Setsum::from_hexdigest("2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();
    let frag3_setsum =
        Setsum::from_hexdigest("3333333333333333333333333333333333333333333333333333333333333333")
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
    let mut first_to_keep = LogPosition::from_offset(12); // Keep snapshots starting from offset 12

    let mut garbage = Garbage {
        snapshots_to_drop: vec![],
        snapshots_to_make: vec![],
        snapshot_for_root: None,
        fragments_to_drop_start: FragmentSeqNo(1),
        fragments_to_drop_limit: FragmentSeqNo(1),
        setsum_to_discard: Setsum::default(),
        first_to_keep,
    };

    let dropped_setsum = garbage
        .replace_snapshot(
            &storage,
            "test-prefix",
            &snapshot_ptr,
            &ThrottleOptions::default(),
            &cache,
            &mut first_to_keep,
            &mut true,
        )
        .await
        .unwrap()
        .0;

    // Should return the setsum of the dropped left leaf
    assert_eq!(frag1_setsum, dropped_setsum);

    // Test the garbage structure
    assert_eq!(garbage.fragments_to_drop_limit, FragmentSeqNo(2));
    assert_eq!(garbage.snapshots_to_make.len(), 1);

    // Check the new snapshot contains the right-most (kept) leaf snapshot and interior fragment
    let new_snapshot = &garbage.snapshots_to_make[0];
    assert_eq!(new_snapshot.snapshots.len(), 1);
    assert_eq!(new_snapshot.fragments.len(), 1);
    assert_eq!(new_snapshot.snapshots[0].setsum, frag2_setsum);
    assert_eq!(new_snapshot.setsum, frag2_setsum + frag3_setsum);
}
