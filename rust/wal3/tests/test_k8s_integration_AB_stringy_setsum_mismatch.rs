//! This test covers a case where the root snapshot to be installed also exists as a snapshot to
//! remove.  In the buggy case, this would yield a 0000 != XXXX error because the setsum to discard
//! exactly matched the root.

use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, FragmentSeqNo, GarbageCollectionOptions, LogPosition, LogWriter,
    LogWriterOptions, Manifest,
};

mod common;

use common::{
    assert_conditions, Condition, FragmentCondition, GarbageCondition, ManifestCondition,
    SnapshotCondition,
};

#[tokio::test]
async fn test_k8s_integration_ab_stringy_setsum_mismatch() {
    // Appending to an initialized log should succeed and if you append enough, it should create a
    // snapshot.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_AB_stringy_setsum_mismatch",
        "init",
    )
    .await
    .unwrap();
    let preconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "init".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    assert_conditions(
        &storage,
        "test_k8s_integration_AB_stringy_setsum_mismatch",
        &preconditions,
    )
    .await;
    let mut options = LogWriterOptions::default();
    options.snapshot_manifest.fragment_rollover_threshold = 2;
    options.snapshot_manifest.snapshot_rollover_threshold = 2;
    let log = LogWriter::open(
        options,
        Arc::clone(&storage),
        "test_k8s_integration_AB_stringy_setsum_mismatch",
        "test writer",
        (),
    )
    .await
    .unwrap();
    let position1 = log.append(vec![10, 11, 12, 13]).await.unwrap();
    let fragment1 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
        seq_no: FragmentSeqNo(1),
        start: 1,
        limit: 2,
        num_bytes: 1044,
        data: vec![(position1, vec![10, 11, 12, 13])],
    };
    let position2 = log.append(vec![20, 21, 22, 23]).await.unwrap();
    let fragment2 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000002.parquet".to_string(),
        seq_no: FragmentSeqNo(2),
        start: 2,
        limit: 3,
        num_bytes: 1044,
        data: vec![(position2, vec![20, 21, 22, 23])],
    };
    let position3 = log.append(vec![30, 31, 32, 33]).await.unwrap();
    let fragment3 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000003.parquet".to_string(),
        seq_no: FragmentSeqNo(3),
        start: 3,
        limit: 4,
        num_bytes: 1044,
        data: vec![(position3, vec![30, 31, 32, 33])],
    };
    let position4 = log.append(vec![40, 41, 42, 43]).await.unwrap();
    let fragment4 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000004.parquet".to_string(),
        seq_no: FragmentSeqNo(4),
        start: 4,
        limit: 5,
        num_bytes: 1044,
        data: vec![(position4, vec![40, 41, 42, 43])],
    };
    log.cursors(Default::default())
        .unwrap()
        .init(
            &CursorName::new("testing").unwrap(),
            Cursor {
                // Overridden with position2 below.
                position: position3,
                epoch_us: 0,
                writer: "testing".to_string(),
            },
        )
        .await
        .unwrap();
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 4176,
            writer: "test writer".to_string(),
            snapshots: vec![SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(3),
                num_bytes: 2088,
                writer: "test writer".to_string(),
                snapshots: vec![],
                fragments: vec![fragment1.clone(), fragment2.clone()],
            }],
            fragments: vec![fragment3.clone(), fragment4.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
        Condition::Fragment(fragment3.clone()),
        Condition::Fragment(fragment4.clone()),
    ];
    assert_conditions(
        &storage,
        "test_k8s_integration_AB_stringy_setsum_mismatch",
        &postconditions,
    )
    .await;
    assert!(log
        .garbage_collect_phase1_compute_garbage(
            &GarbageCollectionOptions::default(),
            Some(position2)
        )
        .await
        .unwrap());
    log.garbage_collect_phase2_update_manifest(&GarbageCollectionOptions::default())
        .await
        .unwrap();
    let postconditions = [
        Condition::Garbage(GarbageCondition {
            first_to_keep: LogPosition::from_offset(2),
            fragments_to_drop_start: FragmentSeqNo(1),
            fragments_to_drop_limit: FragmentSeqNo(2),
            snapshot_for_root: Some(SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(2),
                limit: LogPosition::from_offset(3),
                num_bytes: 1044,
                writer: "test writer".to_string(),
                snapshots: vec![SnapshotCondition {
                    depth: 1,
                    start: LogPosition::from_offset(2),
                    limit: LogPosition::from_offset(3),
                    num_bytes: 1044,
                    writer: "test writer".to_string(),
                    snapshots: vec![],
                    fragments: vec![fragment2.clone()],
                }],
                fragments: vec![],
            }),
            snapshots_to_drop: vec![SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(3),
                num_bytes: 2088,
                writer: "test writer".to_string(),
                snapshots: vec![],
                fragments: vec![fragment1.clone(), fragment2.clone()],
            }],
            snapshots_to_make: vec![SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(2),
                limit: LogPosition::from_offset(3),
                num_bytes: 1044,
                writer: "garbage collection".to_string(),
                snapshots: vec![],
                fragments: vec![fragment2.clone()],
            }],
        }),
        Condition::Manifest(ManifestCondition {
            acc_bytes: 4176,
            writer: "test writer".to_string(),
            snapshots: vec![SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(2),
                limit: LogPosition::from_offset(3),
                num_bytes: 1044,
                writer: "garbage collection".to_string(),
                snapshots: vec![],
                fragments: vec![fragment2.clone()],
            }],
            fragments: vec![fragment3.clone(), fragment4.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
        Condition::Fragment(fragment3.clone()),
        Condition::Fragment(fragment4.clone()),
    ];
    assert_conditions(
        &storage,
        "test_k8s_integration_AB_stringy_setsum_mismatch",
        &postconditions,
    )
    .await;
    log.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default())
        .await
        .unwrap();
}
