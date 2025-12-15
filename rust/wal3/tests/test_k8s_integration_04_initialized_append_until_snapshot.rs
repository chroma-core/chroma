use std::sync::Arc;
use std::time::Duration;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    FragmentIdentifier, FragmentPublisherFactory, FragmentSeqNo, LogPosition, LogWriter,
    LogWriterOptions, Manifest, ManifestPublisherFactory,
};

mod common;

use common::{
    assert_conditions, Condition, FragmentCondition, ManifestCondition, SnapshotCondition,
};

#[tokio::test]
async fn test_k8s_integration_04_initialized_append_until_snapshot() {
    // Appending to an initialized log should succeed and if you append enough, it should create a
    // snapshot.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_04_initialized_append_until_snapshot";
    let writer = "test writer";
    Manifest::initialize(&LogWriterOptions::default(), &storage, prefix, "init")
        .await
        .unwrap();
    let preconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "init".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    assert_conditions(&storage, prefix, &preconditions).await;
    let mut options = LogWriterOptions::default();
    options.snapshot_manifest.fragment_rollover_threshold = 1;
    options.snapshot_manifest.snapshot_rollover_threshold = 2;
    let fragment_factory = FragmentPublisherFactory {
        options: options.clone(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        mark_dirty: Arc::new(()),
    };
    let manifest_factory = ManifestPublisherFactory {
        options: options.clone(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: writer.to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    let log = LogWriter::open(
        options,
        Arc::clone(&storage),
        prefix,
        writer,
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .unwrap();
    let position = log.append(vec![42, 43, 44, 45]).await.unwrap();
    let fragment1 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
        start: 1,
        limit: 2,
        num_bytes: 1044,
        data: vec![(position, vec![42, 43, 44, 45])],
    };
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 1044,
            writer: writer.to_string(),
            snapshots: vec![],
            fragments: vec![fragment1.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
    ];
    assert_conditions(&storage, prefix, &postconditions).await;
    let position = log.append(vec![81, 82, 83, 84]).await.unwrap();
    let fragment2 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000002.parquet".to_string(),
        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
        start: 2,
        limit: 3,
        num_bytes: 1044,
        data: vec![(position, vec![81, 82, 83, 84])],
    };
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 2088,
            writer: writer.to_string(),
            snapshots: vec![SnapshotCondition {
                depth: 1,
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(2),
                num_bytes: 1044,
                writer: writer.to_string(),
                snapshots: vec![],
                fragments: vec![fragment1.clone()],
            }],
            fragments: vec![fragment2.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
    ];
    assert_conditions(&storage, prefix, &postconditions).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let position = log.append(vec![90, 91, 92, 93]).await.unwrap();
    let fragment3 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000003.parquet".to_string(),
        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3)),
        start: 3,
        limit: 4,
        num_bytes: 1044,
        data: vec![(position, vec![90, 91, 92, 93])],
    };
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 3132,
            writer: writer.to_string(),
            snapshots: vec![
                SnapshotCondition {
                    depth: 1,
                    start: LogPosition::from_offset(1),
                    limit: LogPosition::from_offset(2),
                    num_bytes: 1044,
                    writer: writer.to_string(),
                    snapshots: vec![],
                    fragments: vec![fragment1.clone()],
                },
                SnapshotCondition {
                    depth: 1,
                    start: LogPosition::from_offset(2),
                    limit: LogPosition::from_offset(3),
                    num_bytes: 1044,
                    writer: writer.to_string(),
                    snapshots: vec![],
                    fragments: vec![fragment2.clone()],
                },
            ],
            fragments: vec![fragment3.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
        Condition::Fragment(fragment3.clone()),
        // TODO(rescrv):  Add a snapshot condition here.
    ];
    assert_conditions(&storage, prefix, &postconditions).await;
}
