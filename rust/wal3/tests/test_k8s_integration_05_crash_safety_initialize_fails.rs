use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{upload_parquet, FragmentSeqNo, LogPosition, LogWriter, LogWriterOptions, Manifest};

mod common;

use common::{assert_conditions, Condition, FragmentCondition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_05_crash_safety_initialize_fails() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_05_crash_safety_initialize_fails",
        "init",
    )
    .await
    .unwrap();
    let position = LogPosition::from_offset(1);
    let (path, _setsum, size) = upload_parquet(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_05_crash_safety_initialize_fails",
        FragmentSeqNo(1),
        position,
        vec![vec![42, 43, 44, 45]],
    )
    .await
    .unwrap();
    assert_eq!(
        path,
        "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet"
    );
    let fragment1 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
        seq_no: FragmentSeqNo(1),
        start: 1,
        limit: 2,
        num_bytes: size,
        data: vec![(position, vec![42, 43, 44, 45])],
    };
    let conditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 0,
            writer: "init".to_string(),
            snapshots: vec![],
            fragments: vec![],
        }),
        Condition::Fragment(FragmentCondition {
            path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
            seq_no: FragmentSeqNo(1),
            start: 1,
            limit: 2,
            num_bytes: size,
            data: vec![(position, vec![42, 43, 44, 45])],
        }),
    ];
    assert_conditions(
        &storage,
        "test_k8s_integration_05_crash_safety_initialize_fails",
        &conditions,
    )
    .await;
    let log = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_05_crash_safety_initialize_fails",
        "test writer",
        (),
    )
    .await
    .unwrap();
    // The log contention will be transparently sorted out.
    let position = log.append(vec![81, 82, 83, 84]).await.unwrap();
    let fragment2 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000002.parquet".to_string(),
        seq_no: FragmentSeqNo(2),
        start: 2,
        limit: 3,
        num_bytes: 1044,
        data: vec![(position, vec![81, 82, 83, 84])],
    };
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 2088,
            writer: "test writer".to_string(),
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
    ];
    assert_conditions(
        &storage,
        "test_k8s_integration_05_crash_safety_initialize_fails",
        &postconditions,
    )
    .await;
}
