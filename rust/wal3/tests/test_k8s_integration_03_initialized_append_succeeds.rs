use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{FragmentSeqNo, LogWriter, LogWriterOptions, Manifest};

mod common;

use common::{assert_conditions, Condition, FragmentCondition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_03_initialized_append_succeeds() {
    // Appending to an initialized log should succeed.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    Manifest::initialize(
        &LogWriterOptions::default(),
        &storage,
        "test_k8s_integration_03_initialized_append_succeeds",
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
        "test_k8s_integration_03_initialized_append_succeeds",
        &preconditions,
    )
    .await;
    let log = LogWriter::open(
        LogWriterOptions::default(),
        Arc::clone(&storage),
        "test_k8s_integration_03_initialized_append_succeeds",
        "test writer",
        (),
    )
    .await
    .unwrap();
    let position = log.append(vec![42, 43, 44, 45]).await.unwrap();
    let fragment1 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
        seq_no: FragmentSeqNo(1),
        start: 1,
        limit: 2,
        num_bytes: 1044,
        data: vec![(position, vec![42, 43, 44, 45])],
    };
    let postconditions = [
        Condition::Manifest(ManifestCondition {
            acc_bytes: 1044,
            writer: "test writer".to_string(),
            snapshots: vec![],
            fragments: vec![fragment1.clone()],
        }),
        Condition::Fragment(fragment1),
    ];
    assert_conditions(
        &storage,
        "test_k8s_integration_03_initialized_append_succeeds",
        &postconditions,
    )
    .await;
}
