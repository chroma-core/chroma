use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, now_micros, upload_parquet, FragmentIdentifier, FragmentManagerFactory,
    FragmentSeqNo, LogPosition, LogReaderOptions, LogWriter, LogWriterOptions, Manifest,
    ManifestManagerFactory,
};

mod common;

use common::{assert_conditions, Condition, FragmentCondition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_05_crash_safety_initialize_fails() {
    // Appending to a log that has failed to write its manifest fails with log contention.
    // Subsequent writes will repair the log and continue to make progress.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_05_crash_safety_initialize_fails";
    let writer = "test writer";
    let (init_fragment_factory, init_manifest_factory) = create_s3_factories(
        LogWriterOptions::default(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        "init".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let fragment_publisher = init_fragment_factory.make_publisher().await.unwrap();
    init_manifest_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .unwrap();
    let position = LogPosition::from_offset(1);
    let (path, _setsum, size) = upload_parquet(
        &LogWriterOptions::default(),
        &storage,
        prefix,
        FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
        Some(position),
        vec![vec![42, 43, 44, 45]],
        None,
        now_micros(),
    )
    .await
    .unwrap();
    assert_eq!(
        path,
        "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet"
    );
    let fragment1 = FragmentCondition {
        path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet".to_string(),
        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: 1,
            limit: 2,
            num_bytes: size,
            data: vec![(position, vec![42, 43, 44, 45])],
        }),
    ];
    assert_conditions(&fragment_publisher, &conditions).await;
    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.to_string(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log = LogWriter::open(options, writer, fragment_factory, manifest_factory, None)
        .await
        .unwrap();
    // The log contention will be transparently sorted out.
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
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
        }),
        Condition::Fragment(fragment1.clone()),
        Condition::Fragment(fragment2.clone()),
    ];
    assert_conditions(&fragment_publisher, &postconditions).await;
}
