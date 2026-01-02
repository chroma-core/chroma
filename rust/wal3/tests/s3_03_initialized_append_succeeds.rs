use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, FragmentIdentifier, FragmentSeqNo, LogReaderOptions, LogWriter,
    LogWriterOptions, Manifest, ManifestManagerFactory, S3ManifestManagerFactory,
};

mod common;

use common::{assert_conditions, Condition, FragmentCondition, ManifestCondition};

#[tokio::test]
async fn test_k8s_integration_03_initialized_append_succeeds() {
    // Appending to an initialized log should succeed.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_03_initialized_append_succeeds";
    let writer = "test writer";
    let init_factory = S3ManifestManagerFactory {
        write: LogWriterOptions::default(),
        read: LogReaderOptions::default(),
        storage: Arc::clone(&storage),
        prefix: prefix.to_string(),
        writer: "init".to_string(),
        mark_dirty: Arc::new(()),
        snapshot_cache: Arc::new(()),
    };
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .unwrap();
    let preconditions = [Condition::Manifest(ManifestCondition {
        acc_bytes: 0,
        writer: "init".to_string(),
        snapshots: vec![],
        fragments: vec![],
    })];
    assert_conditions(&storage, prefix, &preconditions).await;
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
        Condition::Fragment(fragment1),
    ];
    assert_conditions(&storage, prefix, &postconditions).await;
}
