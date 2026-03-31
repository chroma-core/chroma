use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    create_s3_factories, now_micros, upload_parquet, FragmentIdentifier, FragmentSeqNo,
    LogPosition, LogReaderOptions, LogWriter, LogWriterOptions,
};

mod common;

#[tokio::test]
async fn test_k8s_integration_07_open_or_initialize_recovers_orphan() {
    // open_or_initialize must handle the "no manifest, but one orphan fragment exists" state.
    // The first post-init open repairs the orphan and returns LogContentionRetry; the helper must
    // continue through that transition instead of surfacing the retry.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_07_open_or_initialize_recovers_orphan";
    let writer = "test writer";

    let position = LogPosition::from_offset(1);
    upload_parquet(
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
    let log =
        LogWriter::open_or_initialize(options, writer, fragment_factory, manifest_factory, None)
            .await
            .expect("open_or_initialize should recover a single orphan fragment");

    let next_position = log.append(vec![81, 82, 83, 84]).await.unwrap();
    assert_eq!(LogPosition::from_offset(2), next_position);
}
