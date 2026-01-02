use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, PutOptions};

use wal3::{
    create_s3_factories, Error, LogReaderOptions, LogWriter, LogWriterOptions, ManifestManager,
    SnapshotOptions, ThrottleOptions,
};

#[tokio::test]
async fn test_k8s_integration_97_destroy_wedge() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_97_destroy";
    const WRITER: &str = "test_k8s_integration_97_destroy writer";
    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    let log = LogWriter::open_or_initialize(
        options.clone(),
        Arc::clone(&storage),
        PREFIX,
        WRITER,
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .unwrap();

    for i in 0..100 {
        let mut messages = Vec::with_capacity(1000);
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(messages).await.unwrap();
    }

    storage
        .put_bytes(
            &format!("{}/log/foo", PREFIX),
            Vec::from("CONTENT".to_string()),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let manifest_manager = ManifestManager::new(
        ThrottleOptions::default(),
        options.snapshot_manifest,
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
        Arc::new(()),
        Arc::new(()),
    )
    .await
    .unwrap();

    assert!(matches!(
        wal3::destroy(storage, PREFIX, &manifest_manager)
            .await
            .unwrap_err(),
        Error::GarbageCollection(_)
    ));
}
