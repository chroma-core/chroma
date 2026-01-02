use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, PutOptions};
use uuid::Uuid;

use wal3::{
    create_repl_factories, Error, LogWriter, LogWriterOptions, ManifestManager, SnapshotOptions,
    StorageWrapper, ThrottleOptions,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn repl_97_destroy_wedge() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_97_destroy_wedge/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_97_destroy_wedge writer";

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = LogWriter::open_or_initialize(
        options.clone(),
        Arc::new(storage.clone()),
        &prefix,
        writer,
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open_or_initialize should succeed");

    for i in 0..100 {
        let mut messages = Vec::with_capacity(1000);
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        log.append_many(messages)
            .await
            .expect("append_many should succeed");
    }

    // Put an unexpected file in the log directory to trigger the wedge.
    Arc::new(storage.clone())
        .put_bytes(
            &format!("{}/log/foo", prefix),
            Vec::from("CONTENT".to_string()),
            PutOptions::default(),
        )
        .await
        .expect("put_bytes should succeed");

    // Use S3-based ManifestManager for destroy operation (destroy is storage-level).
    let manifest_manager = ManifestManager::new(
        ThrottleOptions::default(),
        options.snapshot_manifest,
        Arc::new(storage.clone()),
        prefix.clone(),
        writer.to_string(),
        Arc::new(()),
        Arc::new(()),
    )
    .await
    .expect("ManifestManager::new should succeed");

    let result = wal3::destroy(Arc::new(storage), &prefix, &manifest_manager).await;
    assert!(
        matches!(result, Err(Error::GarbageCollection(_))),
        "destroy should fail with GarbageCollection error due to unexpected file, got: {:?}",
        result
    );

    println!("repl_97_destroy_wedge: passed, log_id={}", log_id);
}
