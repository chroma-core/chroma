use std::sync::Arc;

use chroma_storage::{s3_client_for_test_with_new_bucket, PutOptions};
use uuid::Uuid;

use wal3::{
    create_repl_factories, unprefixed_fragment_path, FragmentIdentifier, FragmentSeqNo, LogWriter,
    LogWriterOptions, ManifestManagerFactory, SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_97_destroy() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_97_destroy/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_97_destroy writer";

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
        0,
        storages,
        Arc::clone(&client),
        vec!["test-region".to_string()],
        log_id,
    );

    let log = LogWriter::open_or_initialize(
        options.clone(),
        writer,
        fragment_factory,
        manifest_factory.clone(),
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

    // Put an extra fragment file that should be cleaned up during destroy.
    Arc::new(storage.clone())
        .put_bytes(
            &format!(
                "{}/{}",
                prefix,
                unprefixed_fragment_path(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(
                    100_000
                )))
            ),
            Vec::from("CONTENT".to_string()),
            PutOptions::default(),
        )
        .await
        .expect("put_bytes should succeed");

    let manifest_manager = manifest_factory
        .open_publisher()
        .await
        .expect("open_publisher succeed");

    wal3::destroy(Arc::new(storage), &prefix, &manifest_manager)
        .await
        .expect("destroy should succeed");

    println!("repl_97_destroy: passed, log_id={}", log_id);
}
