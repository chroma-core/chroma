use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, FragmentManagerFactory, Limits, LogPosition, LogReader,
    LogReaderOptions, LogWriter, LogWriterOptions, ManifestManagerFactory, SnapshotOptions,
    StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_84_bootstrap_empty() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_84_bootstrap_empty/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_84_bootstrap writer";
    let mark_dirty = ();
    let first_record_offset_position = LogPosition::from_offset(42);
    let messages = vec![];

    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        Arc::clone(&storages),
        Arc::clone(&client),
        log_id,
    );

    LogWriter::bootstrap(
        &options,
        writer,
        mark_dirty,
        fragment_factory,
        manifest_factory,
        first_record_offset_position,
        messages.clone(),
        None,
    )
    .await
    .expect("bootstrap should succeed");

    // Open a reader using repl factories.
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let reader_storages = Arc::new(vec![wrapper]);
    let (reader_fragment_factory, reader_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        reader_storages,
        Arc::clone(&client),
        log_id,
    );
    let reader_fragment_consumer = reader_fragment_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let reader_manifest_consumer = reader_manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let reader = LogReader::open(
        LogReaderOptions::default(),
        reader_fragment_consumer,
        reader_manifest_consumer,
    )
    .await
    .expect("LogReader::open should succeed");

    let scan = reader
        .scan(LogPosition::from_offset(42), Limits::default())
        .await
        .expect("scan should succeed");
    assert_eq!(0, scan.len(), "empty bootstrap should have 0 fragments");

    reader
        .scrub(Limits {
            max_files: None,
            max_bytes: None,
            max_records: None,
        })
        .await
        .expect("scrub should succeed");

    // Open a writer on the bootstrapped log and append.
    let options2 = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 2,
            fragment_rollover_threshold: 2,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory2, manifest_factory2) = create_repl_factories(
        options2.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log_writer = LogWriter::open(
        options2,
        Arc::new(storage),
        &prefix,
        writer,
        fragment_factory2,
        manifest_factory2,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    log_writer
        .manifest_and_witness()
        .await
        .expect("manifest_and_witness should succeed")
        .manifest
        .scrub()
        .expect("manifest scrub should succeed");

    log_writer
        .append_many(vec![Vec::from("fresh-write".to_string())])
        .await
        .expect("append_many should succeed");

    let scan = reader
        .scan(LogPosition::from_offset(42), Limits::default())
        .await
        .expect("scan should succeed after write");
    assert_eq!(1, scan.len(), "should have 1 fragment after write");

    reader
        .scrub(wal3::Limits::default())
        .await
        .expect("final scrub should succeed");

    println!("repl_84_bootstrap_empty: passed, log_id={}", log_id);
}
