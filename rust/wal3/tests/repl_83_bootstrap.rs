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
async fn repl_83_bootstrap() {
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
    let prefix = format!("repl_83_bootstrap/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_83_bootstrap writer";
    let mark_dirty = ();
    let first_record_offset_position = LogPosition::from_offset(42);

    let mut messages = Vec::with_capacity(1000);
    for i in 0..100 {
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
    }

    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
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
    assert_eq!(1, scan.len(), "should have 1 fragment");

    let (_, records, _, _) = reader
        .read_parquet(&scan[0])
        .await
        .expect("read_parquet should succeed");
    for (returned, expected) in std::iter::zip(records, messages) {
        assert_eq!(returned.1, expected, "record content should match");
    }

    println!("repl_83_bootstrap: passed, log_id={}", log_id);
}
