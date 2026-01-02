use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Cursor, CursorName, CursorStoreOptions, FragmentManagerFactory,
    GarbageCollectionOptions, Limits, LogPosition, LogReader, LogReaderOptions, LogWriter,
    LogWriterOptions, ManifestManagerFactory, ReplicatedManifestManagerFactory, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn repl_82_copy_empty_log_initializes() {
    // Test copying an empty-after-gc log initializes correctly.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_82_copy_empty_log_initializes_source/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        Arc::clone(&storages),
        Arc::clone(&client),
        log_id,
    );

    let log = LogWriter::open_or_initialize(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open_or_initialize should succeed");

    let mut position: LogPosition = LogPosition::default();
    for i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for j in 0..10 {
            batch.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        position = log
            .append_many(batch)
            .await
            .expect("append_many should succeed")
            + 10u64;
    }

    let cursors = log.cursors(CursorStoreOptions::default()).unwrap();
    cursors
        .init(
            &CursorName::new("writer").unwrap(),
            Cursor {
                position,
                epoch_us: 42,
                writer: "unit tests".to_string(),
            },
        )
        .await
        .expect("cursor init should succeed");

    eprintln!("kicking off gc");
    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .expect("garbage_collect should succeed");
    eprintln!("gc finished");

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
        prefix.clone(),
    )
    .await
    .expect("LogReader::open should succeed");

    let scrubbed_source = reader
        .scrub(Limits::default())
        .await
        .expect("scrub should succeed");

    // Copy to target using repl factories.
    let target_log_id = Uuid::new_v4();
    let target_prefix = format!(
        "repl_82_copy_empty_log_initializes_target/{}",
        target_log_id
    );
    let target_manifest_factory =
        ReplicatedManifestManagerFactory::new(Arc::clone(&client), target_log_id);

    wal3::copy(
        &Arc::new(storage.clone()),
        &reader,
        LogPosition::default(),
        target_prefix.clone(),
        target_manifest_factory,
    )
    .await
    .expect("copy should succeed");

    // Scrub the copy using repl factories.
    let target_wrapper = StorageWrapper::new(
        "test-region".to_string(),
        storage.clone(),
        target_prefix.clone(),
    );
    let target_storages = Arc::new(vec![target_wrapper]);
    let (target_fragment_factory, target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        target_storages,
        Arc::clone(&client),
        target_log_id,
    );
    let target_fragment_consumer = target_fragment_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let target_manifest_consumer = target_manifest_factory
        .make_consumer()
        .await
        .expect("make_consumer should succeed");
    let copied = LogReader::open(
        LogReaderOptions::default(),
        target_fragment_consumer,
        target_manifest_consumer,
        target_prefix.clone(),
    )
    .await
    .expect("LogReader::open for target should succeed");

    let scrubbed_target = copied
        .scrub(Limits::default())
        .await
        .expect("target scrub should succeed");

    assert_eq!(
        scrubbed_source.calculated_setsum, scrubbed_target.calculated_setsum,
        "source and target setsums should match"
    );

    let before_mani = reader
        .manifest()
        .await
        .expect("source manifest")
        .expect("manifest exists");
    let after_mani = copied
        .manifest()
        .await
        .expect("target manifest")
        .expect("manifest exists");

    eprintln!("{before_mani:#?}");
    eprintln!("{after_mani:#?}");

    assert_eq!(
        before_mani.oldest_timestamp(),
        before_mani.next_write_timestamp(),
        "source oldest should equal next_write (empty after GC)"
    );
    assert_eq!(
        before_mani.oldest_timestamp(),
        after_mani.oldest_timestamp(),
        "oldest timestamps should match"
    );
    assert_eq!(
        before_mani.next_write_timestamp(),
        after_mani.next_write_timestamp(),
        "next_write timestamps should match"
    );

    // Note: next_fragment_seq_no comparison may differ for repl (UUID-based) vs S3 (sequential).
    // We verify the manifests are consistent but don't require exact seq_no match.
    println!(
        "repl_82_copy_empty_log_initializes: passed, log_id={}, setsum={}",
        log_id,
        scrubbed_source.calculated_setsum.hexdigest()
    );
}
