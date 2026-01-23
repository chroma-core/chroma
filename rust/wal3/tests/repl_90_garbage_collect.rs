use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Cursor, CursorName, CursorStore, CursorStoreOptions, Error,
    GarbageCollectionOptions, LogPosition, LogWriter, LogWriterOptions, SnapshotOptions,
    StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_90_garbage_collect() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_90_garbage_collect/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer = "repl_90_garbage_collect writer";

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

    let log =
        LogWriter::open_or_initialize(options, writer, fragment_factory, manifest_factory, None)
            .await
            .expect("LogWriter::open_or_initialize should succeed");

    let mut position1 = LogPosition::default();
    let mut position2 = LogPosition::default();

    for i in 0..100 {
        let mut messages = Vec::with_capacity(1000);
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        position2 = log
            .append_many(messages)
            .await
            .expect("append_many should succeed");
        if i == 50 {
            position1 = position2;
        }
    }

    // Garbage collection should fail without a cursor.
    let res = log
        .garbage_collect(&GarbageCollectionOptions::default(), None)
        .await;
    assert!(
        matches!(res, Err(Error::NoSuchCursor(_))),
        "garbage_collect should fail without cursor"
    );

    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage.clone()),
        prefix.clone(),
        writer.to_string(),
    );
    let witness = cursors
        .init(
            &CursorName::new("so_you_may_gc").expect("cursor name"),
            Cursor {
                position: position1,
                epoch_us: position1.offset(),
                writer: writer.to_string(),
            },
        )
        .await
        .expect("cursor init should succeed");

    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .expect("garbage_collect should succeed after cursor init");

    cursors
        .save(
            &CursorName::new("so_you_may_gc").expect("cursor name"),
            &Cursor {
                position: position2,
                epoch_us: position2.offset(),
                writer: writer.to_string(),
            },
            &witness,
        )
        .await
        .expect("cursor save should succeed");

    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .expect("garbage_collect should succeed after cursor update");

    println!("repl_90_garbage_collect: passed, log_id={}", log_id);
}
