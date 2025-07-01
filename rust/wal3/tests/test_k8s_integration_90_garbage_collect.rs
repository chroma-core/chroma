use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, Error, GarbageCollectionOptions,
    LogPosition, LogWriter, LogWriterOptions, SnapshotOptions,
};

#[tokio::test]
async fn test_k8s_integration_90_garbage_collect() {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    const PREFIX: &str = "test_k8s_integration_90_garbage_collect";
    const WRITER: &str = "test_k8s_integration_90_garbage_collect writer";
    let log = LogWriter::open_or_initialize(
        LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        },
        Arc::clone(&storage),
        PREFIX,
        WRITER,
        (),
    )
    .await
    .unwrap();
    let mut position1 = LogPosition::default();
    let mut position2 = LogPosition::default();

    for i in 0..100 {
        let mut messages = Vec::with_capacity(1000);
        for j in 0..10 {
            messages.push(Vec::from(format!("key:i={},j={}", i, j)));
        }
        position2 = log.append_many(messages).await.unwrap();
        if i == 50 {
            position1 = position2;
        }
    }

    let res = log
        .garbage_collect(&GarbageCollectionOptions::default(), None)
        .await;
    assert!(matches!(res, Err(Error::NoSuchCursor(_))));

    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::clone(&storage),
        PREFIX.to_string(),
        WRITER.to_string(),
    );
    let witness = cursors
        .init(
            &CursorName::new("so_you_may_gc").unwrap(),
            Cursor {
                position: position1,
                epoch_us: position1.offset(),
                writer: WRITER.to_string(),
            },
        )
        .await
        .unwrap();

    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap();

    cursors
        .save(
            &CursorName::new("so_you_may_gc").unwrap(),
            &Cursor {
                position: position2,
                epoch_us: position2.offset(),
                writer: WRITER.to_string(),
            },
            &witness,
        )
        .await
        .unwrap();

    log.garbage_collect(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap();
}
