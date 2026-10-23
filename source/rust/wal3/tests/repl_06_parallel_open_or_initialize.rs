use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{create_repl_factories, LogWriter, LogWriterOptions, StorageWrapper};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_06_parallel_open_or_initialize() {
    // Multiple concurrent open_or_initialize calls on an uninitialized repl log should all
    // succeed. This exercises the race where one writer wins init and the others must treat
    // AlreadyInitialized as success.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();
    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_06_parallel_open_or_initialize/{log_id}");
    let storages = Arc::new(vec![StorageWrapper::new(
        "test-region".to_string(),
        storage,
        prefix,
    )]);
    let num_writers = 32;
    let done = Arc::new(AtomicBool::new(false));
    let notifier = Arc::new(tokio::sync::Notify::new());
    let mut handles = Vec::with_capacity(num_writers);

    for i in 0..num_writers {
        let client = Arc::clone(&client);
        let storages = Arc::clone(&storages);
        let done = Arc::clone(&done);
        let notifier = Arc::clone(&notifier);
        handles.push(tokio::spawn(async move {
            let writer_name = format!("writer{i}");
            let (fragment_factory, manifest_factory) = create_repl_factories(
                LogWriterOptions::default(),
                default_repl_options(),
                0,
                storages,
                client,
                vec!["test-region".to_string()],
                log_id,
            );
            if !done.load(Ordering::Relaxed) {
                notifier.notified().await;
            }
            notifier.notify_one();
            LogWriter::open_or_initialize(
                LogWriterOptions::default(),
                &writer_name,
                fragment_factory,
                manifest_factory,
                None,
            )
            .await
            .expect("open_or_initialize should succeed even when racing")
        }));
    }

    done.store(true, Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    notifier.notify_waiters();
    for handle in handles {
        notifier.notify_one();
        handle.await.expect("task should not panic");
    }
}
