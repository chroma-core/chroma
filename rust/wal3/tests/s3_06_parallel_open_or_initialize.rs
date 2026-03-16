use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{create_s3_factories, LogReaderOptions, LogWriter, LogWriterOptions};

mod common;

#[tokio::test]
async fn test_k8s_integration_06_parallel_open_or_initialize() {
    // Multiple concurrent open_or_initialize calls on an uninitialized log should all succeed.
    // This exercises the race where one writer wins the initialize and the others see
    // AlreadyInitialized, which must be treated as success.
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_06_parallel_open_or_initialize";
    let num_writers = 32;
    let mut handles = Vec::with_capacity(num_writers);
    let done = Arc::new(AtomicBool::new(false));
    let notifier = Arc::new(tokio::sync::Notify::new());
    for i in 0..num_writers {
        let storage = Arc::clone(&storage);
        let prefix = prefix.to_string();
        let done = Arc::clone(&done);
        let notifier = Arc::clone(&notifier);
        handles.push(tokio::spawn(async move {
            let writer_name = format!("writer{i}");
            let (fragment_factory, manifest_factory) = create_s3_factories(
                LogWriterOptions::default(),
                LogReaderOptions::default(),
                storage,
                prefix,
                writer_name.clone(),
                Arc::new(()),
                Arc::new(()),
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
    notifier.notify_waiters();
    let mut writers = Vec::with_capacity(num_writers);
    for handle in handles {
        writers.push(handle.await.expect("task should not panic"));
    }
}
