#![recursion_limit = "256"]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Error, LogWriter, LogWriterOptions, Manifest, ManifestManagerFactory,
    ReplicatedManifestManagerFactory, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

type ReplLogWriter = LogWriter<
    wal3::FragmentUuid,
    wal3::ReplicatedFragmentManagerFactory,
    wal3::ReplicatedManifestManagerFactory,
>;

async fn writer_thread(
    writer: Arc<ReplLogWriter>,
    running: Arc<AtomicUsize>,
    num_writes: Arc<AtomicUsize>,
    total_writes: usize,
    thread_id: usize,
) -> (usize, usize) {
    let mut successful_writes = 0;
    let mut contention_errors = 0;
    println!(
        "writer {thread_id} also known as {:?}",
        &*writer as *const ReplLogWriter
    );

    while num_writes.load(Ordering::Relaxed) < total_writes {
        let message = format!("Message from writer{}", thread_id).into_bytes();
        match writer.append(message.clone()).await {
            Ok(_) => {
                println!(
                    "writer {thread_id} succeeds {}",
                    num_writes.fetch_add(1, Ordering::Relaxed)
                );
                successful_writes += 1;
            }
            err @ Err(Error::LogContentionDurable)
            | err @ Err(Error::LogContentionRetry)
            | err @ Err(Error::LogContentionFailure) => {
                println!("writer {thread_id} sees contention preventing write {err:?}");
                contention_errors += 1;
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    eprintln!(
        "one thread done: rem={}",
        running.fetch_sub(1, Ordering::Relaxed) - 1
    );
    (successful_writes, contention_errors)
}

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_99_ping_pong_contention() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_99_contention/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);
    let writer_name = "init";

    // Initialize the log.
    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty(writer_name))
        .await
        .expect("init should succeed");

    // Create two writers that will contend with each other.
    let options1 = LogWriterOptions::default();
    let (fragment_factory1, manifest_factory1) = create_repl_factories(
        options1.clone(),
        default_repl_options(),
        0,
        Arc::clone(&storages),
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );
    let writer1 = Arc::new(
        LogWriter::open(
            options1,
            "writer1",
            fragment_factory1,
            manifest_factory1,
            None,
        )
        .await
        .expect("LogWriter::open should succeed"),
    );

    let options2 = LogWriterOptions::default();
    let (fragment_factory2, manifest_factory2) = create_repl_factories(
        options2.clone(),
        default_repl_options(),
        0,
        Arc::clone(&storages),
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );
    let writer2 = Arc::new(
        LogWriter::open(
            options2,
            "writer2",
            fragment_factory2,
            manifest_factory2,
            None,
        )
        .await
        .expect("LogWriter::open should succeed"),
    );

    // Set a timer to make sure the test only runs for 4 minutes.
    let fail = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(250)).await;
        eprintln!("Taking down the test");
        std::process::exit(13);
    });

    let running = Arc::new(AtomicUsize::new(2));
    let num_writes = Arc::new(AtomicUsize::new(0));

    // Launch both threads.
    let handle1 = tokio::spawn(writer_thread(
        Arc::clone(&writer1),
        Arc::clone(&running),
        Arc::clone(&num_writes),
        250,
        1,
    ));

    let handle2 = tokio::spawn(writer_thread(
        Arc::clone(&writer2),
        Arc::clone(&running),
        Arc::clone(&num_writes),
        250,
        2,
    ));

    // Wait for both threads to complete.
    let (writer1_results, writer2_results) = tokio::join!(handle1, handle2);
    fail.abort();

    // Examine results.
    let (writer1_successes, writer1_contentions) =
        writer1_results.expect("writer1 task should complete");
    let (writer2_successes, writer2_contentions) =
        writer2_results.expect("writer2 task should complete");

    println!(
        "Writer 1: {} successful writes, {} contentions",
        writer1_successes, writer1_contentions
    );
    println!(
        "Writer 2: {} successful writes, {} contentions",
        writer2_successes, writer2_contentions
    );

    // Assert some things about the test.
    assert!(
        writer1_successes + writer2_successes > 0,
        "Writers should have some successful writes"
    );

    println!("repl_99_ping_pong_contention: passed, log_id={}", log_id);
}
