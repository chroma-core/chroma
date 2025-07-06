#![recursion_limit = "256"]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{Error, LogWriter, LogWriterOptions, Manifest};

pub mod common;

async fn writer_thread(
    writer: Arc<LogWriter>,
    running: Arc<AtomicUsize>,
    num_writes: Arc<AtomicUsize>,
    total_writes: usize,
    thread_id: usize,
) -> (usize, usize) {
    let mut successful_writes = 0;
    let mut contention_errors = 0;
    println!(
        "writer {thread_id} also known as {:?}",
        &*writer as *const LogWriter
    );

    while num_writes.load(Ordering::Relaxed) < total_writes {
        let message = format!("Message from writer{}", thread_id).into_bytes();
        // We have the lock, do a write
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
async fn test_k8s_integration_99_ping_pong_contention() {
    // Create a shared storage for both threads to use
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_99_ping_pong_contention";
    let writer_name = "init";

    // Initialize the log
    Manifest::initialize(&LogWriterOptions::default(), &storage, prefix, writer_name)
        .await
        .unwrap();

    // Create two writers that will contend with each other
    let writer1 = Arc::new(
        LogWriter::open(
            LogWriterOptions::default(),
            Arc::clone(&storage),
            prefix,
            "writer1",
            (),
        )
        .await
        .unwrap(),
    );

    let writer2 = Arc::new(
        LogWriter::open(
            LogWriterOptions::default(),
            Arc::clone(&storage),
            prefix,
            "writer2",
            (),
        )
        .await
        .unwrap(),
    );

    // Set a timer to make sure the test only runs for 3 minutes.
    let fail = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(250)).await;
        eprintln!("Taking down the test");
        std::process::exit(13);
    });

    let running = Arc::new(AtomicUsize::new(2));
    let num_writes = Arc::new(AtomicUsize::new(0));
    // Launch both threads using the same writer_thread function
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

    // Wait for both threads to complete
    let (writer1_results, writer2_results) = tokio::join!(handle1, handle2);
    fail.abort();

    // Examine results
    let (writer1_successes, writer1_contentions) = writer1_results.unwrap();
    let (writer2_successes, writer2_contentions) = writer2_results.unwrap();

    println!(
        "Writer 1: {} successful writes, {} contentions",
        writer1_successes, writer1_contentions
    );
    println!(
        "Writer 2: {} successful writes, {} contentions",
        writer2_successes, writer2_contentions
    );

    // Assert some things about the test
    assert!(
        writer1_successes + writer2_successes > 0,
        "Writers should have some successful writes"
    );
}
