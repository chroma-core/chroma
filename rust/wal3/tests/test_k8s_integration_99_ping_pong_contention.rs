use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{Error, LogWriter, LogWriterOptions, Manifest};

pub mod common;

async fn writer_thread(
    writer: Arc<LogWriter>,
    mutex: Arc<Mutex<()>>,
    running: Arc<AtomicUsize>,
    thread_id: usize,
    iterations: usize,
) -> (usize, usize) {
    let mut successful_writes = 0;
    let mut contention_errors = 0;
    println!(
        "writer {thread_id} also known as {:?}",
        &*writer as *const LogWriter
    );

    for i in 0..iterations {
        let message = format!("Message from writer{}: {}", thread_id, i).into_bytes();

        // Acquire the mutex, do a write, release the mutex
        loop {
            // Using tokio::sync::Mutex which is safe to use with .await
            let _guard = mutex.lock().await;
            println!("writer {thread_id} grabs lock in iteration {i}");
            // We have the lock, do a write
            match writer.append(message.clone()).await {
                Ok(_) => {
                    println!("writer {thread_id} succeeds in iteration {i}");
                    successful_writes += 1;
                    // Release mutex (implicit) and break
                    break;
                }
                Err(Error::LogContention) => {
                    println!("writer {thread_id} sees contention preventing {i}");
                    contention_errors += 1;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        println!("writer {thread_id} goes for contention {i}");

        // Now write until we hit LogContention
        while i + 1 < iterations && running.load(Ordering::Relaxed) > 1 {
            // NOTE(rescrv):
            // Default batching interval is 100ms.
            // This must be at least that to prevent waste.
            tokio::time::sleep(Duration::from_millis(150)).await;
            match writer.append(message.clone()).await {
                Ok(_) => {
                    successful_writes += 1;
                }
                Err(Error::LogContention) => {
                    println!("writer {thread_id} contends without lock in iteration {i}");
                    contention_errors += 1;
                    // Got contention, now we go back to the top and wait for mutex
                    break;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
    }

    running.fetch_sub(1, Ordering::Relaxed);
    (successful_writes, contention_errors)
}

#[tokio::test]
async fn test_k8s_integration_ping_pong_contention() {
    // Create a shared storage for both threads to use
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_ping_pong_contention";
    let writer_name = "init";

    // Initialize the log
    Manifest::initialize(&LogWriterOptions::default(), &storage, prefix, writer_name)
        .await
        .unwrap();

    // Create a shared mutex that our two threads will use to coordinate access
    let mutex = Arc::new(Mutex::new(()));

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
        tokio::time::sleep(Duration::from_secs(180)).await;
        eprintln!("Taking down the test");
        std::process::exit(13);
    });

    let running = Arc::new(AtomicUsize::new(2));

    // Launch both threads using the same writer_thread function
    let handle1 = tokio::spawn(writer_thread(
        Arc::clone(&writer1),
        Arc::clone(&mutex),
        Arc::clone(&running),
        1,
        20,
    ));

    let handle2 = tokio::spawn(writer_thread(
        Arc::clone(&writer2),
        Arc::clone(&mutex),
        Arc::clone(&running),
        2,
        20,
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
        writer1_successes > 0,
        "Writer 1 should have some successful writes"
    );
    assert!(
        writer2_successes > 0,
        "Writer 2 should have some successful writes"
    );
    assert!(
        writer1_contentions > 0,
        "Writer 1 should encounter contention"
    );
    assert!(
        writer2_contentions > 0,
        "Writer 2 should encounter contention"
    );
}
