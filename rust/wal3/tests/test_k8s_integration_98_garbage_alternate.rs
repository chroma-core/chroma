use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    Cursor, CursorName, CursorStoreOptions, Error, GarbageCollectionOptions, LogReaderOptions,
    LogWriter, LogWriterOptions, Manifest,
};

pub mod common;

async fn writer_thread(
    writer: Arc<LogWriter>,
    mutex: Arc<Mutex<()>>,
    wait: Arc<tokio::sync::Notify>,
    notify: Arc<tokio::sync::Notify>,
    iterations: usize,
) -> (usize, usize) {
    let cursors = writer.cursors(CursorStoreOptions::default()).unwrap();
    let mut witness = cursors
        .load(&CursorName::new("my_cursor").unwrap())
        .await
        .unwrap()
        .expect("test initialized a cursor so witness must be Some(_)");
    let mut successful_writes = 0;
    let mut contention_errors = 0;
    for i in 0..iterations {
        let message = format!("Message from writer: {}", i).into_bytes();
        wait.notified().await;
        let _guard = mutex.lock().await;
        loop {
            match writer.append(message.clone()).await {
                Ok(position) => {
                    println!("writer succeeds in iteration {i}");
                    successful_writes += 1;
                    witness = cursors
                        .save(
                            &CursorName::new("my_cursor").unwrap(),
                            &Cursor {
                                position,
                                epoch_us: position.offset(),
                                writer: "Test Writer".to_string(),
                            },
                            &witness,
                        )
                        .await
                        .unwrap();
                    writer
                        .reader(LogReaderOptions::default())
                        .unwrap()
                        .scrub(wal3::Limits::default())
                        .await
                        .unwrap();
                    break;
                }
                Err(Error::LogContentionDurable)
                | Err(Error::LogContentionRetry)
                | Err(Error::LogContentionFailure) => {
                    println!("writer sees contention preventing {i}");
                    contention_errors += 1;
                    continue;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        notify.notify_one();
    }
    (successful_writes, contention_errors)
}

async fn garbage_collector_thread(
    writer: Arc<LogWriter>,
    mutex: Arc<Mutex<()>>,
    wait: Arc<tokio::sync::Notify>,
    notify: Arc<tokio::sync::Notify>,
    iterations: usize,
) -> (usize, usize) {
    println!("gc {:?}", &*writer as *const LogWriter);
    let mut successes = 0;
    let mut contentions = 0;
    for i in 0..iterations {
        wait.notified().await;
        // Using tokio::sync::Mutex which is safe to use with .await
        let _guard = mutex.lock().await;
        println!("gc grabs lock in iteration {i}");
        loop {
            match writer
                .garbage_collect(&GarbageCollectionOptions::default(), None)
                .await
            {
                Ok(()) => break,
                Err(Error::CorruptGarbage(m))
                    if m.starts_with("First to keep does not overlap manifest") =>
                {
                    println!("gc sees cursor ahead of manifest; only a problem if looping");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    contentions += 1;
                    continue;
                }
                Err(Error::LogContentionDurable)
                | Err(Error::LogContentionRetry)
                | Err(Error::LogContentionFailure) => {
                    println!("gc sees contention preventing {i}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    contentions += 1;
                    continue;
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
        successes += 1;
        notify.notify_one();
    }
    (successes, contentions)
}

#[tokio::test]
async fn test_k8s_integration_98_garbage_alternate() {
    // Create a shared storage for both threads to use
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = "test_k8s_integration_98_garbage_alternate";
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
    let cursors = writer1.cursors(CursorStoreOptions::default()).unwrap();
    cursors
        .init(&CursorName::new("my_cursor").unwrap(), Cursor::default())
        .await
        .unwrap();

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

    let notify_writer = Arc::new(tokio::sync::Notify::new());
    let notify_gcer = Arc::new(tokio::sync::Notify::new());
    notify_writer.notify_one();

    // Launch both threads using the same writer_thread function
    let handle1 = tokio::spawn(writer_thread(
        Arc::clone(&writer1),
        Arc::clone(&mutex),
        Arc::clone(&notify_writer),
        Arc::clone(&notify_gcer),
        20,
    ));

    let handle2 = tokio::spawn(garbage_collector_thread(
        Arc::clone(&writer2),
        Arc::clone(&mutex),
        Arc::clone(&notify_gcer),
        Arc::clone(&notify_writer),
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
        "Writer 1 should have some successful writes"
    );
}
