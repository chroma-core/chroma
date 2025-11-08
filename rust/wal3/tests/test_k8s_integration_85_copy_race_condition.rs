use std::sync::Arc;
use tokio::sync::Barrier;

use chroma_storage::s3_client_for_test_with_new_bucket;

use wal3::{
    LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest,
    ThrottleOptions,
};

#[tokio::test]
async fn test_k8s_integration_85_copy_race_condition() {
    const DELAYS_MS: &[u64] = &[0, 1, 2, 3, 5];
    const ATTEMPTS_PER_DELAY: usize = 5;

    let mut race_detected_count = 0;
    let mut total_attempts = 0;

    for &delay_ms in DELAYS_MS {
        for attempt in 0..ATTEMPTS_PER_DELAY {
            total_attempts += 1;
            println!(
                "\n========== Delay: {}ms, Attempt {} ==========",
                delay_ms, attempt
            );
            if run_single_attempt(total_attempts, delay_ms).await {
                race_detected_count += 1;
                println!(
                    "!!! Race condition detected with {}ms delay, attempt {} !!!",
                    delay_ms, attempt
                );
            }
        }
    }

    println!("\n========== SUMMARY ==========");
    println!(
        "Race condition detected in {} out of {} attempts",
        race_detected_count, total_attempts
    );

    if race_detected_count > 0 {
        panic!(
            "Race condition detected in {} out of {} attempts!",
            race_detected_count, total_attempts
        );
    }

    println!("Test passed: Race condition was not triggered in any attempts.");
}

async fn run_single_attempt(attempt: usize, delay_ms: u64) -> bool {
    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let prefix = format!("test_copy_empty_concurrent_{}", attempt);

    Manifest::initialize(&LogWriterOptions::default(), &storage, &prefix, "init")
        .await
        .unwrap();

    let reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        prefix.clone(),
    )
    .await
    .unwrap();

    let manifest_before = reader.manifest().await.unwrap().unwrap();
    let next_write_before = manifest_before.next_write_timestamp();
    let next_seq_no_before = manifest_before.next_fragment_seq_no();

    let barrier_start = Arc::new(Barrier::new(2));
    let barrier_start_clone = Arc::clone(&barrier_start);

    let storage_clone = Arc::clone(&storage);
    let prefix_clone = prefix.clone();

    let writer_task = tokio::spawn(async move {
        let log = LogWriter::open(
            LogWriterOptions {
                throttle_fragment: ThrottleOptions {
                    batch_size_bytes: 1,
                    batch_interval_us: 1,
                    ..ThrottleOptions::default()
                },
                ..LogWriterOptions::default()
            },
            storage_clone,
            &prefix_clone,
            "concurrent_writer",
            (),
        )
        .await
        .unwrap();

        barrier_start_clone.wait().await;

        log.append_many(vec![Vec::from("concurrent data")])
            .await
            .unwrap();
    });

    barrier_start.wait().await;

    if delay_ms > 0 {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
    }

    wal3::copy(
        &storage,
        &LogWriterOptions::default(),
        &reader,
        LogPosition::default(),
        format!("{}_target", prefix),
    )
    .await
    .unwrap();

    writer_task.await.unwrap();

    let copied_reader = LogReader::open(
        LogReaderOptions::default(),
        Arc::clone(&storage),
        format!("{}_target", prefix),
    )
    .await
    .unwrap();

    let copied_manifest = copied_reader.manifest().await.unwrap().unwrap();

    // Check if race condition was triggered:
    // - Copied log has 0 fragments (scan() saw empty log)
    // - BUT copied log has updated next_write/next_seq_no (second manifest load saw writer's changes)
    let race_detected = copied_manifest.fragments.is_empty()
        && (copied_manifest.next_write_timestamp() != next_write_before
            || copied_manifest.next_fragment_seq_no() != next_seq_no_before);

    if race_detected {
        println!("  Race detected: fragments={}, next_write={:?} (expected {:?}), next_seq_no={:?} (expected {:?})",
            copied_manifest.fragments.len(),
            copied_manifest.next_write_timestamp(),
            next_write_before,
            copied_manifest.next_fragment_seq_no(),
            next_seq_no_before);
    }

    race_detected
}
