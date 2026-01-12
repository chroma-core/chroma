use std::sync::Arc;

use tokio::sync::Barrier;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, FragmentManagerFactory, LogPosition, LogReader, LogReaderOptions,
    LogWriter, LogWriterOptions, Manifest, ManifestManagerFactory,
    ReplicatedManifestManagerFactory, StorageWrapper, ThrottleOptions,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_85_copy_race_condition() {
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
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_race_condition_{}_{}", attempt, log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    // Initialize the manifest.
    let init_manifest_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_manifest_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

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
    )
    .await
    .expect("LogReader::open should succeed");

    let manifest_before = reader
        .manifest()
        .await
        .expect("manifest")
        .expect("manifest exists");
    let next_write_before = manifest_before.next_write_timestamp();
    let next_seq_no_before = manifest_before.next_fragment_seq_no();

    let barrier_start = Arc::new(Barrier::new(2));
    let barrier_start_clone = Arc::clone(&barrier_start);

    let storage_clone = storage.clone();
    let prefix_clone = prefix.clone();
    let storages_clone = Arc::clone(&storages);
    let client_clone = Arc::clone(&client);

    let writer_task = tokio::spawn(async move {
        let writer = "concurrent_writer";
        let options = LogWriterOptions {
            throttle_fragment: ThrottleOptions {
                batch_size_bytes: 1,
                batch_interval_us: 1,
                ..ThrottleOptions::default()
            },
            ..LogWriterOptions::default()
        };
        let (fragment_factory, manifest_factory) = create_repl_factories(
            options.clone(),
            default_repl_options(),
            storages_clone,
            client_clone,
            log_id,
        );
        let log = LogWriter::open(
            options,
            Arc::new(storage_clone),
            &prefix_clone,
            writer,
            fragment_factory,
            manifest_factory,
            None,
        )
        .await
        .expect("LogWriter::open should succeed");

        barrier_start_clone.wait().await;

        log.append_many(vec![Vec::from("concurrent data")])
            .await
            .expect("append_many should succeed");
    });

    barrier_start.wait().await;

    if delay_ms > 0 {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
    }

    // Copy to target using repl factories.
    let target_log_id = Uuid::new_v4();
    let target_prefix = format!("{}_target", prefix);
    let target_wrapper = StorageWrapper::new(
        "test-region".to_string(),
        storage.clone(),
        target_prefix.clone(),
    );
    let copy_target_storages = Arc::new(vec![target_wrapper]);
    let (copy_target_fragment_factory, copy_target_manifest_factory) = create_repl_factories(
        LogWriterOptions::default(),
        default_repl_options(),
        copy_target_storages,
        Arc::clone(&client),
        target_log_id,
    );
    let target_fragment_publisher = copy_target_fragment_factory
        .make_publisher()
        .await
        .expect("make_publisher should succeed");
    wal3::copy(
        &reader,
        LogPosition::default(),
        &target_fragment_publisher,
        copy_target_manifest_factory,
        None,
    )
    .await
    .expect("copy should succeed");

    writer_task.await.expect("writer task should complete");

    // Open the copied reader using repl factories.
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
    let copied_reader = LogReader::open(
        LogReaderOptions::default(),
        target_fragment_consumer,
        target_manifest_consumer,
    )
    .await
    .expect("LogReader::open for target should succeed");

    let copied_manifest = copied_reader
        .manifest()
        .await
        .expect("target manifest")
        .expect("manifest exists");

    // Check if race condition was triggered:
    // - Copied log has 0 fragments (scan() saw empty log)
    // - BUT copied log has updated next_write/next_seq_no (second manifest load saw writer's changes)
    let race_detected = copied_manifest.fragments.is_empty()
        && (copied_manifest.next_write_timestamp() != next_write_before);
    // NOTE(rescrv):  An observer comparing this to the s3 condition will see this missing piece:
    // || copied_manifest.next_fragment_seq_no() != next_seq_no_before);
    // This piece doesn't matter on uuid-based logs because there is no sequentiality.

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
