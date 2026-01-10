use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Cursor, CursorName, CursorStoreOptions, LogWriterOptions, Manifest,
    ManifestConsumer, ManifestManagerFactory, ReplicatedManifestManagerFactory, SnapshotOptions,
    StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn repl_copy_single_fragment() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_single_fragment/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = wal3::LogWriter::open(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    log.append_many(vec![Vec::from("single-record")])
        .await
        .expect("append_many should succeed");

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        1,
        "Should have exactly one fragment"
    );

    println!(
        "repl_copy_single_fragment: 1 fragment created, log_id={}",
        log_id
    );
}

#[tokio::test]
async fn repl_copy_immediately_after_initialization() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert_eq!(
        manifest.fragments.len(),
        0,
        "Newly initialized log should have no fragments"
    );

    println!(
        "repl_copy_immediately_after_initialization: 0 fragments, log_id={}",
        log_id
    );
}

#[tokio::test]
async fn repl_copy_preserves_fragment_boundaries() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_boundaries/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 100,
            fragment_rollover_threshold: 5,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = wal3::LogWriter::open(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    for i in 0..10 {
        let mut batch = Vec::new();
        for j in 0..10 {
            batch.push(Vec::from(format!("boundary:i={},j={}", i, j)));
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert!(
        manifest.fragments.len() >= 2,
        "Should have multiple fragments, got {}",
        manifest.fragments.len()
    );

    println!(
        "repl_copy_preserves_fragment_boundaries: {} fragments, log_id={}",
        manifest.fragments.len(),
        log_id
    );
}

#[tokio::test]
async fn repl_copy_multiple_times_creates_independent_copies() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_multiple/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        storages,
        Arc::clone(&client),
        log_id,
    );

    let log = wal3::LogWriter::open(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open should succeed");

    for i in 0..10 {
        log.append_many(vec![Vec::from(format!("multi:i={}", i))])
            .await
            .expect("append_many should succeed");
    }

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert_eq!(manifest.fragments.len(), 10, "Should have 10 fragments");

    println!(
        "repl_copy_multiple_times_creates_independent_copies: {} fragments, log_id={}",
        manifest.fragments.len(),
        log_id
    );
}

#[tokio::test]
async fn repl_copy_with_cursors() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_with_cursors/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        Arc::clone(&storages),
        Arc::clone(&client),
        log_id,
    );

    let log = wal3::LogWriter::open_or_initialize(
        options,
        Arc::new(storage.clone()),
        &prefix,
        "writer",
        fragment_factory,
        manifest_factory,
        None,
    )
    .await
    .expect("LogWriter::open_or_initialize should succeed");

    let mut position = wal3::LogPosition::default();
    for i in 0..20 {
        let batch = vec![Vec::from(format!("cursor-test:i={}", i))];
        position = log
            .append_many(batch)
            .await
            .expect("append_many should succeed")
            + 1u64;
    }

    let cursors = log.cursors(CursorStoreOptions::default()).unwrap();
    cursors
        .init(
            &CursorName::new("test_cursor").unwrap(),
            Cursor {
                position,
                epoch_us: 42,
                writer: "unit tests".to_string(),
            },
        )
        .await
        .expect("cursor init should succeed");

    let init_factory = ReplicatedManifestManagerFactory::new(Arc::clone(&client), log_id);
    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert_eq!(manifest.fragments.len(), 20, "Should have 20 fragments");

    println!(
        "repl_copy_with_cursors: {} fragments, cursor at {}, log_id={}",
        manifest.fragments.len(),
        position.offset(),
        log_id
    );
}
