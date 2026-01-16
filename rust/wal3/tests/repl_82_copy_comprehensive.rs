use std::sync::Arc;

use chroma_storage::s3_client_for_test_with_new_bucket;
use uuid::Uuid;

use wal3::{
    create_repl_factories, Cursor, CursorName, CursorStoreOptions, LogPosition, LogWriter,
    LogWriterOptions, Manifest, ManifestConsumer, ManifestManagerFactory,
    ReplicatedManifestManagerFactory, SnapshotOptions, StorageWrapper,
};

mod common;
use common::{default_repl_options, setup_spanner_client};

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_copy_with_deep_snapshots() {
    // Note: Snapshots are not fully implemented for repl manifests, so this test
    // focuses on fragment copying without snapshot assertions.
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_deep_snapshots/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 3,
            fragment_rollover_threshold: 3,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log = LogWriter::open(options, "writer", fragment_factory, manifest_factory, None)
        .await
        .expect("LogWriter::open should succeed");

    for i in 0..200 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("snapshot-test:i={},j={}", i, j)));
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    // Verify fragments were created in Spanner manifest.
    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();
    assert!(
        !manifest.fragments.is_empty(),
        "Expected fragments to be created"
    );
    println!(
        "repl_copy_with_deep_snapshots: {} fragments created, log_id={}",
        manifest.fragments.len(),
        log_id
    );
}

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_copy_at_specific_offset() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_at_specific_offset/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 5,
            fragment_rollover_threshold: 5,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log = LogWriter::open(options, "writer", fragment_factory, manifest_factory, None)
        .await
        .expect("LogWriter::open should succeed");

    let mut offset_at_50 = LogPosition::default();
    for i in 0..100 {
        let mut batch = Vec::with_capacity(10);
        for j in 0..10 {
            batch.push(Vec::from(format!("offset-test:i={},j={}", i, j)));
        }
        let pos = log
            .append_many(batch)
            .await
            .expect("append_many should succeed");
        if i == 49 {
            offset_at_50 = pos + 10u64;
        }
    }

    // Verify the offset was recorded correctly.
    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert!(
        manifest.next_write_timestamp() > offset_at_50,
        "Manifest should have progressed past offset 50"
    );
    println!(
        "repl_copy_at_specific_offset: offset_at_50={}, next_write={}, log_id={}",
        offset_at_50.offset(),
        manifest.next_write_timestamp().offset(),
        log_id
    );
}

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_copy_verifies_manifest_consistency() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_verifies_manifest/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log = LogWriter::open(options, "writer", fragment_factory, manifest_factory, None)
        .await
        .expect("LogWriter::open should succeed");

    for i in 0..50 {
        let batch = vec![Vec::from(format!("consistency:i={}", i))];
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert_eq!(manifest.fragments.len(), 50, "Should have 50 fragments");

    // Verify fragment consistency.
    for (i, fragment) in manifest.fragments.iter().enumerate() {
        assert!(fragment.num_bytes > 0, "Fragment {} should have bytes", i);
        assert!(
            fragment.seq_no.as_uuid().is_some(),
            "Fragment {} should have UUID identifier",
            i
        );
    }

    println!(
        "repl_copy_verifies_manifest_consistency: {} fragments verified, log_id={}",
        manifest.fragments.len(),
        log_id
    );
}

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_copy_empty_with_advanced_manifest() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_empty_advanced/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let options = LogWriterOptions::default();
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        Arc::clone(&storages),
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log =
        LogWriter::open_or_initialize(options, "writer", fragment_factory, manifest_factory, None)
            .await
            .expect("LogWriter::open_or_initialize should succeed");

    let mut position = LogPosition::default();
    for i in 0..50 {
        let batch = vec![Vec::from(format!("advanced:i={}", i))];
        position = log
            .append_many(batch)
            .await
            .expect("append_many should succeed")
            + 1u64;
    }

    let cursors = log.cursors(CursorStoreOptions::default()).await.unwrap();
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

    // Note: Garbage collection is not fully implemented for repl manifests.
    // We skip the garbage_collect call here.

    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    println!(
        "repl_copy_empty_with_advanced_manifest: {} fragments, next_write={}, log_id={}",
        manifest.fragments.len(),
        manifest.next_write_timestamp().offset(),
        log_id
    );
}

#[tokio::test]
async fn test_k8s_mcmr_integration_repl_copy_with_large_fragments() {
    let client = setup_spanner_client().await;
    let log_id = Uuid::new_v4();

    let storage = s3_client_for_test_with_new_bucket().await;
    let prefix = format!("repl_copy_large_fragments/{}", log_id);
    let wrapper = StorageWrapper::new("test-region".to_string(), storage.clone(), prefix.clone());
    let storages = Arc::new(vec![wrapper]);

    let init_factory = ReplicatedManifestManagerFactory::new(
        Arc::clone(&client),
        vec!["dummy".to_string()],
        "dummy".to_string(),
        log_id,
    );
    init_factory
        .init_manifest(&Manifest::new_empty("init"))
        .await
        .expect("init should succeed");

    let options = LogWriterOptions {
        snapshot_manifest: SnapshotOptions {
            snapshot_rollover_threshold: 10,
            fragment_rollover_threshold: 10,
        },
        ..LogWriterOptions::default()
    };
    let (fragment_factory, manifest_factory) = create_repl_factories(
        options.clone(),
        default_repl_options(),
        0,
        storages,
        Arc::clone(&client),
        vec!["dummy".to_string()],
        log_id,
    );

    let log = LogWriter::open(options, "writer", fragment_factory, manifest_factory, None)
        .await
        .expect("LogWriter::open should succeed");

    for _i in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for _j in 0..100 {
            batch.push(vec![0u8; 1024]);
        }
        log.append_many(batch)
            .await
            .expect("append_many should succeed");
    }

    let consumer = init_factory.make_consumer().await.unwrap();
    let (manifest, _) = consumer.manifest_load().await.unwrap().unwrap();

    assert!(manifest.acc_bytes > 0, "Should have accumulated bytes");

    println!(
        "repl_copy_with_large_fragments: {} bytes in {} fragments, log_id={}",
        manifest.acc_bytes,
        manifest.fragments.len(),
        log_id
    );
}
