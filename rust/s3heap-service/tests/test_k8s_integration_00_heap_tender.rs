use std::sync::Arc;

use chroma_storage::Storage;
use chroma_sysdb::{SysDb, TestSysDb};
use chroma_types::{CollectionUuid, DirtyMarker};
use wal3::{CursorStore, CursorStoreOptions, LogPosition, LogReader, LogReaderOptions};

use s3heap::{HeapPruner, HeapReader, HeapWriter};
use s3heap_service::{HeapTender, HEAP_TENDER_CURSOR_NAME};

// Dummy scheduler for testing purposes
struct DummyScheduler;

#[async_trait::async_trait]
impl s3heap::HeapScheduler for DummyScheduler {
    async fn are_done(
        &self,
        items: &[(s3heap::Triggerable, uuid::Uuid)],
    ) -> Result<Vec<bool>, s3heap::Error> {
        Ok(vec![false; items.len()])
    }

    async fn get_schedules(
        &self,
        _ids: &[uuid::Uuid],
    ) -> Result<Vec<s3heap::Schedule>, s3heap::Error> {
        Ok(vec![])
    }
}

async fn test_heap_tender(storage: Storage, test_id: &str) -> HeapTender {
    let dirty_log_prefix = format!("test-dirty-log-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);
    create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await
}

async fn create_heap_tender(
    storage: Storage,
    dirty_log_prefix: &str,
    heap_prefix: &str,
) -> HeapTender {
    let sysdb = SysDb::Test(TestSysDb::new());
    let reader = LogReader::new(
        LogReaderOptions::default(),
        Arc::new(storage.clone()),
        dirty_log_prefix.to_string(),
    );
    let cursor = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage.clone()),
        dirty_log_prefix.to_string(),
        "test-tender".to_string(),
    );
    let scheduler = Arc::new(DummyScheduler) as _;
    let writer = HeapWriter::new(
        storage.clone(),
        heap_prefix.to_string(),
        Arc::clone(&scheduler),
    )
    .await
    .unwrap();
    let heap_reader = HeapReader::new(
        storage.clone(),
        heap_prefix.to_string(),
        Arc::clone(&scheduler),
    )
    .await
    .unwrap();
    let heap_pruner =
        HeapPruner::new(storage, heap_prefix.to_string(), Arc::clone(&scheduler)).unwrap();
    HeapTender::new(sysdb, reader, cursor, writer, heap_reader, heap_pruner)
}

#[tokio::test]
async fn test_k8s_integration_empty_dirty_log_returns_empty_list() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let tender = test_heap_tender(storage, "empty").await;

    let result = tender.read_and_coalesce_dirty_log().await;
    if let Err(ref e) = result {
        println!("Error: {:?}", e);
    }
    assert!(result.is_ok());
    let (witness, _cursor, tended) = result.unwrap();
    assert!(witness.is_none());
    assert_eq!(tended.len(), 0);
}

#[tokio::test]
async fn test_k8s_integration_single_mark_dirty_returns_collection() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-dirty-single-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id = CollectionUuid::new();
    let marker = DirtyMarker::MarkDirty {
        collection_id,
        log_position: 100,
        num_records: 10,
        reinsert_count: 0,
        initial_insertion_epoch_us: 1234567890,
    };

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    let marker_bytes = serde_json::to_vec(&marker).unwrap();
    log_writer.append(marker_bytes).await.unwrap();

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 1);
    assert_eq!(tended[0].0, collection_id);
    assert_eq!(tended[0].1, LogPosition::from_offset(110));
}

#[tokio::test]
async fn test_k8s_integration_multiple_markers_same_collection_keeps_max() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-dirty-multi-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id = CollectionUuid::new();
    let markers = vec![
        DirtyMarker::MarkDirty {
            collection_id,
            log_position: 100,
            num_records: 10,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
        DirtyMarker::MarkDirty {
            collection_id,
            log_position: 200,
            num_records: 5,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
        DirtyMarker::MarkDirty {
            collection_id,
            log_position: 150,
            num_records: 3,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
    ];

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    for marker in markers {
        let marker_bytes = serde_json::to_vec(&marker).unwrap();
        log_writer.append(marker_bytes).await.unwrap();
    }

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 1);
    assert_eq!(tended[0].0, collection_id);
    assert_eq!(tended[0].1, LogPosition::from_offset(205));
}

#[tokio::test]
async fn test_k8s_integration_reinsert_count_nonzero_filters_marker() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-dirty-reinsert-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id1 = CollectionUuid::new();
    let collection_id2 = CollectionUuid::new();
    let markers = vec![
        DirtyMarker::MarkDirty {
            collection_id: collection_id1,
            log_position: 100,
            num_records: 10,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
        DirtyMarker::MarkDirty {
            collection_id: collection_id2,
            log_position: 200,
            num_records: 5,
            reinsert_count: 1,
            initial_insertion_epoch_us: 1234567890,
        },
    ];

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    for marker in markers {
        let marker_bytes = serde_json::to_vec(&marker).unwrap();
        log_writer.append(marker_bytes).await.unwrap();
    }

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 1);
    assert_eq!(tended[0].0, collection_id1);
}

#[tokio::test]
async fn test_k8s_integration_purge_and_cleared_markers_ignored() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-dirty-purge-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id1 = CollectionUuid::new();
    let collection_id2 = CollectionUuid::new();
    let collection_id3 = CollectionUuid::new();
    let markers = vec![
        DirtyMarker::MarkDirty {
            collection_id: collection_id1,
            log_position: 100,
            num_records: 10,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
        DirtyMarker::Purge {
            collection_id: collection_id2,
        },
        DirtyMarker::Cleared,
        DirtyMarker::MarkDirty {
            collection_id: collection_id3,
            log_position: 200,
            num_records: 5,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        },
    ];

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    for marker in markers {
        let marker_bytes = serde_json::to_vec(&marker).unwrap();
        log_writer.append(marker_bytes).await.unwrap();
    }

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 2);
    let collection_ids: std::collections::HashSet<_> = tended.iter().map(|(id, _)| *id).collect();
    assert!(collection_ids.contains(&collection_id1));
    assert!(collection_ids.contains(&collection_id3));
}

#[tokio::test]
async fn test_k8s_integration_multiple_collections_all_processed() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-dirty-multiple-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_ids: Vec<_> = (0..5).map(|_| CollectionUuid::new()).collect();
    let markers: Vec<_> = collection_ids
        .iter()
        .enumerate()
        .map(|(i, &collection_id)| DirtyMarker::MarkDirty {
            collection_id,
            log_position: (i as u64 + 1) * 100,
            num_records: 10,
            reinsert_count: 0,
            initial_insertion_epoch_us: 1234567890,
        })
        .collect();

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    for marker in markers {
        let marker_bytes = serde_json::to_vec(&marker).unwrap();
        log_writer.append(marker_bytes).await.unwrap();
    }

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 5);
    let found_ids: std::collections::HashSet<_> = tended.iter().map(|(id, _)| *id).collect();
    for &id in &collection_ids {
        assert!(found_ids.contains(&id));
    }
}

#[tokio::test]
async fn test_k8s_integration_cursor_initialized_on_first_run() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-cursor-init-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id = CollectionUuid::new();
    let marker = DirtyMarker::MarkDirty {
        collection_id,
        log_position: 100,
        num_records: 10,
        reinsert_count: 0,
        initial_insertion_epoch_us: 1234567890,
    };

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    let marker_bytes = serde_json::to_vec(&marker).unwrap();
    log_writer.append(marker_bytes).await.unwrap();

    let tender = create_heap_tender(storage.clone(), &dirty_log_prefix, &heap_prefix).await;

    let result = tender.tend_to_heap().await;
    assert!(result.is_ok());

    // Create a separate cursor store to verify the result
    let verify_cursor = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage),
        dirty_log_prefix.to_string(),
        "test-verify".to_string(),
    );
    let witness = verify_cursor.load(&HEAP_TENDER_CURSOR_NAME).await.unwrap();
    assert!(witness.is_some());
}

#[tokio::test]
async fn test_k8s_integration_cursor_advances_on_subsequent_runs() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-cursor-advance-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();

    let collection_id1 = CollectionUuid::new();
    let marker1 = DirtyMarker::MarkDirty {
        collection_id: collection_id1,
        log_position: 100,
        num_records: 10,
        reinsert_count: 0,
        initial_insertion_epoch_us: 1234567890,
    };
    log_writer
        .append(serde_json::to_vec(&marker1).unwrap())
        .await
        .unwrap();

    let tender = create_heap_tender(storage.clone(), &dirty_log_prefix, &heap_prefix).await;

    tender.tend_to_heap().await.unwrap();

    // Create a separate cursor store to verify the result
    let verify_cursor = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage.clone()),
        dirty_log_prefix.clone(),
        "test-verify".to_string(),
    );
    let first_witness = verify_cursor.load(&HEAP_TENDER_CURSOR_NAME).await.unwrap();
    let first_position = first_witness.as_ref().unwrap().cursor().position;

    let collection_id2 = CollectionUuid::new();
    let marker2 = DirtyMarker::MarkDirty {
        collection_id: collection_id2,
        log_position: 200,
        num_records: 5,
        reinsert_count: 0,
        initial_insertion_epoch_us: 1234567890,
    };
    log_writer
        .append(serde_json::to_vec(&marker2).unwrap())
        .await
        .unwrap();

    tender.tend_to_heap().await.unwrap();
    let second_witness = verify_cursor.load(&HEAP_TENDER_CURSOR_NAME).await.unwrap();
    let second_position = second_witness.as_ref().unwrap().cursor().position;

    assert!(second_position > first_position);
}

#[tokio::test]
async fn test_k8s_integration_cursor_not_updated_when_no_new_data() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-cursor-no-update-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let collection_id = CollectionUuid::new();
    let marker = DirtyMarker::MarkDirty {
        collection_id,
        log_position: 100,
        num_records: 10,
        reinsert_count: 0,
        initial_insertion_epoch_us: 1234567890,
    };

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    log_writer
        .append(serde_json::to_vec(&marker).unwrap())
        .await
        .unwrap();

    let tender = create_heap_tender(storage.clone(), &dirty_log_prefix, &heap_prefix).await;

    tender.tend_to_heap().await.unwrap();

    // Create a separate cursor store to verify the result
    let verify_cursor = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage),
        dirty_log_prefix,
        "test-verify".to_string(),
    );
    let first_witness = verify_cursor.load(&HEAP_TENDER_CURSOR_NAME).await.unwrap();
    let first_position = first_witness.as_ref().unwrap().cursor().position;

    tender.tend_to_heap().await.unwrap();
    let second_witness = verify_cursor.load(&HEAP_TENDER_CURSOR_NAME).await.unwrap();
    let second_position = second_witness.as_ref().unwrap().cursor().position;

    assert_eq!(first_position, second_position);
}

#[tokio::test]
async fn test_k8s_integration_invalid_json_in_dirty_log_fails() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-invalid-json-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    let invalid_json = b"not valid json at all".to_vec();
    log_writer.append(invalid_json).await.unwrap();

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        s3heap_service::Error::Json(_)
    ));
}

#[tokio::test]
async fn test_k8s_integration_handles_empty_markers_after_filtering() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let test_id = uuid::Uuid::new_v4();
    let dirty_log_prefix = format!("test-empty-after-filter-{}", test_id);
    let heap_prefix = format!("test-heap-{}", test_id);

    let markers = vec![
        DirtyMarker::MarkDirty {
            collection_id: CollectionUuid::new(),
            log_position: 100,
            num_records: 10,
            reinsert_count: 5,
            initial_insertion_epoch_us: 1234567890,
        },
        DirtyMarker::Purge {
            collection_id: CollectionUuid::new(),
        },
        DirtyMarker::Cleared,
    ];

    let log_writer = wal3::LogWriter::open_or_initialize(
        wal3::LogWriterOptions::default(),
        Arc::new(storage.clone()),
        &dirty_log_prefix,
        &format!("test-writer-{}", test_id),
        (),
    )
    .await
    .unwrap();
    for marker in markers {
        log_writer
            .append(serde_json::to_vec(&marker).unwrap())
            .await
            .unwrap();
    }

    let tender = create_heap_tender(storage, &dirty_log_prefix, &heap_prefix).await;

    let result = tender.read_and_coalesce_dirty_log().await;
    assert!(result.is_ok());
    let (_witness, _cursor, tended) = result.unwrap();
    assert_eq!(tended.len(), 0);
}
