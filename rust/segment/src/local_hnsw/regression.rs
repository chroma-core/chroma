use super::*;
use chroma_sqlite::db::test_utils::get_new_sqlite_db;
use chroma_types::{
    Collection, KnnIndex, OperationRecord, Schema, SegmentScope, SegmentType, SegmentUuid,
};
use proptest::prelude::*;

async fn fixture() -> (
    tempfile::TempDir,
    SqliteDb,
    Collection,
    Segment,
    LocalHnswSegmentWriter,
) {
    let root = tempfile::tempdir().unwrap();
    let sqlite = get_new_sqlite_db().await;
    let mut collection = Collection::test_collection(3);
    collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
    let segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::HnswLocalPersisted,
        scope: SegmentScope::VECTOR,
        collection: collection.collection_id,
        metadata: None,
        file_path: Default::default(),
    };
    let writer = LocalHnswSegmentWriter::from_segment(
        &collection,
        &segment,
        3,
        Some(root.path().to_str().unwrap().into()),
        sqlite.clone(),
    )
    .await
    .unwrap();
    (root, sqlite, collection, segment, writer)
}

fn record(offset: i64, id: u8, kind: u8) -> LogRecord {
    let operation = match kind {
        0 => Operation::Add,
        1 => Operation::Upsert,
        2 => Operation::Update,
        _ => Operation::Delete,
    };
    LogRecord {
        log_offset: offset,
        record: OperationRecord {
            id: id.to_string(),
            embedding: (kind != 3).then(|| vec![offset as f32; 3]),
            encoding: None,
            metadata: None,
            document: None,
            operation,
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn replay_is_idempotent(history in prop::collection::vec((0..4u8, 0..4u8), 1..30)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (_root, _sqlite, _collection, _segment, mut writer) = fixture().await;
            let records = history.iter().enumerate().map(|(i, &(id, kind))| record(i as i64 + 1, id, kind)).collect::<Vec<_>>();
            let chunk = Chunk::new(records.into());
            writer.apply_log_chunk(chunk.clone()).await.unwrap();
            let snapshot = |guard: &Inner| (
                guard.last_seen_seq_id, guard.num_elements_since_last_persist,
                guard.id_map.total_elements_added, guard.id_map.id_to_label.clone(),
                guard.id_map.label_to_id.clone(), guard.index.len_with_deleted(),
            );
            let before = snapshot(&*writer.index.inner.read().await);
            for _ in 0..3 {
                writer.apply_log_chunk(chunk.clone()).await.unwrap();
                assert_eq!(snapshot(&*writer.index.inner.read().await), before);
            }
        });
    }

    #[test]
    fn corrupt_checkpoints_fail_before_watermark_migration(file in 0..5usize, truncate in 0..4usize) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (root, sqlite, collection, segment, mut writer) = fixture().await;
            writer.apply_log_chunk(Chunk::new(vec![record(42, 1, 0)].into())).await.unwrap();
            let mut guard = writer.index.inner.write().await;
            guard.id_map.max_seq_id = Some(42);
            drop(persist(guard).await.unwrap());
            writer.index.close().await;
            drop(writer);
            let files = [METADATA_FILE, "header.bin", "data_level0.bin", "length.bin", "link_lists.bin"];
            let path = root.path().join(segment.id.to_string()).join(files[file]);
            std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(truncate as u64).unwrap();
            let persist_path = Some(root.path().to_str().unwrap().to_string());
            assert!(LocalHnswSegmentReader::from_segment(&collection, &segment, 3, persist_path.clone(), sqlite.clone()).await.is_err());
            assert!(LocalHnswSegmentWriter::from_segment(&collection, &segment, 3, persist_path, sqlite.clone()).await.is_err());
            assert_eq!(get_current_seq_id(&segment, &sqlite).await.unwrap(), 0);
        });
    }
}

#[tokio::test]
async fn missing_pickle_does_not_reinitialize_durable_index() {
    let (root, sqlite, collection, segment, mut writer) = fixture().await;
    writer.index.inner.write().await.sync_threshold = 1;
    writer
        .apply_log_chunk(Chunk::new(vec![record(42, 1, 0)].into()))
        .await
        .unwrap();
    writer.index.close().await;
    drop(writer);
    let path = root.path().join(segment.id.to_string());
    let before = std::fs::read(path.join(HNSW_HEADER_FILE)).unwrap();
    std::fs::remove_file(path.join(METADATA_FILE)).unwrap();
    let persist_path = Some(root.path().to_str().unwrap().to_string());
    assert!(LocalHnswSegmentReader::from_segment(
        &collection,
        &segment,
        3,
        persist_path.clone(),
        sqlite.clone()
    )
    .await
    .is_err());
    assert!(LocalHnswSegmentWriter::from_segment(
        &collection,
        &segment,
        3,
        persist_path,
        sqlite.clone()
    )
    .await
    .is_err());
    assert_eq!(
        (
            std::fs::read(path.join(HNSW_HEADER_FILE)).unwrap(),
            get_current_seq_id(&segment, &sqlite).await.unwrap()
        ),
        (before, 42)
    );
}

#[tokio::test]
async fn persistence_reopens_evicted_files_and_empty_checkpoints_reload() {
    let (root, sqlite, collection, segment, mut writer) = fixture().await;
    writer.index.inner.write().await.sync_threshold = 1;
    writer.index.close().await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(1, 1, 0)].into()))
        .await
        .unwrap();
    writer.index.close().await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(2, 1, 3)].into()))
        .await
        .unwrap();
    writer.index.close().await;
    drop(writer);
    let reader = LocalHnswSegmentReader::from_segment(
        &collection,
        &segment,
        3,
        Some(root.path().to_str().unwrap().into()),
        sqlite,
    )
    .await
    .unwrap();
    let guard = reader.index.inner.read().await;
    assert_eq!(
        (
            guard.last_seen_seq_id,
            guard.index.len(),
            guard.id_map.total_elements_added
        ),
        (2, 0, 1)
    );
}

#[tokio::test]
async fn initialized_files_without_pickle_are_valid_until_first_persist() {
    let (root, _sqlite, _collection, segment, mut writer) = fixture().await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(1, 1, 0)].into()))
        .await
        .unwrap();
    let path = root.path().join(segment.id.to_string());
    let info = inspect_persisted_hnsw_index(&path).unwrap();
    assert_eq!(
        (
            info.elements,
            info.dimensionality,
            path.join(METADATA_FILE).exists()
        ),
        (0, 3, false)
    );
}

// Simulate a process dying at each publication boundary without invoking a
// destructor that could hide an incomplete checkpoint.
#[tokio::test]
async fn interrupted_checkpoints_replay_add_update_and_delete() {
    for first_save in [false, true] {
        for publish_pickle in [false, true] {
            let (root, sqlite, collection, segment, mut writer) = fixture().await;
            let initial = vec![record(1, 1, 0), record(2, 2, 0)];
            writer
                .apply_log_chunk(Chunk::new(initial.clone().into()))
                .await
                .unwrap();
            if !first_save {
                writer.index.inner.write().await.sync_threshold = 1;
                writer
                    .apply_log_chunk(Chunk::new(vec![record(3, 9, 2)].into()))
                    .await
                    .unwrap();
                writer.index.inner.write().await.sync_threshold = 1000;
            }
            let tail = vec![record(4, 1, 2), record(5, 2, 3), record(6, 3, 0)];
            writer
                .apply_log_chunk(Chunk::new(tail.clone().into()))
                .await
                .unwrap();
            if publish_pickle {
                drop(persist(writer.index.inner.write().await).await.unwrap());
            } else {
                writer.index.inner.write().await.index.save().unwrap();
            }
            writer.index.close().await;
            drop(writer);
            let folder = root.path().join(segment.id.to_string());
            let inspection = inspect_persisted_hnsw_index(&folder).unwrap();
            assert_eq!(inspection.recovery_required, !publish_pickle);
            let persist_path = Some(root.path().to_str().unwrap().to_string());
            let mut reopened = LocalHnswSegmentWriter::from_segment(
                &collection,
                &segment,
                3,
                persist_path.clone(),
                sqlite.clone(),
            )
            .await
            .unwrap();
            let records = if first_save {
                [initial, tail].concat()
            } else {
                tail
            };
            reopened.index.inner.write().await.sync_threshold = 1;
            reopened
                .apply_log_chunk(Chunk::new(records.into()))
                .await
                .unwrap();
            reopened.index.close().await;
            drop(reopened);
            let reader = LocalHnswSegmentReader::from_segment(
                &collection,
                &segment,
                3,
                persist_path,
                sqlite.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"1".to_string())
                    .await
                    .unwrap(),
                vec![4.0; 3]
            );
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"3".to_string())
                    .await
                    .unwrap(),
                vec![6.0; 3]
            );
            assert!(reader
                .get_offset_id_by_user_id(&"2".to_string())
                .await
                .is_err());
            assert_eq!(reader.index.inner.read().await.index.len(), 2);
            assert_eq!(get_current_seq_id(&segment, &sqlite).await.unwrap(), 6);
            assert!(
                !inspect_persisted_hnsw_index(&folder)
                    .unwrap()
                    .recovery_required
            );
        }
    }
}

#[tokio::test]
async fn updates_keep_one_slot_and_survive_reload() {
    let (root, sqlite, collection, segment, mut writer) = fixture().await;
    writer.index.inner.write().await.sync_threshold = 1;
    for offset in 1..100 {
        writer
            .apply_log_chunk(Chunk::new(vec![record(offset, 1, 1)].into()))
            .await
            .unwrap();
    }
    assert_eq!(writer.index.inner.read().await.index.len_with_deleted(), 1);
    writer.index.close().await;
    drop(writer);
    let reader = LocalHnswSegmentReader::from_segment(
        &collection,
        &segment,
        3,
        Some(root.path().to_str().unwrap().into()),
        sqlite,
    )
    .await
    .unwrap();
    assert_eq!(
        reader
            .get_embedding_by_user_id(&"1".to_string())
            .await
            .unwrap(),
        vec![99.0; 3]
    );
}

#[tokio::test]
async fn partial_mutation_invalidates_all_handles_without_publishing_maps() {
    let (_root, sqlite, _collection, segment, mut writer) = fixture().await;
    let reader = LocalHnswSegmentReader::from_index(writer.index.clone());
    writer.index.inner.write().await.mutation_budget = Some(std::sync::atomic::AtomicUsize::new(1));
    let chunk = Chunk::new(vec![record(1, 1, 0), record(2, 1, 2)].into());
    assert!(writer.apply_log_chunk(chunk.clone()).await.is_err());
    {
        let guard = writer.index.inner.read().await;
        assert_eq!(guard.index.len(), 1); // The first native mutation succeeded.
        assert!(guard.id_map.id_to_label.is_empty());
        assert_eq!(guard.id_map.total_elements_added, 0);
        assert_eq!(guard.last_seen_seq_id, 0);
    }
    assert!(writer.index.ensure_usable().await.is_err());
    assert!(writer.apply_log_chunk(chunk).await.is_err());
    assert!(reader.query_embedding(&[], vec![0.0; 3], 10).await.is_err());
    assert_eq!(get_current_seq_id(&segment, &sqlite).await.unwrap(), 0);
}

#[tokio::test]
async fn resize_failure_does_not_publish_staged_maps() {
    let (_root, _sqlite, _collection, _segment, mut writer) = fixture().await;
    writer.index.inner.write().await.fail_resize = true;
    let records = (1..=200)
        .map(|offset| {
            let mut log = record(offset, 0, 0);
            log.record.id = offset.to_string();
            log
        })
        .collect::<Vec<_>>();
    let chunk = Chunk::new(records.into());
    assert!(writer.apply_log_chunk(chunk.clone()).await.is_err());
    {
        let guard = writer.index.inner.read().await;
        assert!(guard.id_map.id_to_label.is_empty());
        assert_eq!(guard.id_map.total_elements_added, 0);
        assert_eq!(guard.index.len(), 0);
    }
    writer.index.inner.write().await.fail_resize = false;
    writer.apply_log_chunk(chunk).await.unwrap();
    assert_eq!(writer.index.inner.read().await.index.len(), 200);
}

#[tokio::test]
async fn unsafe_header_values_are_rejected_before_load() {
    let (root, _sqlite, _collection, segment, mut writer) = fixture().await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(1, 1, 0)].into()))
        .await
        .unwrap();
    drop(persist(writer.index.inner.write().await).await.unwrap());
    writer.index.close().await;
    drop(writer);
    let folder = root.path().join(segment.id.to_string());
    let header_path = folder.join(HNSW_HEADER_FILE);
    let header = std::fs::read(&header_path).unwrap();
    let word = std::mem::size_of::<usize>();
    let cases = [
        (4 + word, usize::MAX.to_ne_bytes().to_vec()),
        (4 + 8 * word + 8, 17usize.to_ne_bytes().to_vec()),
        (4 + 9 * word + 8, f64::NAN.to_ne_bytes().to_vec()),
        (4 + 9 * word + 8, f64::INFINITY.to_ne_bytes().to_vec()),
        (4 + 9 * word + 8, (-1.0f64).to_ne_bytes().to_vec()),
        (4 + 9 * word + 8, 1e100f64.to_ne_bytes().to_vec()),
    ];
    for (offset, bytes) in cases {
        let mut invalid = header.clone();
        invalid[offset..offset + bytes.len()].copy_from_slice(&bytes);
        std::fs::write(&header_path, invalid).unwrap();
        assert!(inspect_persisted_hnsw_index(&folder).is_err());
    }
}

#[tokio::test]
async fn upper_level_links_require_a_neighbor_at_that_level() {
    let (root, _sqlite, _collection, segment, mut writer) = fixture().await;
    let records = (1..=200)
        .map(|offset| {
            let mut log = record(offset, 0, 0);
            log.record.id = offset.to_string();
            log
        })
        .collect::<Vec<_>>();
    writer
        .apply_log_chunk(Chunk::new(records.into()))
        .await
        .unwrap();
    drop(persist(writer.index.inner.write().await).await.unwrap());
    writer.index.close().await;
    drop(writer);
    let folder = root.path().join(segment.id.to_string());
    assert!(inspect_persisted_hnsw_index(&folder).is_ok());
    let path = folder.join("link_lists.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    let mut offset = 0;
    let mut zero_level = None;
    let mut upper_link = None;
    for node in 0..200u32 {
        let len = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if len == 0 {
            zero_level = Some(node);
        } else {
            upper_link = Some(offset + 4);
        }
        offset += 4 + len;
    }
    let offset = upper_link.unwrap();
    bytes[offset..offset + 4].copy_from_slice(&1u32.to_ne_bytes());
    bytes[offset + 4..offset + 8].copy_from_slice(&zero_level.unwrap().to_ne_bytes());
    std::fs::write(path, bytes).unwrap();
    assert!(inspect_persisted_hnsw_index(&folder).is_err());
}

#[tokio::test]
async fn exhausted_labels_do_not_change_the_map_or_graph() {
    let (_root, _sqlite, _collection, _segment, mut writer) = fixture().await;
    writer.index.inner.write().await.id_map.total_elements_added = u32::MAX - 1;
    assert!(matches!(
        writer
            .apply_log_chunk(Chunk::new(vec![record(1, 1, 0)].into()))
            .await,
        Err(LocalHnswSegmentWriterError::LabelExhausted)
    ));
    let guard = writer.index.inner.read().await;
    assert!(guard.id_map.id_to_label.is_empty());
    assert_eq!(guard.index.len(), 0);
    assert_eq!(guard.last_seen_seq_id, 0);
}
