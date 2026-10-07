use super::*;
use chroma_sqlite::db::test_utils::get_new_sqlite_db;
use chroma_types::{
    Collection, InternalCollectionConfiguration, KnnIndex, OperationRecord, Schema, SegmentScope,
    SegmentType, SegmentUuid, VectorIndexConfiguration,
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

#[tokio::test]
async fn persisted_capacity_is_bounded_by_native_slots() {
    for count in [0usize, 1, 101] {
        let (root, sqlite, collection, segment, mut writer) = fixture().await;
        let records = (1..=count)
            .map(|offset| record(offset as i64, offset as u8, 0))
            .collect::<Vec<_>>();
        writer
            .apply_log_chunk(Chunk::new(records.into()))
            .await
            .unwrap();
        // The bound must count tombstones as well as live vectors.
        if count > 1 {
            writer
                .apply_log_chunk(Chunk::new(vec![record(count as i64 + 1, 1, 3)].into()))
                .await
                .unwrap();
        }
        drop(persist(writer.index.inner.write().await).await.unwrap());
        writer.index.close().await;
        drop(writer);
        let folder = root.path().join(segment.id.to_string());
        let header_path = folder.join(HNSW_HEADER_FILE);
        let mut header = std::fs::read(&header_path).unwrap();
        let word = std::mem::size_of::<usize>();
        let capacity_offset = 4 + word;
        let limit = (count * 10).max(1000);
        let persist_path = Some(root.path().to_str().unwrap().to_owned());
        for capacity in [limit + 1, usize::try_from(1u64 << 40).unwrap_or(usize::MAX)] {
            header[capacity_offset..capacity_offset + word]
                .copy_from_slice(&capacity.to_ne_bytes());
            std::fs::write(&header_path, &header).unwrap();
            // Assert rejection before exercising a loader, so a regression
            // cannot make this test attempt the oversized native allocation.
            assert!(inspect_persisted_hnsw_index(&folder).is_err());
            assert!(inspect_persisted_hnsw_index_for_config_repair(&folder).is_err());
            assert!(LocalHnswSegmentReader::from_segment(
                &collection,
                &segment,
                3,
                persist_path.clone(),
                sqlite.clone(),
            )
            .await
            .is_err());
            assert!(LocalHnswSegmentWriter::from_segment(
                &collection,
                &segment,
                3,
                persist_path.clone(),
                sqlite.clone(),
            )
            .await
            .is_err());
            assert_eq!(get_current_seq_id(&segment, &sqlite).await.unwrap(), 0);
        }
        // Both the legacy initial allocation and the maximum supported growth
        // factor remain loadable, including a checkpoint containing a deletion.
        header[capacity_offset..capacity_offset + word].copy_from_slice(&limit.to_ne_bytes());
        std::fs::write(header_path, header).unwrap();
        assert_eq!(
            inspect_persisted_hnsw_index(&folder).unwrap().elements,
            count
        );
        let reader =
            LocalHnswSegmentReader::from_segment(&collection, &segment, 3, persist_path, sqlite)
                .await
                .unwrap();
        assert_eq!(
            reader.index.inner.read().await.index.len(),
            count - usize::from(count > 1)
        );
    }
}

#[tokio::test]
async fn minimum_neighbors_rejects_one_and_two_survives_reload() {
    for m in [1, 2] {
        let root = tempfile::tempdir().unwrap();
        let sqlite = get_new_sqlite_db().await;
        let mut collection = Collection::test_collection(3);
        let mut config = InternalCollectionConfiguration::default_hnsw();
        if let VectorIndexConfiguration::Hnsw(hnsw) = &mut config.vector_index {
            hnsw.max_neighbors = m;
            hnsw.sync_threshold = 2;
        }
        collection.schema = Some(Schema::try_from(&config).unwrap());
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let persist_path = Some(root.path().to_str().unwrap().to_owned());
        let writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &segment,
            3,
            persist_path.clone(),
            sqlite.clone(),
        )
        .await;
        if m == 1 {
            assert!(matches!(
                writer,
                Err(LocalHnswSegmentWriterError::InvalidHnswConfiguration(_))
            ));
            assert!(!root.path().join(segment.id.to_string()).exists());
            continue;
        }
        let mut writer = writer.unwrap();
        writer
            .apply_log_chunk(Chunk::new(vec![record(1, 1, 0), record(2, 2, 0)].into()))
            .await
            .unwrap();
        writer.index.close().await;
        drop(writer);
        let reader =
            LocalHnswSegmentReader::from_segment(&collection, &segment, 3, persist_path, sqlite)
                .await
                .unwrap();
        assert_eq!(
            reader
                .get_embedding_by_user_id(&"2".to_string())
                .await
                .unwrap(),
            vec![2.0; 3]
        );
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
                // Bypass pickle publication to simulate an interrupted save,
                // but use the same bounded stream lifetime as checkpointing.
                let guard = writer.index.inner.write().await;
                let _permit = acquire_hnsw_files().await;
                let files = HnswFiles(&guard.index);
                files.0.open_fd().unwrap();
                files.0.save().unwrap();
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
            // A raised threshold must not postpone publishing recovery of an
            // existing checkpoint. Other cases exercise ordinary persistence.
            reopened.index.inner.write().await.sync_threshold = if !first_save && !publish_pickle {
                1000
            } else {
                1
            };
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

#[tokio::test]
async fn checkpoint_offset_prevents_replaying_ignored_updates_after_restart() {
    for existing_watermark in [false, true] {
        for load_reader in [false, true] {
            let (root, sqlite, collection, segment, mut writer) = fixture().await;
            if existing_watermark {
                writer.index.inner.write().await.sync_threshold = 1;
                writer
                    .apply_log_chunk(Chunk::new(vec![record(1, 9, 0)].into()))
                    .await
                    .unwrap();
            }
            writer.index.inner.write().await.sync_threshold = 2;
            // The update is ignored because ID 1 does not exist until the add.
            let chunk = Chunk::new(vec![record(2, 1, 2), record(3, 1, 0)].into());
            sqlx::query("CREATE TRIGGER reject_checkpoint BEFORE INSERT ON max_seq_id BEGIN SELECT RAISE(ABORT, 'checkpoint failure'); END")
                .execute(sqlite.get_conn()).await.unwrap();
            assert!(matches!(
                writer.apply_log_chunk(chunk.clone()).await,
                Err(LocalHnswSegmentWriterError::MaxSeqIdUpdateError(_))
            ));
            assert_eq!(
                get_current_seq_id(&segment, &sqlite).await.unwrap(),
                u64::from(existing_watermark)
            );
            writer.index.close().await;
            drop(writer);
            // A failed watermark restore must fail loading, leaving replay retryable.
            assert!(LocalHnswSegmentReader::from_segment(
                &collection,
                &segment,
                3,
                Some(root.path().to_str().unwrap().to_owned()),
                sqlite.clone(),
            )
            .await
            .is_err());
            assert_eq!(
                get_current_seq_id(&segment, &sqlite).await.unwrap(),
                u64::from(existing_watermark)
            );
            sqlx::query("DROP TRIGGER reject_checkpoint")
                .execute(sqlite.get_conn())
                .await
                .unwrap();

            let persist_path = Some(root.path().to_str().unwrap().to_owned());
            let index = if load_reader {
                LocalHnswSegmentReader::from_segment(
                    &collection,
                    &segment,
                    3,
                    persist_path,
                    sqlite.clone(),
                )
                .await
                .unwrap()
                .index
            } else {
                LocalHnswSegmentWriter::from_segment(
                    &collection,
                    &segment,
                    3,
                    persist_path,
                    sqlite.clone(),
                )
                .await
                .unwrap()
                .index
            };
            assert_eq!(get_current_seq_id(&segment, &sqlite).await.unwrap(), 3);
            let reader = LocalHnswSegmentReader::from_index(index.clone());
            let mut reopened = LocalHnswSegmentWriter::from_index(index).unwrap();
            reopened.apply_log_chunk(chunk).await.unwrap();
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"1".to_owned())
                    .await
                    .unwrap(),
                vec![3.0; 3]
            );
            reopened
                .apply_log_chunk(Chunk::new(vec![record(4, 1, 2)].into()))
                .await
                .unwrap();
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"1".to_owned())
                    .await
                    .unwrap(),
                vec![4.0; 3]
            );
        }
    }
}

#[tokio::test]
async fn persisted_construction_limits_are_checked_before_watermark_restore() {
    let (root, sqlite, collection, segment, mut writer) = fixture().await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(1, 1, 0), record(2, 2, 0)].into()))
        .await
        .unwrap();
    drop(persist(writer.index.inner.write().await).await.unwrap());
    writer.index.close().await;
    drop(writer);
    let folder = root.path().join(segment.id.to_string());
    let header_path = folder.join(HNSW_HEADER_FILE);
    let header = std::fs::read(&header_path).unwrap();
    let word = std::mem::size_of::<usize>();
    let offset = 20 + 9 * word;
    let persist_path = Some(root.path().to_str().unwrap().to_owned());
    for value in [0usize, 4097, usize::MAX, 1, 4096] {
        let mut bytes = header.clone();
        bytes[offset..offset + word].copy_from_slice(&value.to_ne_bytes());
        std::fs::write(&header_path, bytes).unwrap();
        let valid = (1..=4096).contains(&value);
        assert_eq!(inspect_persisted_hnsw_index(&folder).is_ok(), valid);
        assert!(inspect_persisted_hnsw_index_for_config_repair(&folder).is_ok());
        let reader = LocalHnswSegmentReader::from_segment(
            &collection,
            &segment,
            3,
            persist_path.clone(),
            sqlite.clone(),
        )
        .await;
        assert_eq!(reader.is_ok(), valid);
        if let Ok(reader) = reader {
            reader.index.close().await;
        }
        let writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &segment,
            3,
            persist_path.clone(),
            sqlite.clone(),
        )
        .await;
        assert_eq!(writer.is_ok(), valid);
        if let Ok(writer) = writer {
            writer.index.close().await;
        }
        assert_eq!(
            get_current_seq_id(&segment, &sqlite).await.unwrap(),
            if valid { 2 } else { 0 }
        );
    }
}

#[tokio::test]
async fn checkpoint_restore_preserves_legacy_and_newer_sqlite_offsets() {
    let (_root, sqlite, _collection, segment, writer) = fixture().await;
    let mut map = IdMap::new(3);
    map.max_seq_id = Some(10);
    assert_eq!(
        restore_checkpoint_seq_id(&segment, &sqlite, &map)
            .await
            .unwrap(),
        10
    );
    map.max_seq_id = Some(20);
    // Legacy pickle offsets must not replace an existing SQLite watermark.
    assert_eq!(
        restore_checkpoint_seq_id(&segment, &sqlite, &map)
            .await
            .unwrap(),
        10
    );
    map.checkpoint_seq_id = Some(30);
    assert_eq!(
        restore_checkpoint_seq_id(&segment, &sqlite, &map)
            .await
            .unwrap(),
        30
    );
    map.checkpoint_seq_id = Some(25);
    assert_eq!(
        restore_checkpoint_seq_id(&segment, &sqlite, &map)
            .await
            .unwrap(),
        30
    );
    drop(writer);
}

#[tokio::test]
async fn config_repair_inspection_retains_structural_checks() {
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
    let original = std::fs::read(&header_path).unwrap();
    let word = std::mem::size_of::<usize>();
    let offset = 20 + 9 * word;
    let mut repairable = original.clone();
    repairable[offset..offset + word].copy_from_slice(&0usize.to_ne_bytes());
    std::fs::write(&header_path, &repairable).unwrap();
    assert!(inspect_persisted_hnsw_index_for_config_repair(&folder).is_ok());
    assert_eq!(std::fs::read(&header_path).unwrap(), repairable);
    assert!(inspect_persisted_hnsw_index(&folder).is_err());

    // Fixing only the construction setting makes ordinary validation succeed.
    std::fs::write(&header_path, &original).unwrap();
    assert!(inspect_persisted_hnsw_index(&folder).is_ok());

    // A bad capacity or truncated header is not a configuration-only repair.
    let mut bad_capacity = repairable.clone();
    bad_capacity[4 + word..4 + 2 * word].copy_from_slice(&0usize.to_ne_bytes());
    for bytes in [bad_capacity, repairable[..offset].to_vec()] {
        std::fs::write(&header_path, bytes).unwrap();
        assert!(inspect_persisted_hnsw_index_for_config_repair(&folder).is_err());
    }
    std::fs::write(&header_path, &repairable).unwrap();
    // Neither missing graph bytes nor a broken ID map can use the escape hatch.
    for name in ["data_level0.bin", METADATA_FILE] {
        let path = folder.join(name);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, []).unwrap();
        assert!(inspect_persisted_hnsw_index_for_config_repair(&folder).is_err());
        std::fs::write(path, bytes).unwrap();
    }
}

#[tokio::test]
async fn checkpoint_validation_reuses_successful_saves_and_detects_external_changes() {
    let (root, _sqlite, _collection, segment, mut writer) = fixture().await;
    writer.index.set_sync_threshold(1).await;
    writer
        .apply_log_chunk(Chunk::new(vec![record(1, 1, 0)].into()))
        .await
        .unwrap();
    let path = root.path().join(segment.id.to_string());
    assert!(writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
    assert!(!writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
    for offset in 2..=5 {
        writer
            .apply_log_chunk(Chunk::new(vec![record(offset, 1, 1)].into()))
            .await
            .unwrap();
        assert!(
            !writer
                .index
                .validate_persisted_checkpoint(&path)
                .await
                .unwrap(),
            "our successful save should preserve validation"
        );
    }
    // A file replacement before our own save must not be blessed merely
    // because that save succeeds. Inspect the resulting checkpoint again.
    let header = path.join(HNSW_HEADER_FILE);
    let bytes = std::fs::read(&header).unwrap();
    let replacement = path.join("replacement.bin");
    std::fs::write(&replacement, bytes).unwrap();
    writer.index.close().await;
    std::fs::rename(&replacement, &header).unwrap();
    writer
        .apply_log_chunk(Chunk::new(vec![record(6, 1, 1)].into()))
        .await
        .unwrap();
    assert!(writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
    // Same-size edits must also invalidate validation, not merely truncations.
    let data_path = path.join("data_level0.bin");
    let saved = std::fs::read(&data_path).unwrap();
    let mut corrupt = saved.clone();
    corrupt[..4].copy_from_slice(&u32::MAX.to_ne_bytes());
    std::fs::write(&data_path, corrupt).unwrap();
    assert!(writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .is_err());
    std::fs::write(&data_path, saved).unwrap();
    assert!(writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
    // A failed metadata publication must not retain the previous validation.
    let pickle = path.join(METADATA_FILE);
    let saved = std::fs::read(&pickle).unwrap();
    std::fs::remove_file(&pickle).unwrap();
    std::fs::create_dir(&pickle).unwrap();
    assert!(writer
        .apply_log_chunk(Chunk::new(vec![record(7, 1, 1)].into()))
        .await
        .is_err());
    assert!(writer
        .index
        .inner
        .read()
        .await
        .verified_checkpoint
        .is_none());
    std::fs::remove_dir(&pickle).unwrap();
    std::fs::write(&pickle, saved).unwrap();
    // Live replay now has no new records; still retry the failed checkpoint.
    writer
        .apply_log_chunk(Chunk::new(vec![].into()))
        .await
        .unwrap();
    assert!(writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
    assert!(!writer
        .index
        .validate_persisted_checkpoint(&path)
        .await
        .unwrap());
}
