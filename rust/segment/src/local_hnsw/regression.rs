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
