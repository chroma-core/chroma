use super::*;
use chroma_segment::local_hnsw::LocalHnswSegmentWriter;
use chroma_sqlite::db::test_utils::get_new_sqlite_db;
use chroma_types::{
    Chunk, Collection, InternalCollectionConfiguration, InternalHnswConfiguration, LogRecord,
    Operation, OperationRecord, Schema, Segment, SegmentScope, SegmentUuid,
    VectorIndexConfiguration,
};
use proptest::prelude::*;

async fn fixture(persist: bool) -> (tempfile::TempDir, SegmentRow) {
    let root = tempfile::tempdir().unwrap();
    let sqlite = get_new_sqlite_db().await;
    let mut collection = Collection::test_collection(3);
    collection.config = InternalCollectionConfiguration {
        vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
            sync_threshold: 2,
            ..Default::default()
        }),
        embedding_function: None,
    };
    collection.schema = Some(Schema::try_from(&collection.config).unwrap());
    let segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::HnswLocalPersisted,
        scope: SegmentScope::VECTOR,
        collection: collection.collection_id,
        metadata: None,
        file_path: Default::default(),
    };
    let mut writer = LocalHnswSegmentWriter::from_segment(
        &collection,
        &segment,
        3,
        Some(root.path().to_str().unwrap().into()),
        sqlite,
    )
    .await
    .unwrap();
    let records = (1..=if persist { 2 } else { 1 })
        .map(|offset| LogRecord {
            log_offset: offset,
            record: OperationRecord {
                id: offset.to_string(),
                embedding: Some(vec![1.0; 3]),
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        })
        .collect::<Vec<_>>();
    writer
        .apply_log_chunk(Chunk::new(records.into()))
        .await
        .unwrap();
    writer.index.close().await;
    let row = SegmentRow {
        collection_id: collection.collection_id,
        collection_name: collection.name,
        collection_dimension: Some(3),
        vector_segment_id: segment.id.to_string(),
        vector_max_seq_id: persist.then_some(2),
        metadata_max_seq_id: Some(if persist { 2 } else { 1 }),
    };
    (root, row)
}

fn inspect(path: &Path, segment: &SegmentRow) -> Vec<Issue> {
    let mut issues = vec![];
    inspect_segment(
        path,
        segment,
        LogState {
            topic: "topic".into(),
            row_count: 0,
            min_seq_id: None,
            max_seq_id: None,
            rows_at_or_below_vector_watermark: 0,
            rows_below_purge_watermark: 0,
        },
        &mut issues,
    );
    issues
}

#[tokio::test]
async fn healthy_indexes_pass_before_and_after_first_persist() {
    for persisted in [false, true] {
        let (root, row) = fixture(persisted).await;
        let issues = inspect(root.path(), &row);
        assert!(issues.is_empty(), "{issues:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn truncated_native_files_are_corrupt(file in 0..4usize, length in 0..4u64) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (root, row) = fixture(true).await;
            let path = root.path().join(&row.vector_segment_id).join(HNSW_INDEX_FILES[file]);
            std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(length).unwrap();
            let before = std::fs::read(&path).unwrap();
            let issues = inspect(root.path(), &row);
            assert!(issues.iter().any(|issue| issue.severity == Severity::Corrupt && issue.kind == "invalid_hnsw_index"), "{issues:?}");
            assert_eq!(std::fs::read(path).unwrap(), before);
        });
    }
}
