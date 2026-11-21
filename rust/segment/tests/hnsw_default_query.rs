use chroma_distance::DistanceFunction;
use chroma_index::{HnswIndex, HnswIndexConfig, IndexConfig, IndexUuid};
use chroma_segment::local_hnsw::{LocalHnswSegmentReader, LocalHnswSegmentWriter};
use chroma_sqlite::db::test_utils::get_new_sqlite_db;
use chroma_types::{
    test_segment, Chunk, Collection, InternalCollectionConfiguration, InternalHnswConfiguration,
    LogRecord, Operation, OperationRecord, Schema, SegmentScope, SegmentType,
    VectorIndexConfiguration,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

const DIM: usize = 12;
const WIDE: usize = 96;

fn points() -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(7963);
    (0..WIDE)
        .map(|_| (0..DIM).map(|_| rng.gen_range(0..32) as f32).collect())
        .collect()
}

#[test]
fn index_query_uses_configured_breadth() {
    let points = points();
    for default_ef in [1, WIDE] {
        let index = HnswIndex::init(
            &IndexConfig::new(DIM as i32, DistanceFunction::Euclidean),
            Some(&HnswIndexConfig::new_ephemeral(3, 12, default_ef)),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .unwrap();
        for (id, point) in points.iter().enumerate() {
            index.add(id, point).unwrap();
        }

        let mut distinguished = false;
        for (id, point) in points.iter().enumerate() {
            let low = index.query_with_ef(point, 1, &[], &[], Some(1)).unwrap();
            let high = index.query_with_ef(point, 1, &[], &[], Some(WIDE)).unwrap();
            if low.0 != high.0 && high == (vec![id], vec![0.0]) {
                let expected = if default_ef == 1 { low } else { high };
                assert_eq!(index.query(point, 1, &[], &[]).unwrap(), expected);
                distinguished = true;
                break;
            }
        }
        assert!(
            distinguished,
            "the graph must distinguish narrow and wide searches"
        );
    }
}

#[tokio::test]
async fn segment_query_embedding_uses_configured_breadth() {
    let points = points();
    for default_ef in [1, WIDE] {
        let mut collection = Collection::test_collection(DIM as i32);
        collection.schema = Some(
            Schema::try_from(&InternalCollectionConfiguration {
                vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                    max_neighbors: 3,
                    ef_construction: 12,
                    ef_search: default_ef,
                    ..Default::default()
                }),
                embedding_function: None,
            })
            .unwrap(),
        );
        let mut segment = test_segment(collection.collection_id, SegmentScope::VECTOR);
        segment.r#type = SegmentType::HnswLocalPersisted;
        let mut writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &segment,
            DIM,
            None,
            get_new_sqlite_db().await,
        )
        .await
        .unwrap();

        // Insert sequentially so parallel graph construction cannot change the fixture.
        for (i, point) in points.iter().enumerate() {
            let log = LogRecord {
                log_offset: i as i64 + 1,
                record: OperationRecord {
                    id: i.to_string(),
                    embedding: Some(point.clone()),
                    operation: Operation::Add,
                    encoding: None,
                    metadata: None,
                    document: None,
                },
            };
            writer
                .apply_log_chunk(Chunk::new(vec![log].into()))
                .await
                .unwrap();
        }

        let reader = LocalHnswSegmentReader::from_index(writer.index);
        let mut distinguished = false;
        for (i, point) in points.iter().enumerate() {
            let low = reader
                .query_embedding_with_ef(&[], point.clone(), 1, Some(1))
                .await
                .unwrap();
            let high = reader
                .query_embedding_with_ef(&[], point.clone(), 1, Some(WIDE))
                .await
                .unwrap();
            let id = reader
                .get_offset_id_by_user_id(&i.to_string())
                .await
                .unwrap();
            if low[0].offset_id != high[0].offset_id
                && (high[0].offset_id, high[0].measure) == (id, 0.0)
            {
                let expected = if default_ef == 1 { &low[0] } else { &high[0] };
                let actual = reader.query_embedding(&[], point.clone(), 1).await.unwrap();
                assert_eq!(actual.len(), 1);
                assert_eq!(
                    (actual[0].offset_id, actual[0].measure),
                    (expected.offset_id, expected.measure)
                );
                distinguished = true;
                break;
            }
        }
        assert!(
            distinguished,
            "the graph must distinguish narrow and wide searches"
        );
    }
}
