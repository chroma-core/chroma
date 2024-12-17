use chroma_benchmark::datasets::sift::Sift1MData;
use chroma_types::{
    Chunk, CollectionUuid, DirectWhereComparison, LogRecord, MetadataSetValue, Operation,
    OperationRecord, SetOperator, Where, WhereComparison,
};
use indicatif::ProgressIterator;
use worker::{
    execution::operators::{
        fetch_log::FetchLogOperator, filter::FilterOperator, limit::LimitOperator,
        projection::ProjectionOperator,
    },
    log::{
        log::{InMemoryLog, Log},
        test::modulo_metadata,
    },
    segment::test::TestSegment,
};

const DATA_CHUNK_SIZE: usize = 10000;

pub async fn sift1m_segments() -> TestSegment {
    let mut segments = TestSegment::default();
    let mut sift1m = Sift1MData::init()
        .await
        .expect("Should be able to download Sift1M data");

    for chunk_start in (0..Sift1MData::collection_size())
        .step_by(DATA_CHUNK_SIZE)
        .progress()
        .with_message("Loading Sift1M Data")
    {
        let embedding_chunk = sift1m
            .data_range(chunk_start..(chunk_start + DATA_CHUNK_SIZE))
            .await
            .expect("Should be able to decode data chunk");

        let log_records = embedding_chunk
            .into_iter()
            .enumerate()
            .map(|(index, embedding)| LogRecord {
                log_offset: (chunk_start + index) as i64,
                record: OperationRecord {
                    id: (chunk_start + index).to_string(),
                    embedding: Some(embedding),
                    encoding: None,
                    metadata: Some(modulo_metadata(chunk_start + index)),
                    document: None,
                    operation: Operation::Add,
                },
            })
            .collect::<Vec<_>>();
        segments
            .compact_log(Chunk::new(log_records.into()), chunk_start)
            .await;
    }
    segments
}

pub fn empty_fetch_log(collection_uuid: CollectionUuid) -> FetchLogOperator {
    FetchLogOperator {
        log_client: Log::InMemory(InMemoryLog::default()).into(),
        batch_size: 100,
        start_log_offset_id: 0,
        maximum_fetch_count: Some(0),
        collection_uuid,
    }
}

pub fn trivial_filter() -> FilterOperator {
    FilterOperator {
        query_ids: None,
        where_clause: None,
    }
}

pub fn always_false_filter_for_modulo_metadata() -> FilterOperator {
    FilterOperator {
        query_ids: None,
        where_clause: Some(Where::disjunction(vec![
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "is_even".to_string(),
                comparison: WhereComparison::Set(
                    SetOperator::NotIn,
                    MetadataSetValue::Bool(vec![false, true]),
                ),
            }),
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_3".to_string(),
                comparison: WhereComparison::Set(
                    SetOperator::NotIn,
                    MetadataSetValue::Int(vec![0, 1, 2]),
                ),
            }),
        ])),
    }
}

pub fn always_true_filter_for_modulo_metadata() -> FilterOperator {
    FilterOperator {
        query_ids: None,
        where_clause: Some(Where::conjunction(vec![
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "is_even".to_string(),
                comparison: WhereComparison::Set(
                    SetOperator::In,
                    MetadataSetValue::Bool(vec![false, true]),
                ),
            }),
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_3".to_string(),
                comparison: WhereComparison::Set(
                    SetOperator::In,
                    MetadataSetValue::Int(vec![0, 1, 2]),
                ),
            }),
        ])),
    }
}

pub fn trivial_limit() -> LimitOperator {
    LimitOperator {
        skip: 0,
        fetch: Some(100),
    }
}

pub fn offset_limit() -> LimitOperator {
    LimitOperator {
        skip: 100,
        fetch: Some(100),
    }
}

pub fn trivial_projection() -> ProjectionOperator {
    ProjectionOperator {
        document: false,
        embedding: false,
        metadata: false,
    }
}

pub fn all_projection() -> ProjectionOperator {
    ProjectionOperator {
        document: true,
        embedding: true,
        metadata: true,
    }
}
