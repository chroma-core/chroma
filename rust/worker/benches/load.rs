use chroma_benchmark::datasets::sift::Sift1MData;
use chroma_log::{in_memory_log::InMemoryLog, test::modulo_metadata, Log};
use chroma_segment::test::TestDistributedSegment;
use chroma_types::{
    operator::{Filter, Limit, Projection},
    Chunk, CollectionUuid, LogRecord, MetadataComparison, MetadataExpression, MetadataSetValue,
    Operation, OperationRecord, SetOperator, Where,
};
use indicatif::ProgressIterator;
use worker::execution::operators::fetch_log::FetchLogOperator;

const DATA_CHUNK_SIZE: usize = 10000;

pub async fn sift1m_segments() -> TestDistributedSegment {
    let mut segments = TestDistributedSegment::new().await;
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
        Box::pin(segments.compact_log(Chunk::new(log_records.into()), chunk_start)).await;
    }
    segments
}

pub fn empty_fetch_log(collection_uuid: CollectionUuid) -> FetchLogOperator {
    FetchLogOperator {
        log_client: Log::InMemory(InMemoryLog::default()),
        batch_size: 100,
        start_log_offset_id: 0,
        maximum_fetch_count: Some(0),
        collection_uuid,
        tenant: "default_tenant".to_string(),
    }
}

pub fn trivial_filter() -> Filter {
    Filter {
        query_ids: None,
        where_clause: None,
    }
}

pub fn always_false_filter_for_modulo_metadata() -> Filter {
    Filter {
        query_ids: None,
        where_clause: Some(Where::disjunction(vec![
            Where::Metadata(MetadataExpression {
                key: "is_even".to_string(),
                comparison: MetadataComparison::Set(
                    SetOperator::NotIn,
                    MetadataSetValue::Bool(vec![false, true]),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "modulo_3".to_string(),
                comparison: MetadataComparison::Set(
                    SetOperator::NotIn,
                    MetadataSetValue::Int(vec![0, 1, 2]),
                ),
            }),
        ])),
    }
}

pub fn always_true_filter_for_modulo_metadata() -> Filter {
    Filter {
        query_ids: None,
        where_clause: Some(Where::conjunction(vec![
            Where::Metadata(MetadataExpression {
                key: "is_even".to_string(),
                comparison: MetadataComparison::Set(
                    SetOperator::In,
                    MetadataSetValue::Bool(vec![false, true]),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "modulo_3".to_string(),
                comparison: MetadataComparison::Set(
                    SetOperator::In,
                    MetadataSetValue::Int(vec![0, 1, 2]),
                ),
            }),
        ])),
    }
}

pub fn trivial_limit() -> Limit {
    Limit {
        offset: 0,
        limit: Some(100),
    }
}

pub fn offset_limit() -> Limit {
    Limit {
        offset: 100,
        limit: Some(100),
    }
}

pub fn trivial_projection() -> Projection {
    Projection {
        document: false,
        embedding: false,
        metadata: false,
    }
}

pub fn all_projection() -> Projection {
    Projection {
        document: true,
        embedding: true,
        metadata: true,
    }
}
