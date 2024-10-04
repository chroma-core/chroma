use std::collections::HashMap;
use std::iter::once;

use chroma_test::benchmark::{bench, tokio_multi_thread};
use chroma_test::log::{offset_as_id, random_document, random_embedding, LogGenerator};
use chroma_test::segment::CompactSegment;
use chroma_types::{
    BooleanOperator, Chunk, DirectDocumentComparison, DirectWhereComparison, DocumentOperator,
    MetadataValue, Operation, OperationRecord, PrimitiveOperator, UpdateMetadataValue, Where,
    WhereChildren, WhereComparison,
};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use worker::execution::operator::Operator;
use worker::execution::operators::metadata_filtering::{
    MetadataFilteringInput, MetadataFilteringOperator,
};

const DOCUMENT_LENGTH: usize = 64;
const EMBEDDING_DIMENSION: usize = 6;
const PRIMES: [usize; 8] = [2, 3, 5, 7, 11, 13, 17, 19];

fn modulo_metadata(id: usize) -> HashMap<String, UpdateMetadataValue> {
    PRIMES
        .iter()
        .map(|p| {
            (
                format!("modulo_{p}"),
                UpdateMetadataValue::Int((id % p) as i64),
            )
        })
        .chain(once((
            "val".to_string(),
            UpdateMetadataValue::Int(id as i64),
        )))
        .collect()
}

fn log_generator(id: usize) -> OperationRecord {
    OperationRecord {
        id: offset_as_id(id),
        embedding: Some(random_embedding(EMBEDDING_DIMENSION)),
        encoding: None,
        metadata: Some(modulo_metadata(id)),
        document: Some(random_document(DOCUMENT_LENGTH)),
        operation: Operation::Add,
    }
}

fn baseline_where_clauses() -> Vec<(&'static str, Option<Where>)> {
    use BooleanOperator::*;
    use DocumentOperator::*;
    use MetadataValue::*;
    use PrimitiveOperator::*;
    use WhereComparison::*;
    vec![
        (
            "$eq",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_11".to_string(),
                comparison: Primitive(Equal, Int(6)),
            }),
        ),
        (
            "$ne",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_11".to_string(),
                comparison: Primitive(NotEqual, Int(6)),
            }),
        ),
        (
            "$gt-small",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_11".to_string(),
                comparison: Primitive(GreaterThan, Int(6)),
            }),
        ),
        (
            "$gt-large",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "val".to_string(),
                comparison: Primitive(GreaterThan, Int(0)),
            }),
        ),
        (
            "$and-[$ne, $eq]",
            Where::WhereChildren(WhereChildren {
                operator: And,
                children: vec![
                    Where::DirectWhereComparison(DirectWhereComparison {
                        key: "modulo_11".to_string(),
                        comparison: Primitive(NotEqual, Int(6)),
                    }),
                    Where::DirectWhereComparison(DirectWhereComparison {
                        key: "modulo_2".to_string(),
                        comparison: Primitive(Equal, Int(0)),
                    }),
                ],
            }),
        ),
        (
            "$contains",
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: random_document(4),
                operator: Contains,
            }),
        ),
    ]
    .into_iter()
    .map(|(s, w)| (s, Some(w)))
    .chain(once(("$true", None)))
    .collect()
}

fn bench_metadata_filtering(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let logen = LogGenerator {
        generator: log_generator,
    };

    let routine = |metadata_filter_input| async move {
        MetadataFilteringOperator::new()
            .run(&metadata_filter_input)
            .await
            .expect("Metadata filtering should not fail.");
    };

    for record_count in [1000, 10000, 100000] {
        let mut compact = CompactSegment::default();
        runtime.block_on(async { compact.populate_with_generator(record_count, &logen).await });

        for (op, where_clause) in baseline_where_clauses() {
            let setup = || {
                MetadataFilteringInput::new(
                    compact.blockfile_provider.clone(),
                    compact.record.clone(),
                    compact.metadata.clone(),
                    Chunk::new(Vec::new().into()),
                    None,
                    where_clause.clone(),
                    None,
                    None,
                )
            };
            bench(
                format!("metadata-filtering-{}-{}", record_count, op).as_str(),
                criterion,
                &runtime,
                setup,
                routine,
            );
        }
    }
}

criterion_group!(benches, bench_metadata_filtering);
criterion_main!(benches);
