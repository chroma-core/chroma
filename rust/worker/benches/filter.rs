use std::iter::once;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_log::test::{upsert_generator, LoadFromGenerator};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::Operator;
use chroma_types::operator::Filter;
use chroma_types::{
    BooleanOperator, Chunk, CompositeExpression, MetadataComparison, MetadataExpression,
    MetadataValue, PrimitiveOperator, Where,
};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use worker::execution::operators::filter::FilterInput;

fn baseline_where_clauses() -> Vec<(&'static str, Option<Where>)> {
    use BooleanOperator::*;
    use MetadataComparison::*;
    use MetadataValue::*;
    use PrimitiveOperator::*;
    vec![
        (
            "$eq",
            Where::Metadata(MetadataExpression {
                key: "modulo_3".to_string(),
                comparison: Primitive(Equal, Int(0)),
            }),
        ),
        (
            "$ne",
            Where::Metadata(MetadataExpression {
                key: "modulo_3".to_string(),
                comparison: Primitive(NotEqual, Int(0)),
            }),
        ),
        (
            "$gt-small",
            Where::Metadata(MetadataExpression {
                key: "modulo_3".to_string(),
                comparison: Primitive(GreaterThan, Int(0)),
            }),
        ),
        (
            "$gt-large",
            Where::Metadata(MetadataExpression {
                key: "id".to_string(),
                comparison: Primitive(GreaterThan, Int(0)),
            }),
        ),
        (
            "$and-[$ne, $eq]",
            Where::Composite(CompositeExpression {
                operator: And,
                children: vec![
                    Where::Metadata(MetadataExpression {
                        key: "is_even".to_string(),
                        comparison: Primitive(NotEqual, Bool(false)),
                    }),
                    Where::Metadata(MetadataExpression {
                        key: "modulo_3".to_string(),
                        comparison: Primitive(Equal, Int(1)),
                    }),
                ],
            }),
        ),
    ]
    .into_iter()
    .map(|(s, w)| (s, Some(w)))
    .chain(once(("$true", None)))
    .collect()
}

fn bench_filter(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();

    for record_count in [1000, 10000, 100000] {
        let test_segment = runtime.block_on(async {
            let mut segment = TestDistributedSegment::new().await;
            segment
                .populate_with_generator(record_count, upsert_generator)
                .await;
            segment
        });

        let filter_input = FilterInput {
            logs: Chunk::new(Vec::new().into()),
            blockfile_provider: test_segment.blockfile_provider,
            metadata_segment: test_segment.metadata_segment,
            record_segment: test_segment.record_segment,
        };

        for (op, where_clause) in baseline_where_clauses() {
            let filter_operator = Filter {
                query_ids: None,
                where_clause: where_clause.clone(),
            };

            let routine = |(op, input): (Filter, FilterInput)| async move {
                op.run(&input)
                    .await
                    .expect("FilterOperator should not fail");
            };

            let setup = || (filter_operator.clone(), filter_input.clone());

            bench_run(
                format!("filter-{}-{}", record_count, op).as_str(),
                criterion,
                &runtime,
                setup,
                routine,
            );
        }
    }
}

criterion_group!(benches, bench_filter);
criterion_main!(benches);
