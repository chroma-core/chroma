use std::iter::once;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_types::{
    BooleanOperator, Chunk, DirectWhereComparison, MetadataValue, PrimitiveOperator, Where,
    WhereChildren, WhereComparison,
};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use worker::execution::operator::Operator;
use worker::execution::operators::filter::{FilterInput, FilterOperator};
use worker::log::test::{upsert_generator, LogGenerator};
use worker::segment::test::TestSegment;

fn baseline_where_clauses() -> Vec<(&'static str, Option<Where>)> {
    use BooleanOperator::*;
    use MetadataValue::*;
    use PrimitiveOperator::*;
    use WhereComparison::*;
    vec![
        (
            "$eq",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_3".to_string(),
                comparison: Primitive(Equal, Int(0)),
            }),
        ),
        (
            "$ne",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_3".to_string(),
                comparison: Primitive(NotEqual, Int(0)),
            }),
        ),
        (
            "$gt-small",
            Where::DirectWhereComparison(DirectWhereComparison {
                key: "modulo_3".to_string(),
                comparison: Primitive(GreaterThan, Int(0)),
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
    ]
    .into_iter()
    .map(|(s, w)| (s, Some(w)))
    .chain(once(("$true", None)))
    .collect()
}

fn bench_filter(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let logen = LogGenerator {
        generator: upsert_generator,
    };

    for record_count in [1000, 10000, 100000] {
        let test_segment = runtime.block_on(async {
            let mut segment = TestSegment::default();
            segment.populate_with_generator(record_count, &logen).await;
            segment
        });

        let filter_input = FilterInput {
            logs: Chunk::new(Vec::new().into()),
            blockfile_provider: test_segment.blockfile_provider,
            metadata_segment: test_segment.metadata_segment,
            record_segment: test_segment.record_segment,
        };

        for (op, where_clause) in baseline_where_clauses() {
            let filter_operator = FilterOperator {
                query_ids: None,
                where_clause: where_clause.clone(),
            };

            let routine = |(op, input): (FilterOperator, FilterInput)| async move {
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
