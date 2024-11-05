use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_types::{Chunk, SignedRoaringBitmap};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use worker::execution::operator::Operator;
use worker::execution::operators::limit::{LimitInput, LimitOperator};
use worker::log::test::{upsert_generator, LogGenerator};
use worker::segment::test::TestSegment;

const FETCH: usize = 100;

fn bench_limit(criterion: &mut Criterion) {
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

        let limit_input = LimitInput {
            logs: Chunk::new(Vec::new().into()),
            blockfile_provider: test_segment.blockfile_provider,
            record_segment: test_segment.record_segment,
            log_offset_ids: SignedRoaringBitmap::empty(),
            compact_offset_ids: SignedRoaringBitmap::full(),
        };

        for offset in [0, record_count / 2, record_count - FETCH] {
            let limit_operator = LimitOperator {
                skip: offset as u32,
                fetch: Some(FETCH as u32),
            };

            let routine = |(op, input): (LimitOperator, LimitInput)| async move {
                op.run(&input).await.expect("LimitOperator should not fail");
            };

            let setup = || (limit_operator.clone(), limit_input.clone());

            bench_run(
                format!("limit-{}-{}", record_count, offset).as_str(),
                criterion,
                &runtime,
                setup,
                routine,
            );
        }
    }
}

criterion_group!(benches, bench_limit);
criterion_main!(benches);
