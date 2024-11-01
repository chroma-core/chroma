use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_types::{Chunk, SignedRoaringBitmap};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use worker::execution::operator::Operator;
use worker::execution::operators::limit::{LimitInput, LimitOperator};
use worker::log::test::{add_generator, LogGenerator};
use worker::segment::test::TestSegment;

const LIMIT: usize = 100;

fn bench_limit(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let logen = LogGenerator {
        generator: add_generator,
    };

    let routine = |_limit_input| async move {
        // TODO: Run limit operator
    };

    for record_count in [1000, 10000, 100000] {
        let mut compact = TestSegment::default();
        runtime.block_on(async { compact.populate_with_generator(record_count, &logen).await });

        for offset in [0, record_count / 2, record_count - LIMIT] {
            let setup = || {
                // TODO: Construct limit input
            };
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
