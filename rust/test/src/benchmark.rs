use std::{fmt::Debug, future::Future};

use criterion::{BenchmarkId, Criterion};
use tokio::runtime::Runtime;

pub fn tokio_multi_thread() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime should be set up correctly.")
}

pub fn bench_group<'t, Arg, Fut>(
    name: &'t str,
    criterion: &'t mut Criterion,
    runtime: &'t Runtime,
    inputs: &'t [Arg],
    routine: impl Fn(&'t Arg) -> Fut,
) where
    Arg: Clone + Debug,
    Fut: Future<Output = ()>,
{
    let mut input_cycle = inputs.iter().cycle();
    let mut group = criterion.benchmark_group(name);
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function(
        BenchmarkId::from_parameter(format!("{:#?}", inputs)),
        |bencher| {
            bencher.to_async(runtime).iter_batched(
                || {
                    input_cycle
                        .next()
                        .expect("Cycled inputs should be endless.")
                },
                &routine,
                criterion::BatchSize::SmallInput,
            );
        },
    );
    group.finish();
}
