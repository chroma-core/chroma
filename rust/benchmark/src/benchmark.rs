use std::future::Future;

use criterion::Criterion;
use tokio::runtime::Runtime;

pub fn tokio_multi_thread() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime should be set up correctly.")
}

pub fn bench_run<'t, Arg, Fut>(
    name: &'t str,
    criterion: &'t mut Criterion,
    runtime: &'t Runtime,
    setup: impl Fn() -> Arg,
    routine: impl Fn(Arg) -> Fut,
) where
    Fut: Future<Output = ()>,
{
    criterion.bench_function(name, |bencher| {
        bencher
            .to_async(runtime)
            .iter_batched(&setup, &routine, criterion::BatchSize::SmallInput);
    });
}
