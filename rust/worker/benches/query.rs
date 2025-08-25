#[allow(dead_code)]
mod load;

use chroma_benchmark::{
    benchmark::{bench_run, tokio_multi_thread},
    datasets::sift::Sift1MData,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_types::operator::Knn;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::{stream, StreamExt, TryStreamExt};
use load::{
    always_false_filter_for_modulo_metadata, always_true_filter_for_modulo_metadata,
    empty_fetch_log, sift1m_segments, trivial_filter,
};
use rand::{seq::SliceRandom, thread_rng};
use worker::{
    config::RootConfig,
    execution::orchestration::{
        knn::KnnOrchestrator,
        knn_filter::{KnnFilterOrchestrator, KnnFilterOutput},
    },
};

fn trivial_knn_filter(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> KnnFilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    KnnFilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        trivial_filter(),
    )
}

fn always_true_knn_filter(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> KnnFilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    KnnFilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_true_filter_for_modulo_metadata(),
    )
}

fn always_false_knn_filter(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> KnnFilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    KnnFilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_false_filter_for_modulo_metadata(),
    )
}

fn knn(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
    knn_filter_output: KnnFilterOutput,
    query: Vec<f32>,
) -> KnnOrchestrator {
    KnnOrchestrator::new(
        test_segments.blockfile_provider.clone(),
        dispatcher_handle.clone(),
        1000,
        test_segments.into(),
        knn_filter_output.clone(),
        Knn {
            embedding: query,
            fetch: Sift1MData::k() as u32,
        },
    )
}

async fn bench_routine(
    input: (
        System,
        KnnFilterOrchestrator,
        impl Fn(KnnFilterOutput) -> Vec<(KnnOrchestrator, Vec<u32>)>,
    ),
) {
    let (system, knn_filter, knn_constructor) = input;
    let knn_filter_output = knn_filter
        .run(system.clone())
        .await
        .expect("Orchestrator should not fail");
    let (knns, _expected): (Vec<_>, Vec<_>) =
        knn_constructor(knn_filter_output).into_iter().unzip();
    let _results = stream::iter(knns.into_iter().map(|knn| knn.run(system.clone())))
        .buffered(32)
        .try_collect::<Vec<_>>()
        .await
        .expect("Orchestrators should not fail");
    // TODO: verify recall
}

fn bench_query(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let test_segments = runtime.block_on(sift1m_segments());

    let config = RootConfig::default();
    let system = System::default();
    let registry = Registry::new();
    let dispatcher = runtime
        .block_on(Dispatcher::try_from_config(
            &config.query_service.dispatcher,
            &registry,
        ))
        .expect("Should be able to initialize dispatcher");
    let dispatcher_handle = runtime.block_on(async { system.start_component(dispatcher) });

    let mut sift1m = runtime
        .block_on(Sift1MData::init())
        .expect("Should be able to download Sift1M data");
    let mut sift1m_queries = runtime
        .block_on(sift1m.query())
        .expect("Should be able to load Sift1M queries");

    sift1m_queries.as_mut_slice().shuffle(&mut thread_rng());

    let trivial_knn_setup = || {
        (
            system.clone(),
            trivial_knn_filter(&test_segments, dispatcher_handle.clone().clone()),
            |knn_filter_output: KnnFilterOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                knn_filter_output.clone(),
                                query.clone(),
                            ),
                            expected.clone(),
                        )
                    })
                    .collect()
            },
        )
    };

    let true_filter_knn_setup = || {
        (
            system.clone(),
            always_true_knn_filter(&test_segments, dispatcher_handle.clone().clone()),
            |knn_filter_output: KnnFilterOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                knn_filter_output.clone(),
                                query.clone(),
                            ),
                            expected.clone(),
                        )
                    })
                    .collect()
            },
        )
    };

    let false_filter_knn_setup = || {
        (
            system.clone(),
            always_false_knn_filter(&test_segments, dispatcher_handle.clone().clone()),
            |knn_filter_output: KnnFilterOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, _)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                knn_filter_output.clone(),
                                query.clone(),
                            ),
                            Vec::new(),
                        )
                    })
                    .collect()
            },
        )
    };

    bench_run(
        "test-trivial-knn",
        criterion,
        &runtime,
        trivial_knn_setup,
        bench_routine,
    );

    bench_run(
        "test-true-filter-knn",
        criterion,
        &runtime,
        true_filter_knn_setup,
        bench_routine,
    );

    bench_run(
        "test-false-filter-knn",
        criterion,
        &runtime,
        false_filter_knn_setup,
        bench_routine,
    );
}
criterion_group!(benches, bench_query);
criterion_main!(benches);
