#[allow(dead_code)]
mod load;

use chroma_benchmark::{
    benchmark::{bench_run, tokio_multi_thread},
    datasets::sift::Sift1MData,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_types::{operator::Knn, plan::ReadLevel};
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
        filter::{FilterOrchestrator, FilterOrchestratorOutput},
        knn::KnnOrchestrator,
    },
};

fn trivial_filter_orchestrator(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> FilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    FilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        trivial_filter(),
        ReadLevel::IndexAndWal,
        250,
        None,
        0,
        1,
    )
}

fn always_true_filter_orchestrator(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> FilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    FilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_true_filter_for_modulo_metadata(),
        ReadLevel::IndexAndWal,
        250,
        None,
        0,
        1,
    )
}

fn always_false_filter_orchestrator(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> FilterOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let hnsw_provider = test_segments.hnsw_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    FilterOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        hnsw_provider,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_false_filter_for_modulo_metadata(),
        ReadLevel::IndexAndWal,
        250,
        None,
        0,
        1,
    )
}

fn knn(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
    filter_orchestrator_output: FilterOrchestratorOutput,
    query: Vec<f32>,
) -> KnnOrchestrator {
    KnnOrchestrator::new(
        test_segments.blockfile_provider.clone(),
        dispatcher_handle.clone(),
        1000,
        test_segments.into(),
        filter_orchestrator_output.clone(),
        Knn {
            embedding: query,
            fetch: Sift1MData::k() as u32,
        },
        None,
        0,
    )
}

async fn bench_routine(
    input: (
        System,
        FilterOrchestrator,
        impl Fn(FilterOrchestratorOutput) -> Vec<(KnnOrchestrator, Vec<u32>)>,
    ),
) {
    let (system, filter_orchestrator, knn_constructor) = input;
    let filter_orchestrator_output = filter_orchestrator
        .run(system.clone())
        .await
        .expect("Orchestrator should not fail");
    let (knns, _expected): (Vec<_>, Vec<_>) = knn_constructor(filter_orchestrator_output)
        .into_iter()
        .unzip();
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
            trivial_filter_orchestrator(&test_segments, dispatcher_handle.clone().clone()),
            |filter_orchestrator_output: FilterOrchestratorOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                filter_orchestrator_output.clone(),
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
            always_true_filter_orchestrator(&test_segments, dispatcher_handle.clone().clone()),
            |filter_orchestrator_output: FilterOrchestratorOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                filter_orchestrator_output.clone(),
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
            always_false_filter_orchestrator(&test_segments, dispatcher_handle.clone().clone()),
            |filter_orchestrator_output: FilterOrchestratorOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, _)| {
                        (
                            knn(
                                &test_segments,
                                dispatcher_handle.clone(),
                                filter_orchestrator_output.clone(),
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
