#[allow(dead_code)]
mod load;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_config::{registry::Registry, Configurable};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_types::operator::{Filter, Limit, Projection};
use criterion::{criterion_group, criterion_main, Criterion};
use load::{
    all_projection, always_false_filter_for_modulo_metadata,
    always_true_filter_for_modulo_metadata, empty_fetch_log, offset_limit, sift1m_segments,
    trivial_filter, trivial_limit, trivial_projection,
};
use worker::{
    config::RootConfig,
    execution::orchestration::{filter::FilterOrchestrator, get::GetOrchestrator},
};

async fn bench_routine(
    (system, dispatcher, test_segments, filter, limit, projection, expected_ids): (
        System,
        ComponentHandle<Dispatcher>,
        &TestDistributedSegment,
        Filter,
        Limit,
        Projection,
        Vec<String>,
    ),
) {
    let matching_records = FilterOrchestrator::new(
        test_segments.blockfile_provider.clone(),
        dispatcher.clone(),
        test_segments.hnsw_provider.clone(),
        1000,
        test_segments.into(),
        empty_fetch_log(test_segments.collection.collection_id),
        filter,
    )
    .run(system.clone())
    .await
    .expect("Filter orchestrator should not fail");
    let output = GetOrchestrator::new(
        test_segments.blockfile_provider.clone(),
        dispatcher,
        1000,
        matching_records,
        limit,
        projection,
    )
    .run(system)
    .await
    .expect("Get orchestrator should not fail");
    assert_eq!(
        output
            .result
            .records
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        expected_ids
    );
}

fn bench_get(criterion: &mut Criterion) {
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

    let trivial_get_setup = || {
        (
            system.clone(),
            dispatcher_handle.clone(),
            &test_segments,
            trivial_filter(),
            trivial_limit(),
            trivial_projection(),
            (0..100).map(|id| id.to_string()).collect(),
        )
    };
    let get_false_filter_setup = || {
        (
            system.clone(),
            dispatcher_handle.clone(),
            &test_segments,
            always_false_filter_for_modulo_metadata(),
            trivial_limit(),
            trivial_projection(),
            Vec::new(),
        )
    };
    let get_true_filter_setup = || {
        (
            system.clone(),
            dispatcher_handle.clone(),
            &test_segments,
            always_true_filter_for_modulo_metadata(),
            trivial_limit(),
            trivial_projection(),
            (0..100).map(|id| id.to_string()).collect(),
        )
    };
    let get_true_filter_limit_setup = || {
        (
            system.clone(),
            dispatcher_handle.clone(),
            &test_segments,
            always_true_filter_for_modulo_metadata(),
            offset_limit(),
            trivial_projection(),
            (100..200).map(|id| id.to_string()).collect(),
        )
    };
    let get_true_filter_limit_projection_setup = || {
        (
            system.clone(),
            dispatcher_handle.clone(),
            &test_segments,
            always_true_filter_for_modulo_metadata(),
            offset_limit(),
            all_projection(),
            (100..200).map(|id| id.to_string()).collect(),
        )
    };

    bench_run(
        "test-trivial-get",
        criterion,
        &runtime,
        trivial_get_setup,
        bench_routine,
    );
    bench_run(
        "test-get-false-filter",
        criterion,
        &runtime,
        get_false_filter_setup,
        bench_routine,
    );
    bench_run(
        "test-get-true-filter",
        criterion,
        &runtime,
        get_true_filter_setup,
        bench_routine,
    );
    bench_run(
        "test-get-true-filter-limit",
        criterion,
        &runtime,
        get_true_filter_limit_setup,
        bench_routine,
    );
    bench_run(
        "test-get-true-filter-limit-projection",
        criterion,
        &runtime,
        get_true_filter_limit_projection_setup,
        bench_routine,
    );
}
criterion_group!(benches, bench_get);
criterion_main!(benches);
