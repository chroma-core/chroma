#[allow(dead_code)]
mod load;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_config::{registry::Registry, Configurable};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use criterion::{criterion_group, criterion_main, Criterion};
use load::{
    all_projection, always_false_filter_for_modulo_metadata,
    always_true_filter_for_modulo_metadata, empty_fetch_log, offset_limit, sift1m_segments,
    trivial_filter, trivial_limit, trivial_projection,
};
use worker::{config::RootConfig, execution::orchestration::get::GetOrchestrator};

fn trivial_get(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> GetOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    GetOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        trivial_filter(),
        trivial_limit(),
        trivial_projection(),
    )
}

fn get_false_filter(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> GetOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    GetOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_false_filter_for_modulo_metadata(),
        trivial_limit(),
        trivial_projection(),
    )
}

fn get_true_filter(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> GetOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    GetOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_true_filter_for_modulo_metadata(),
        trivial_limit(),
        trivial_projection(),
    )
}

fn get_true_filter_limit(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> GetOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    GetOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_true_filter_for_modulo_metadata(),
        offset_limit(),
        trivial_projection(),
    )
}

fn get_true_filter_limit_projection(
    test_segments: &TestDistributedSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
) -> GetOrchestrator {
    let blockfile_provider = test_segments.blockfile_provider.clone();
    let collection_uuid = test_segments.collection.collection_id;
    GetOrchestrator::new(
        blockfile_provider,
        dispatcher_handle,
        1000,
        test_segments.into(),
        empty_fetch_log(collection_uuid),
        always_true_filter_for_modulo_metadata(),
        offset_limit(),
        all_projection(),
    )
}

async fn bench_routine(input: (System, GetOrchestrator, Vec<String>)) {
    let (system, orchestrator, expected_ids) = input;
    let output = orchestrator
        .run(system)
        .await
        .expect("Orchestrator should not fail");
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
            trivial_get(&test_segments, dispatcher_handle.clone()),
            (0..100).map(|id| id.to_string()).collect(),
        )
    };
    let get_false_filter_setup = || {
        (
            system.clone(),
            get_false_filter(&test_segments, dispatcher_handle.clone()),
            Vec::new(),
        )
    };
    let get_true_filter_setup = || {
        (
            system.clone(),
            get_true_filter(&test_segments, dispatcher_handle.clone()),
            (0..100).map(|id| id.to_string()).collect(),
        )
    };
    let get_true_filter_limit_setup = || {
        (
            system.clone(),
            get_true_filter_limit(&test_segments, dispatcher_handle.clone()),
            (100..200).map(|id| id.to_string()).collect(),
        )
    };
    let get_true_filter_limit_projection_setup = || {
        (
            system.clone(),
            get_true_filter_limit_projection(&test_segments, dispatcher_handle.clone()),
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
