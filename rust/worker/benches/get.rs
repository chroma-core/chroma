mod load;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_config::Configurable;
use criterion::{criterion_group, criterion_main, Criterion};
use load::sift1m_segments;
use worker::{
    config::RootConfig,
    execution::{
        dispatcher::Dispatcher,
        operators::{
            fetch_log::FetchLogOperator, filter::FilterOperator, limit::LimitOperator,
            projection::ProjectionOperator,
        },
        orchestration::get::GetOrchestrator,
    },
    log::log::{InMemoryLog, Log},
    system::System,
};

fn bench_get(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let test_segments = runtime.block_on(sift1m_segments());

    let config = RootConfig::default();
    let system = System::default();
    let dispatcher = runtime
        .block_on(Dispatcher::try_from_config(
            &config.query_service.dispatcher,
        ))
        .expect("Should be able to initialize dispatcher");
    let dispatcher_handle = runtime.block_on(async { system.start_component(dispatcher) });

    let filter = FilterOperator {
        query_ids: None,
        where_clause: None,
    };
    let limit = LimitOperator {
        skip: 0,
        fetch: Some(100),
    };
    let projection = ProjectionOperator {
        document: true,
        embedding: true,
        metadata: true,
    };

    let test_name = format!("get-sift1m-{:?}-{:?}-{:?}", filter, limit, projection);

    let setup = || {
        (
            system.clone(),
            GetOrchestrator::new(
                test_segments.blockfile_provider.clone(),
                dispatcher_handle.clone(),
                100,
                test_segments.clone().into(),
                FetchLogOperator {
                    log_client: Log::InMemory(InMemoryLog::default()).into(),
                    batch_size: 100,
                    start_log_offset_id: 0,
                    maximum_fetch_count: Some(0),
                    collection_uuid: test_segments.collection.collection_id,
                },
                FilterOperator {
                    query_ids: None,
                    where_clause: None,
                },
                LimitOperator {
                    skip: 0,
                    fetch: Some(100),
                },
                ProjectionOperator {
                    document: true,
                    embedding: true,
                    metadata: true,
                },
            ),
        )
    };

    let routine = |(system, orchestrator): (System, GetOrchestrator)| async move {
        orchestrator
            .run(system)
            .await
            .expect("Orchestrator should not fail");
    };

    bench_run(&test_name, criterion, &runtime, setup, routine);
}
criterion_group!(benches, bench_get);
criterion_main!(benches);
