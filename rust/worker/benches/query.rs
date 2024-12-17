#[allow(dead_code)]
mod load;

use std::collections::HashSet;

use chroma_benchmark::{
    benchmark::{bench_run, tokio_multi_thread},
    datasets::sift::Sift1MData,
};
use chroma_config::Configurable;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::{stream, StreamExt, TryStreamExt};
use load::{
    all_projection, always_true_filter_for_modulo_metadata, empty_fetch_log, sift1m_segments,
    trivial_filter,
};
use rand::{seq::SliceRandom, thread_rng};
use worker::{
    config::RootConfig,
    execution::{
        dispatcher::Dispatcher,
        operators::{knn::KnnOperator, knn_projection::KnnProjectionOperator},
        orchestration::{
            knn::KnnOrchestrator,
            knn_filter::{KnnFilterOrchestrator, KnnFilterOutput},
        },
    },
    segment::test::TestSegment,
    system::{ComponentHandle, System},
};

fn trivial_knn_filter(
    test_segments: TestSegment,
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
    test_segments: TestSegment,
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

fn knn(
    test_segments: TestSegment,
    dispatcher_handle: ComponentHandle<Dispatcher>,
    knn_filter_output: KnnFilterOutput,
    query: Vec<f32>,
) -> KnnOrchestrator {
    KnnOrchestrator::new(
        test_segments.blockfile_provider.clone(),
        dispatcher_handle.clone(),
        1000,
        knn_filter_output.clone(),
        KnnOperator {
            embedding: query,
            fetch: Sift1MData::k() as u32,
        },
        KnnProjectionOperator {
            projection: all_projection(),
            distance: true,
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
    let (knns, expected): (Vec<_>, Vec<_>) = knn_constructor(knn_filter_output).into_iter().unzip();
    let results = stream::iter(knns.into_iter().map(|knn| knn.run(system.clone())))
        .buffered(32)
        .try_collect::<Vec<_>>()
        .await
        .expect("Orchestrators should not fail");
    results
        .into_iter()
        .map(|result| {
            result
                .records
                .into_iter()
                .map(|record| record.record.id.parse())
                // .collect::<Result<_, _>>()
                .collect::<Result<Vec<u32>, _>>()
                .expect("Record id should be parsable to u32")
        })
        .zip(expected)
        .for_each(|(got, expected)| {
            let expected_set: HashSet<_> = HashSet::from_iter(expected);
            let recall = got
                .into_iter()
                .filter(|id| expected_set.contains(id))
                .count() as f64
                / expected_set.len() as f64;
            assert!(recall > 0.9);
        });
}

fn bench_query(criterion: &mut Criterion) {
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
            trivial_knn_filter(test_segments.clone(), dispatcher_handle.clone().clone()),
            |knn_filter_output: KnnFilterOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                test_segments.clone(),
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

    let filtered_knn_setup = || {
        (
            system.clone(),
            always_true_knn_filter(test_segments.clone(), dispatcher_handle.clone().clone()),
            |knn_filter_output: KnnFilterOutput| {
                sift1m_queries
                    .iter()
                    .take(4)
                    .map(|(query, expected)| {
                        (
                            knn(
                                test_segments.clone(),
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

    bench_run(
        "test-trivial-knn",
        criterion,
        &runtime,
        trivial_knn_setup,
        bench_routine,
    );

    bench_run(
        "test-filtered-knn",
        criterion,
        &runtime,
        filtered_knn_setup,
        bench_routine,
    );
}
criterion_group!(benches, bench_query);
criterion_main!(benches);
