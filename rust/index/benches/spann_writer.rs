//! Comprehensive benchmark harness for SpannIndexWriter ingestion performance.
//!
//! This benchmark tests:
//! - Single-point insertion throughput
//! - Concurrent insertion performance
//! - Impact of different configurations (split_threshold, reassign_neighbor_count, etc.)
//! - Breakdown of time spent in different operations
//! - Blockfile writer performance in isolation

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use chroma_blockstore::{
    arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
    BlockfileWriterOptions,
};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_config::registry::Registry;
use chroma_index::{
    config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannIndexWriter, SpannMetrics},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, InternalSpannConfiguration, SpannPostingList};
use chroma_config::Configurable;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::hint::black_box;
use tokio::runtime::Runtime;

// ============================================================================
// Configuration and Test Data Generation
// ============================================================================

/// Parameters for generating test data
#[derive(Clone)]
struct TestDataConfig {
    num_points: usize,
    dimensionality: usize,
    seed: u64,
}

impl Default for TestDataConfig {
    fn default() -> Self {
        Self {
            num_points: 10_000,
            dimensionality: 128,
            seed: 42,
        }
    }
}

/// Generate random embeddings for testing
fn generate_test_data(config: &TestDataConfig) -> Vec<(u32, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(config.seed);
    (1..=config.num_points)
        .map(|id| {
            let embedding: Vec<f32> = (0..config.dimensionality)
                .map(|_| rng.gen::<f32>())
                .collect();
            (id as u32, embedding)
        })
        .collect()
}

/// Generate clustered test data (more realistic distribution)
#[allow(dead_code)]
fn generate_clustered_test_data(config: &TestDataConfig, num_clusters: usize) -> Vec<(u32, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(config.seed);

    // Generate cluster centers
    let centers: Vec<Vec<f32>> = (0..num_clusters)
        .map(|_| {
            (0..config.dimensionality)
                .map(|_| rng.gen::<f32>() * 10.0)
                .collect()
        })
        .collect();

    // Generate points around clusters
    (1..=config.num_points)
        .map(|id| {
            let cluster_idx = rng.gen_range(0..num_clusters);
            let center = &centers[cluster_idx];
            let embedding: Vec<f32> = center
                .iter()
                .map(|c| c + (rng.gen::<f32>() - 0.5) * 0.5) // Small perturbation
                .collect();
            (id as u32, embedding)
        })
        .collect()
}

// ============================================================================
// Infrastructure Setup
// ============================================================================

struct BenchmarkInfra {
    #[allow(dead_code)]
    storage: Storage,
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    _tmp_dir: tempfile::TempDir,
}

impl BenchmarkInfra {
    fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let max_block_size_bytes = 8 * 1024 * 1024; // 8 MB
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            false,
        );

        Self {
            storage,
            blockfile_provider,
            hnsw_provider,
            _tmp_dir: tmp_dir,
        }
    }
}

async fn create_writer(
    infra: &BenchmarkInfra,
    params: InternalSpannConfiguration,
    dimensionality: usize,
) -> SpannIndexWriter {
    let collection_id = CollectionUuid::new();
    let gc_context = GarbageCollectionContext::try_from_config(
        &(
            PlGarbageCollectionConfig::default(),
            HnswGarbageCollectionConfig::default(),
        ),
        &Registry::default(),
    )
    .await
    .expect("Error creating gc context");

    let prefix_path = "";
    let pl_block_size = 5 * 1024 * 1024;

    SpannIndexWriter::from_id(
        &infra.hnsw_provider,
        None,
        None,
        None,
        None,
        &collection_id,
        prefix_path,
        dimensionality,
        &infra.blockfile_provider,
        params,
        gc_context,
        pl_block_size,
        SpannMetrics::default(),
    )
    .await
    .expect("Error creating spann index writer")
}

// ============================================================================
// Timing Utilities
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
struct OperationTimings {
    add_total_ns: AtomicU64,
    add_count: AtomicU64,
}

#[allow(dead_code)]
impl OperationTimings {
    fn record_add(&self, duration: Duration) {
        self.add_total_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.add_count.fetch_add(1, Ordering::Relaxed);
    }

    fn avg_add_ns(&self) -> f64 {
        let total = self.add_total_ns.load(Ordering::Relaxed) as f64;
        let count = self.add_count.load(Ordering::Relaxed) as f64;
        if count > 0.0 {
            total / count
        } else {
            0.0
        }
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn create_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
}

/// Benchmark: Sequential single-point insertion
fn bench_sequential_add(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_sequential_add");

    for num_points in [1000, 5000, 10000] {
        let data_config = TestDataConfig {
            num_points,
            dimensionality: 128,
            seed: 42,
        };
        let test_data = generate_test_data(&data_config);

        group.throughput(Throughput::Elements(num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("points", num_points),
            &test_data,
            |b, data| {
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration::default();
                        (infra, params, data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = create_writer(&infra, params, 128).await;
                        for (id, embedding) in data {
                            writer.add(id, &embedding).await.expect("Failed to add");
                        }
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Impact of split_threshold configuration
fn bench_split_threshold_impact(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_split_threshold");
    group.sample_size(10);

    let data_config = TestDataConfig {
        num_points: 5000,
        dimensionality: 128,
        seed: 42,
    };
    let test_data = generate_test_data(&data_config);

    for split_threshold in [25, 50, 100, 150, 200] {
        group.throughput(Throughput::Elements(data_config.num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("threshold", split_threshold),
            &test_data,
            |b, data| {
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration {
                            split_threshold,
                            ..Default::default()
                        };
                        (infra, params, data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = create_writer(&infra, params, 128).await;
                        for (id, embedding) in data {
                            writer.add(id, &embedding).await.expect("Failed to add");
                        }
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Impact of reassign_neighbor_count configuration
fn bench_reassign_neighbor_count_impact(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_reassign_neighbor_count");
    group.sample_size(10);

    let data_config = TestDataConfig {
        num_points: 5000,
        dimensionality: 128,
        seed: 42,
    };
    let test_data = generate_test_data(&data_config);

    for reassign_count in [0, 8, 32, 64] {
        group.throughput(Throughput::Elements(data_config.num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("reassign_count", reassign_count),
            &test_data,
            |b, data| {
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration {
                            reassign_neighbor_count: reassign_count,
                            ..Default::default()
                        };
                        (infra, params, data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = create_writer(&infra, params, 128).await;
                        for (id, embedding) in data {
                            writer.add(id, &embedding).await.expect("Failed to add");
                        }
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Concurrent insertion with multiple tasks
fn bench_concurrent_add(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_concurrent_add");
    group.sample_size(10);

    let data_config = TestDataConfig {
        num_points: 10000,
        dimensionality: 128,
        seed: 42,
    };
    let test_data = Arc::new(generate_test_data(&data_config));

    for num_tasks in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(data_config.num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("tasks", num_tasks),
            &test_data,
            |b, data| {
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration::default();
                        (infra, params, data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = Arc::new(create_writer(&infra, params, 128).await);
                        let chunk_size = data.len() / num_tasks;

                        let handles: Vec<_> = (0..num_tasks)
                            .map(|task_id| {
                                let writer_clone = writer.clone();
                                let data_clone = data.clone();
                                let start = task_id * chunk_size;
                                let end = if task_id == num_tasks - 1 {
                                    data.len()
                                } else {
                                    (task_id + 1) * chunk_size
                                };

                                tokio::spawn(async move {
                                    for (id, embedding) in &data_clone[start..end] {
                                        writer_clone
                                            .add(*id, embedding)
                                            .await
                                            .expect("Failed to add");
                                    }
                                })
                            })
                            .collect();

                        futures::future::join_all(handles).await;
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Dimensionality impact
fn bench_dimensionality_impact(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_dimensionality");
    group.sample_size(10);

    for dimensionality in [64, 128, 256, 512, 1024] {
        let data_config = TestDataConfig {
            num_points: 2000,
            dimensionality,
            seed: 42,
        };
        let test_data = generate_test_data(&data_config);

        group.throughput(Throughput::Elements(data_config.num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("dim", dimensionality),
            &test_data,
            |b, data| {
                let dim = dimensionality;
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration::default();
                        (infra, params, data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = create_writer(&infra, params, dim).await;
                        for (id, embedding) in data {
                            writer.add(id, &embedding).await.expect("Failed to add");
                        }
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Commit and flush performance
fn bench_commit_flush(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_commit_flush");
    group.sample_size(10);

    for num_points in [1000, 5000, 10000] {
        let data_config = TestDataConfig {
            num_points,
            dimensionality: 128,
            seed: 42,
        };
        let test_data = generate_test_data(&data_config);

        group.bench_with_input(
            BenchmarkId::new("points", num_points),
            &test_data,
            |b, data| {
                b.to_async(&runtime).iter_batched(
                    || {
                        // Setup: create writer and add all data
                        let infra = BenchmarkInfra::new();
                        let params = InternalSpannConfiguration::default();
                        let data = data.clone();

                        runtime.block_on(async {
                            let writer = create_writer(&infra, params, 128).await;
                            for (id, embedding) in &data {
                                writer.add(*id, embedding).await.expect("Failed to add");
                            }
                            (infra, writer)
                        })
                    },
                    |(infra, writer)| async move {
                        // Only measure commit + flush
                        let flusher = Box::pin(writer.commit()).await.expect("Failed to commit");
                        let _ = Box::pin(flusher.flush()).await.expect("Failed to flush");
                        black_box(infra)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Blockfile writer in isolation (posting list writes)
fn bench_blockfile_posting_list_writes(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("blockfile_posting_list");
    group.sample_size(10);

    for num_heads in [100, 500, 1000] {
        for pl_size in [10, 50, 100] {
            let label = format!("heads_{}_plsize_{}", num_heads, pl_size);

            group.bench_function(&label, |b| {
                b.to_async(&runtime).iter_batched(
                    || {
                        // Generate posting list data
                        let mut rng = StdRng::seed_from_u64(42);
                        let dim = 128;

                        let posting_lists: Vec<(u32, Vec<u32>, Vec<u32>, Vec<f32>)> = (1..=num_heads)
                            .map(|head_id| {
                                let doc_offset_ids: Vec<u32> = (0..pl_size).map(|i| i as u32).collect();
                                let doc_versions: Vec<u32> = vec![1; pl_size];
                                let doc_embeddings: Vec<f32> = (0..pl_size * dim)
                                    .map(|_| rng.gen::<f32>())
                                    .collect();
                                (head_id as u32, doc_offset_ids, doc_versions, doc_embeddings)
                            })
                            .collect();

                        let infra = BenchmarkInfra::new();
                        (infra, posting_lists)
                    },
                    |(infra, posting_lists)| async move {
                        let mut bf_options = BlockfileWriterOptions::new("".to_string());
                        bf_options = bf_options.unordered_mutations();

                        let writer = infra
                            .blockfile_provider
                            .write::<u32, &SpannPostingList<'_>>(bf_options)
                            .await
                            .expect("Failed to create writer");

                        for (head_id, doc_offset_ids, doc_versions, doc_embeddings) in &posting_lists {
                            let pl = SpannPostingList {
                                doc_offset_ids,
                                doc_versions,
                                doc_embeddings,
                            };
                            writer.set("", *head_id, &pl).await.expect("Failed to set");
                        }

                        let flusher = writer
                            .commit::<u32, &SpannPostingList<'_>>()
                            .await
                            .expect("Failed to commit");
                        flusher
                            .flush::<u32, &SpannPostingList<'_>>()
                            .await
                            .expect("Failed to flush");

                        black_box(infra)
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

/// Benchmark: Profile breakdown (detailed timing of add operations)
fn bench_add_profile(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("spann_add_profile");
    group.sample_size(10);

    let data_config = TestDataConfig {
        num_points: 5000,
        dimensionality: 128,
        seed: 42,
    };
    let test_data = generate_test_data(&data_config);

    // Test with different configurations to see where time is spent
    let configs = vec![
        ("baseline", InternalSpannConfiguration::default()),
        (
            "no_reassign",
            InternalSpannConfiguration {
                reassign_neighbor_count: 0,
                ..Default::default()
            },
        ),
        (
            "high_split_threshold",
            InternalSpannConfiguration {
                split_threshold: 200,
                ..Default::default()
            },
        ),
        (
            "optimized",
            InternalSpannConfiguration {
                split_threshold: 150,
                reassign_neighbor_count: 0,
                write_nprobe: 16,
                nreplica_count: 2,
                ..Default::default()
            },
        ),
    ];

    for (name, params) in configs {
        group.throughput(Throughput::Elements(data_config.num_points as u64));
        group.bench_with_input(
            BenchmarkId::new("config", name),
            &test_data,
            |b, data| {
                let params = params.clone();
                b.to_async(&runtime).iter_batched(
                    || {
                        let infra = BenchmarkInfra::new();
                        (infra, params.clone(), data.clone())
                    },
                    |(infra, params, data)| async move {
                        let writer = create_writer(&infra, params, 128).await;
                        for (id, embedding) in data {
                            writer.add(id, &embedding).await.expect("Failed to add");
                        }
                        black_box(writer)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Standalone Profiling Runner (for detailed analysis)
// ============================================================================

/// This function can be called directly for detailed profiling
/// Run with: cargo run --example spann_profile --release
#[allow(dead_code)]
pub async fn run_detailed_profile() {
    println!("=== SpannIndexWriter Detailed Profile ===\n");

    let infra = BenchmarkInfra::new();
    let params = InternalSpannConfiguration {
        split_threshold: 100,
        reassign_neighbor_count: 0,
        ..Default::default()
    };

    let data_config = TestDataConfig {
        num_points: 10_000,
        dimensionality: 128,
        seed: 42,
    };

    println!("Generating {} test embeddings (dim={})...",
        data_config.num_points, data_config.dimensionality);
    let test_data = generate_test_data(&data_config);

    println!("Creating SpannIndexWriter...");
    let writer = create_writer(&infra, params, data_config.dimensionality).await;

    println!("\nStarting insertion benchmark...\n");

    let mut add_times: Vec<Duration> = Vec::with_capacity(data_config.num_points);
    let overall_start = Instant::now();

    for (i, (id, embedding)) in test_data.iter().enumerate() {
        let start = Instant::now();
        writer.add(*id, embedding).await.expect("Failed to add");
        let elapsed = start.elapsed();
        add_times.push(elapsed);

        if (i + 1) % 1000 == 0 {
            let avg_so_far: Duration = add_times.iter().sum::<Duration>() / add_times.len() as u32;
            println!(
                "Progress: {}/{} | Avg add time: {:.2}ms | Throughput: {:.0} points/sec",
                i + 1,
                data_config.num_points,
                avg_so_far.as_secs_f64() * 1000.0,
                (i + 1) as f64 / overall_start.elapsed().as_secs_f64()
            );
        }
    }

    let total_insert_time = overall_start.elapsed();

    println!("\n=== Insertion Summary ===");
    println!("Total points: {}", data_config.num_points);
    println!("Total time: {:.2}s", total_insert_time.as_secs_f64());
    println!(
        "Throughput: {:.0} points/sec",
        data_config.num_points as f64 / total_insert_time.as_secs_f64()
    );

    // Compute percentiles
    add_times.sort();
    let p50 = add_times[add_times.len() / 2];
    let p90 = add_times[(add_times.len() as f64 * 0.9) as usize];
    let p99 = add_times[(add_times.len() as f64 * 0.99) as usize];
    let avg: Duration = add_times.iter().sum::<Duration>() / add_times.len() as u32;

    println!("\n=== Latency Distribution ===");
    println!("Avg:  {:.3}ms", avg.as_secs_f64() * 1000.0);
    println!("P50:  {:.3}ms", p50.as_secs_f64() * 1000.0);
    println!("P90:  {:.3}ms", p90.as_secs_f64() * 1000.0);
    println!("P99:  {:.3}ms", p99.as_secs_f64() * 1000.0);
    println!("Max:  {:.3}ms", add_times.last().unwrap().as_secs_f64() * 1000.0);

    // Commit and flush timing
    println!("\n=== Commit & Flush ===");
    let commit_start = Instant::now();
    let flusher = Box::pin(writer.commit()).await.expect("Failed to commit");
    let commit_time = commit_start.elapsed();
    println!("Commit time: {:.2}ms", commit_time.as_secs_f64() * 1000.0);

    let flush_start = Instant::now();
    let _ = Box::pin(flusher.flush()).await.expect("Failed to flush");
    let flush_time = flush_start.elapsed();
    println!("Flush time: {:.2}ms", flush_time.as_secs_f64() * 1000.0);

    println!("\n=== Total Pipeline Time ===");
    println!(
        "Insert + Commit + Flush: {:.2}s",
        (total_insert_time + commit_time + flush_time).as_secs_f64()
    );
}

// ============================================================================
// Criterion Groups
// ============================================================================

criterion_group!(
    name = basic_benches;
    config = Criterion::default().sample_size(10);
    targets =
        bench_sequential_add,
        bench_concurrent_add,
);

criterion_group!(
    name = config_benches;
    config = Criterion::default().sample_size(10);
    targets =
        bench_split_threshold_impact,
        bench_reassign_neighbor_count_impact,
        bench_dimensionality_impact,
);

criterion_group!(
    name = detailed_benches;
    config = Criterion::default().sample_size(10);
    targets =
        bench_commit_flush,
        bench_blockfile_posting_list_writes,
        bench_add_profile,
);

criterion_main!(basic_benches, config_benches, detailed_benches);
