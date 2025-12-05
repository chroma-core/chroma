//! Standalone profiling tool for SpannIndexWriter
//!
//! Run with: cargo run --example spann_profile --release
//!
//! This provides detailed timing breakdowns and can be used with external
//! profilers like `perf`, `samply`, or `cargo flamegraph`.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use chroma_blockstore::{
    arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_config::{registry::Registry, Configurable};
use chroma_index::{
    config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannIndexWriter, SpannMetrics},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, InternalSpannConfiguration};
use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of points to insert
    #[arg(short, long, default_value_t = 10000)]
    num_points: usize,

    /// Embedding dimensionality
    #[arg(short, long, default_value_t = 128)]
    dimensionality: usize,

    /// Split threshold for posting lists
    #[arg(long, default_value_t = 50)]
    split_threshold: u32,

    /// Number of neighbors to reassign after splits
    #[arg(long, default_value_t = 64)]
    reassign_neighbor_count: u32,

    /// Number of concurrent tasks
    #[arg(short, long, default_value_t = 1)]
    concurrency: usize,

    /// Random seed
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Skip commit/flush (useful for isolating insertion performance)
    #[arg(long)]
    skip_flush: bool,

    /// Batch size for batch insertion (0 = use single-point add)
    #[arg(short, long, default_value_t = 0)]
    batch_size: usize,
}

fn generate_test_data(num_points: usize, dimensionality: usize, seed: u64) -> Vec<(u32, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (1..=num_points)
        .map(|id| {
            let embedding: Vec<f32> = (0..dimensionality).map(|_| rng.gen::<f32>()).collect();
            (id as u32, embedding)
        })
        .collect()
}

async fn create_infrastructure() -> (
    Storage,
    BlockfileProvider,
    HnswIndexProvider,
    tempfile::TempDir,
) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    let block_cache = new_cache_for_test();
    let sparse_index_cache = new_cache_for_test();
    let max_block_size_bytes = 8 * 1024 * 1024;
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage.clone(),
        max_block_size_bytes,
        block_cache,
        sparse_index_cache,
        BlockManagerConfig::default_num_concurrent_block_flushes(),
    );
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
    let hnsw_cache = new_non_persistent_cache_for_test();
    let hnsw_provider = HnswIndexProvider::new(
        storage.clone(),
        PathBuf::from(tmp_dir.path().to_str().unwrap()),
        hnsw_cache,
        16,
        false,
    );

    (storage, blockfile_provider, hnsw_provider, tmp_dir)
}

async fn create_writer(
    hnsw_provider: &HnswIndexProvider,
    blockfile_provider: &BlockfileProvider,
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
        hnsw_provider,
        None,
        None,
        None,
        None,
        &collection_id,
        prefix_path,
        dimensionality,
        blockfile_provider,
        params,
        gc_context,
        pl_block_size,
        SpannMetrics::default(),
    )
    .await
    .expect("Error creating spann index writer")
}

fn print_latency_stats(name: &str, times: &[Duration]) {
    if times.is_empty() {
        return;
    }

    let mut sorted = times.to_vec();
    sorted.sort();

    let sum: Duration = sorted.iter().sum();
    let avg = sum / sorted.len() as u32;
    let p50 = sorted[sorted.len() / 2];
    let p90 = sorted[(sorted.len() as f64 * 0.9) as usize];
    let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];

    println!("\n{} Latency Stats:", name);
    println!("  Count: {}", sorted.len());
    println!("  Min:   {:.3}ms", min.as_secs_f64() * 1000.0);
    println!("  Avg:   {:.3}ms", avg.as_secs_f64() * 1000.0);
    println!("  P50:   {:.3}ms", p50.as_secs_f64() * 1000.0);
    println!("  P90:   {:.3}ms", p90.as_secs_f64() * 1000.0);
    println!("  P99:   {:.3}ms", p99.as_secs_f64() * 1000.0);
    println!("  Max:   {:.3}ms", max.as_secs_f64() * 1000.0);
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("=== SpannIndexWriter Profiler ===\n");
    println!("Configuration:");
    println!("  Points:                  {}", args.num_points);
    println!("  Dimensionality:          {}", args.dimensionality);
    println!("  Split Threshold:         {}", args.split_threshold);
    println!(
        "  Reassign Neighbor Count: {}",
        args.reassign_neighbor_count
    );
    println!("  Concurrency:             {}", args.concurrency);
    println!("  Seed:                    {}", args.seed);
    println!();

    // Generate test data
    println!("Generating test data...");
    let start = Instant::now();
    let test_data = Arc::new(generate_test_data(
        args.num_points,
        args.dimensionality,
        args.seed,
    ));
    println!(
        "Generated {} embeddings in {:.2}ms\n",
        args.num_points,
        start.elapsed().as_secs_f64() * 1000.0
    );

    // Create infrastructure
    println!("Setting up infrastructure...");
    let (_storage, blockfile_provider, hnsw_provider, _tmp_dir) = create_infrastructure().await;

    // Create writer with custom config
    let mut params = InternalSpannConfiguration::default();
    params.split_threshold = args.split_threshold;
    params.reassign_neighbor_count = args.reassign_neighbor_count;

    let writer = Arc::new(
        create_writer(
            &hnsw_provider,
            &blockfile_provider,
            params,
            args.dimensionality,
        )
        .await,
    );
    println!("Infrastructure ready.\n");

    // Run insertion benchmark
    println!("Starting insertion benchmark...\n");
    let overall_start = Instant::now();

    if args.batch_size > 0 && args.concurrency > 1 {
        // Concurrent batch insertion mode
        println!(
            "Using CONCURRENT BATCH insertion (batch_size={}, tasks={})\n",
            args.batch_size, args.concurrency
        );

        // Split data among tasks, each task processes its portion in batches
        let chunk_size = test_data.len() / args.concurrency;
        let batch_size = args.batch_size;
        let dimensionality = args.dimensionality;

        let handles: Vec<_> = (0..args.concurrency)
            .map(|task_id| {
                let writer_clone = writer.clone();
                let data_clone = test_data.clone();
                let start_idx = task_id * chunk_size;
                let end_idx = if task_id == args.concurrency - 1 {
                    test_data.len()
                } else {
                    (task_id + 1) * chunk_size
                };

                tokio::spawn(async move {
                    let task_data = &data_clone[start_idx..end_idx];
                    let mut batch_count = 0;

                    for chunk in task_data.chunks(batch_size) {
                        let ids: Vec<u32> = chunk.iter().map(|(id, _)| *id).collect();
                        let embeddings: Vec<f32> = chunk
                            .iter()
                            .flat_map(|(_, emb)| emb.iter().cloned())
                            .collect();

                        writer_clone
                            .add_batch(&ids, &embeddings)
                            .await
                            .expect("Failed to add batch");
                        batch_count += 1;
                    }
                    batch_count
                })
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        let total_batches: usize = results.into_iter().map(|r| r.expect("Task failed")).sum();
        println!(
            "Completed {} total batches across {} tasks",
            total_batches, args.concurrency
        );
    } else if args.batch_size > 0 {
        // Single-task batch insertion mode
        println!("Using BATCH insertion (batch_size={})\n", args.batch_size);
        let mut batch_times: Vec<Duration> = Vec::new();

        for chunk in test_data.chunks(args.batch_size) {
            let ids: Vec<u32> = chunk.iter().map(|(id, _)| *id).collect();
            let embeddings: Vec<f32> = chunk
                .iter()
                .flat_map(|(_, emb)| emb.iter().cloned())
                .collect();

            let start = Instant::now();
            writer
                .add_batch(&ids, &embeddings)
                .await
                .expect("Failed to add batch");
            batch_times.push(start.elapsed());

            let points_so_far = (batch_times.len() * args.batch_size).min(args.num_points);
            if points_so_far % 10000 == 0 || points_so_far == args.num_points {
                let elapsed = overall_start.elapsed();
                let throughput = points_so_far as f64 / elapsed.as_secs_f64();
                println!(
                    "Progress: {}/{} ({:.1}%) | Throughput: {:.0} pts/sec",
                    points_so_far,
                    args.num_points,
                    points_so_far as f64 / args.num_points as f64 * 100.0,
                    throughput
                );
            }
        }

        println!(
            "\nBatch Stats: {} batches, avg {:.2}ms per batch ({:.0} pts/batch)",
            batch_times.len(),
            batch_times
                .iter()
                .map(|d| d.as_secs_f64() * 1000.0)
                .sum::<f64>()
                / batch_times.len() as f64,
            args.batch_size as f64
        );
    } else if args.concurrency == 1 {
        // Sequential insertion with detailed timing
        println!("Using SEQUENTIAL insertion\n");
        let mut add_times: Vec<Duration> = Vec::with_capacity(args.num_points);

        for (i, (id, embedding)) in test_data.iter().enumerate() {
            let start = Instant::now();
            writer.add(*id, embedding).await.expect("Failed to add");
            add_times.push(start.elapsed());

            if (i + 1) % 1000 == 0 || i + 1 == args.num_points {
                let elapsed = overall_start.elapsed();
                let throughput = (i + 1) as f64 / elapsed.as_secs_f64();
                println!(
                    "Progress: {}/{} ({:.1}%) | Throughput: {:.0} pts/sec",
                    i + 1,
                    args.num_points,
                    (i + 1) as f64 / args.num_points as f64 * 100.0,
                    throughput
                );
            }
        }

        print_latency_stats("Add Operation", &add_times);
    } else {
        // Concurrent insertion
        println!("Using CONCURRENT insertion ({} tasks)\n", args.concurrency);
        let chunk_size = test_data.len() / args.concurrency;

        let handles: Vec<_> = (0..args.concurrency)
            .map(|task_id| {
                let writer_clone = writer.clone();
                let data_clone = test_data.clone();
                let start_idx = task_id * chunk_size;
                let end_idx = if task_id == args.concurrency - 1 {
                    test_data.len()
                } else {
                    (task_id + 1) * chunk_size
                };

                tokio::spawn(async move {
                    let mut times = Vec::with_capacity(end_idx - start_idx);
                    for (id, embedding) in &data_clone[start_idx..end_idx] {
                        let start = Instant::now();
                        writer_clone
                            .add(*id, embedding)
                            .await
                            .expect("Failed to add");
                        times.push(start.elapsed());
                    }
                    times
                })
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        let all_times: Vec<Duration> = results
            .into_iter()
            .flat_map(|r| r.expect("Task failed"))
            .collect();

        print_latency_stats("Add Operation (Concurrent)", &all_times);
    }

    let total_insert_time = overall_start.elapsed();

    // Get timing breakdown
    let (rng_time_us, append_time_us, vm_time_us) = writer.get_timing_stats();
    let total_us = total_insert_time.as_micros() as f64;

    println!("\n=== Time Breakdown ===");
    println!(
        "RNG Query:     {:>8.2}ms ({:>5.1}%)",
        rng_time_us as f64 / 1000.0,
        rng_time_us as f64 / total_us * 100.0
    );
    println!(
        "Append:        {:>8.2}ms ({:>5.1}%)",
        append_time_us as f64 / 1000.0,
        append_time_us as f64 / total_us * 100.0
    );
    println!(
        "Version Map:   {:>8.2}ms ({:>5.1}%)",
        vm_time_us as f64 / 1000.0,
        vm_time_us as f64 / total_us * 100.0
    );
    let other_us = total_us - (rng_time_us + append_time_us + vm_time_us) as f64;
    println!(
        "Other:         {:>8.2}ms ({:>5.1}%)",
        other_us / 1000.0,
        other_us / total_us * 100.0
    );

    println!("\n=== Insertion Summary ===");
    println!("Total points:  {}", args.num_points);
    println!("Total time:    {:.2}s", total_insert_time.as_secs_f64());
    println!(
        "Throughput:    {:.0} points/sec",
        args.num_points as f64 / total_insert_time.as_secs_f64()
    );
    println!(
        "Avg per point: {:.3}ms",
        total_insert_time.as_secs_f64() * 1000.0 / args.num_points as f64
    );

    // Commit and flush
    if !args.skip_flush {
        println!("\n=== Commit & Flush ===");

        // Need to get the writer out of the Arc
        let writer = match Arc::try_unwrap(writer) {
            Ok(w) => w,
            Err(_) => panic!("Multiple references to writer still exist"),
        };

        let commit_start = Instant::now();
        let flusher = Box::pin(writer.commit()).await.expect("Failed to commit");
        let commit_time = commit_start.elapsed();
        println!("Commit time: {:.2}ms", commit_time.as_secs_f64() * 1000.0);

        let flush_start = Instant::now();
        let ids = Box::pin(flusher.flush()).await.expect("Failed to flush");
        let flush_time = flush_start.elapsed();
        println!("Flush time:  {:.2}ms", flush_time.as_secs_f64() * 1000.0);

        println!("\nIndex IDs:");
        println!("  Posting List: {}", ids.pl_id);
        println!("  Versions Map: {}", ids.versions_map_id);
        println!("  HNSW:         {}", ids.hnsw_id);

        println!("\n=== Total Pipeline Time ===");
        println!(
            "Insert + Commit + Flush: {:.2}s",
            (total_insert_time + commit_time + flush_time).as_secs_f64()
        );
    }
}
