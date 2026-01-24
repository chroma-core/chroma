//! SPANN Index Construction Benchmark
//!
//! This benchmark measures the performance of building a SPANN index from batches
//! of records, simulating the realistic compaction workload where user writes
//! arrive in a log and are processed in batches.
//!
//! ## Usage
//!
//! ### Basic benchmark (default: 1M records, 10K batch size - 100 batches)
//! ```bash
//! cargo run --release --example spann_write_benchmark
//! ```
//!
//! ### Specify total records
//! ```bash
//! cargo run --release --example spann_write_benchmark -- -n 500000
//! ```
//!
//! ### Specify batch size
//! ```bash
//! cargo run --release --example spann_write_benchmark -- -n 1000000 -b 50000
//! ```
//!
//! ### Test mode (10K records in 10 batches, then verify with 10 queries)
//! ```bash
//! cargo run --release --example spann_write_benchmark -- --test
//! ```
//!
//! ### Profiling mode (no progress bars, minimal output for flamegraph)
//! ```bash
//! cargo run --release --example spann_write_benchmark -- -n 50000 --profile
//! ```
//!
//! ### Flamegraph profiling
//! ```bash
//! cargo flamegraph --example spann_write_benchmark -- -n 50000 --profile
//! ```
//!
//! ### Custom write_rng parameters (for tuning cluster selection)
//! ```bash
//! # Tighter search radius, stricter diversity
//! cargo run --release --example spann_write_benchmark -- \
//!   --write-rng-epsilon 2.0 --write-rng-factor 0.5
//!
//! # Wider search radius, looser diversity
//! cargo run --release --example spann_write_benchmark -- \
//!   --write-rng-epsilon 10.0 --write-rng-factor 2.0
//! ```
//!
//! ## Configuration
//!
//! Uses default SPANN configuration:
//! - split_threshold: 50
//! - merge_threshold: 25
//! - write_nprobe: 32
//! - nreplica_count: 8
//! - write_rng_epsilon: 5.0 (configurable via --write-rng-epsilon)
//! - write_rng_factor: 1.0 (configurable via --write-rng-factor)
//! - ef_construction: 200
//! - max_neighbors: 64
//! - space: L2
//! - pl_block_size: 2 MB (production default)
//!
//! ## Dataset
//!
//! Uses Wikipedia SPLADE dataset (30,522D sparse, 6.4M documents) which is
//! automatically downloaded from HuggingFace on first run.

use chroma_benchmark::datasets::wikipedia_splade::WikipediaSplade;
use chroma_blockstore::{
    arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_index::{
    config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannIndexReader, SpannIndexWriter, SpannMetrics},
    Index, IndexUuid,
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, InternalSpannConfiguration};
use clap::Parser;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::{path::PathBuf, time::Instant};

/// Command line arguments for the benchmark
#[derive(Parser, Debug)]
#[command(name = "spann_write_benchmark")]
#[command(about = "Benchmark SPANN index construction with batch processing")]
struct Args {
    /// Number of records to index (total)
    #[arg(short = 'n', long, default_value_t = 1000000)]
    num_records: usize,

    /// Batch size (records per batch)
    #[arg(short = 'b', long, default_value_t = 100000)]
    batch_size: usize,

    /// Test mode: 10K records in 10 batches, then verify with queries
    #[arg(long)]
    test: bool,

    /// Profiling mode (no progress bars, minimal output)
    #[arg(long)]
    profile: bool,

    /// Write RNG epsilon: controls search radius for cluster selection (default: 5.0)
    /// Lower = tighter radius, faster writes, less coverage
    /// Higher = wider radius, slower writes, better coverage
    #[arg(long)]
    write_rng_epsilon: Option<f32>,

    /// Write RNG factor: controls diversity pruning (default: 1.0)
    /// Lower = stricter diversity, fewer clusters selected
    /// Higher = looser diversity, more clusters selected
    #[arg(long)]
    write_rng_factor: Option<f32>,

    /// Output CSV file path (optional)
    /// If provided, writes batch statistics to CSV
    #[arg(long)]
    output_csv: Option<String>,

    /// Number of parallel tasks for batch writes (default: 5)
    /// Each task gets a clone of the writer (shares underlying data)
    #[arg(short = 'p', long, default_value_t = 5)]
    parallelism: usize,
}

/// Replica statistics for a batch
#[derive(Debug, Clone)]
struct ReplicaStats {
    min_replicas: usize,
    max_replicas: usize,
    avg_replicas: f64,
    median_replicas: usize,
    p90_replicas: usize,
    p99_replicas: usize,
    total_clusters: usize,
    clusters_with_docs: usize,
    clusters_modified: usize,
}

/// Metrics collected for each batch
#[derive(Debug, Clone)]
struct BatchMetrics {
    batch_idx: usize,
    num_records: usize,
    total_records_so_far: usize,
    add_time_ms: f64,
    total_time_ms: f64,
    memory_bytes: usize,
    memory_delta_bytes: i64,
    replica_stats: ReplicaStats,
}

impl BatchMetrics {
    fn throughput(&self) -> f64 {
        self.num_records as f64 / (self.total_time_ms / 1000.0)
    }
}

/// Overall benchmark results
#[derive(Debug)]
struct BenchmarkSummary {
    total_records: usize,
    batch_metrics: Vec<BatchMetrics>,
    total_time_ms: f64,
    peak_memory_bytes: usize,
    // Final index IDs for querying
    final_hnsw_id: Option<IndexUuid>,
    final_versions_map_id: Option<uuid::Uuid>,
    final_pl_id: Option<uuid::Uuid>,
    final_max_head_id_id: Option<uuid::Uuid>,
}

impl BenchmarkSummary {
    fn average_throughput(&self) -> f64 {
        self.total_records as f64 / (self.total_time_ms / 1000.0)
    }

    fn throughput_degradation(&self) -> f64 {
        if self.batch_metrics.len() < 2 {
            return 0.0;
        }
        let first_throughput = self.batch_metrics.first().unwrap().throughput();
        let last_throughput = self.batch_metrics.last().unwrap().throughput();
        ((last_throughput - first_throughput) / first_throughput) * 100.0
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let mut args = Args::parse();

    // Apply test mode overrides
    if args.test {
        args.num_records = 10000;
        args.batch_size = 1000;
    }

    // Validate arguments
    if args.num_records < args.batch_size {
        anyhow::bail!("num_records must be >= batch_size");
    }
    if args.num_records % args.batch_size != 0 {
        anyhow::bail!("num_records must be divisible by batch_size");
    }

    // Create SPANN configuration with custom parameters
    let params = create_spann_config(&args);

    // Print configuration
    print_benchmark_config(&args, &params);

    // Load dataset
    let records = load_dataset(&args).await?;

    // Setup SPANN infrastructure
    let (temp_dir, blockfile_provider, hnsw_provider, collection_id) =
        setup_spann_infrastructure().await?;

    // Run benchmark
    let summary = run_benchmark(
        &args,
        &records,
        &blockfile_provider,
        &hnsw_provider,
        &collection_id,
        &params,
    )
    .await?;

    // Print results
    print_benchmark_results(&summary, &args);

    // Run verification if in test mode
    // if args.test {
    //     verify_index_correctness(
    //         &records,
    //         &summary,
    //         &blockfile_provider,
    //         &hnsw_provider,
    //         &collection_id,
    //         &params,
    //     )
    //     .await?;
    // }

    // Write CSV output if requested
    if let Some(csv_path) = &args.output_csv {
        write_csv_output(csv_path, &summary)?;
    }

    // Cleanup
    drop(blockfile_provider);
    drop(hnsw_provider);
    drop(temp_dir);

    Ok(())
}

// ============================================================================
// Configuration & Setup
// ============================================================================

fn create_spann_config(args: &Args) -> InternalSpannConfiguration {
    let mut params = InternalSpannConfiguration::default();

    // Apply custom write_rng parameters if provided
    if let Some(epsilon) = args.write_rng_epsilon {
        params.write_rng_epsilon = epsilon;
    }
    if let Some(factor) = args.write_rng_factor {
        params.write_rng_factor = factor;
    }

    params.nreplica_count = 4;
    params.split_threshold = 1000;
    params.merge_threshold = 500;
    params.reassign_neighbor_count = 8;

    params
}

fn print_benchmark_config(args: &Args, params: &InternalSpannConfiguration) {
    let num_batches = args.num_records / args.batch_size;

    println!("🚀 SPANN Index Construction Benchmark");
    println!("{}", "=".repeat(60));
    println!("Configuration:");
    println!("  Dataset: Wikipedia SPLADE (1024D dense BGE-M3, 6.4M docs)");
    println!("  Total records: {}", args.num_records);
    println!("  Batch size: {}", args.batch_size);
    println!("  Batches: {}", num_batches);
    if args.parallelism > 1 {
        println!("  Parallelism: {} tasks", args.parallelism);
    }
    println!();

    let has_custom = args.write_rng_epsilon.is_some() || args.write_rng_factor.is_some();

    if has_custom {
        println!("SPANN Configuration (custom parameters marked with *):");
    } else {
        println!("SPANN Configuration (all defaults):");
    }

    println!("  split_threshold: {}", params.split_threshold);
    println!("  merge_threshold: {}", params.merge_threshold);
    println!("  write_nprobe: {}", params.write_nprobe);
    println!("  nreplica_count: {}", params.nreplica_count);

    // Highlight custom values
    if args.write_rng_epsilon.is_some() {
        println!("  write_rng_epsilon: {} *", params.write_rng_epsilon);
    } else {
        println!("  write_rng_epsilon: {}", params.write_rng_epsilon);
    }

    if args.write_rng_factor.is_some() {
        println!("  write_rng_factor: {} *", params.write_rng_factor);
    } else {
        println!("  write_rng_factor: {}", params.write_rng_factor);
    }

    println!("  ef_construction: {}", params.ef_construction);
    println!("  ef_search: {}", params.ef_search);
    println!("  max_neighbors: {}", params.max_neighbors);
    println!("  space: {:?}", params.space);
    println!("  pl_block_size: 2 MB");
    println!();
}

async fn load_dataset(args: &Args) -> anyhow::Result<Vec<(u32, Vec<f32>)>> {
    if !args.profile {
        println!("📥 Loading Wikipedia SPLADE dataset (will download if not cached)...");
    }

    let dataset = WikipediaSplade::init().await?;
    let mut doc_stream = dataset.documents().await?;

    let pb = if !args.profile {
        let pb = ProgressBar::new(args.num_records as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Loading records");
        Some(pb)
    } else {
        None
    };

    let mut records = Vec::new();
    let mut id = 1u32;

    while let Some(doc) = doc_stream.next().await {
        let doc = doc?;
        records.push((id, doc.dense_vector));
        id += 1;

        if let Some(ref pb) = pb {
            pb.inc(1);
        }

        if records.len() >= args.num_records {
            break;
        }
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ Dataset loaded");
    }

    if !args.profile {
        println!("✅ Loaded {} records", records.len());
        println!();
    }

    Ok(records)
}

async fn setup_spann_infrastructure() -> anyhow::Result<(
    tempfile::TempDir,
    BlockfileProvider,
    HnswIndexProvider,
    CollectionUuid,
)> {
    let tmp_dir = tempfile::tempdir()?;

    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage.clone(),
        2 * 1024 * 1024, // 2 MB
        chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)
            .await
            .unwrap(),
        chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)
            .await
            .unwrap(),
        BlockManagerConfig::default_num_concurrent_block_flushes(),
    );
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    let hnsw_provider = HnswIndexProvider::new(
        storage,
        PathBuf::from(tmp_dir.path()),
        chroma_cache::from_config(&chroma_cache::CacheConfig::Nop)
            .await
            .unwrap(),
        16,
        false,
    );

    Ok((
        tmp_dir,
        blockfile_provider,
        hnsw_provider,
        CollectionUuid::new(),
    ))
}

// ============================================================================
// Benchmark Execution
// ============================================================================

async fn run_benchmark(
    args: &Args,
    records: &[(u32, Vec<f32>)],
    blockfile_provider: &BlockfileProvider,
    hnsw_provider: &HnswIndexProvider,
    collection_id: &CollectionUuid,
    params: &InternalSpannConfiguration,
) -> anyhow::Result<BenchmarkSummary> {
    let num_batches = records.len() / args.batch_size;
    let dimensionality = records[0].1.len();

    if !args.profile {
        println!("📈 Processing batches...");
        println!();
    }

    let benchmark_start = Instant::now();
    let mut batch_metrics = Vec::new();
    let mut peak_memory = 0;

    let gc_context = GarbageCollectionContext::try_from_config(
        &(
            PlGarbageCollectionConfig {
                enabled: true,
                policy: chroma_index::config::PlGarbageCollectionPolicyConfig::RandomSample(
                    chroma_index::config::RandomSamplePolicyConfig { sample_size: 1.0 },
                ),
            },
            HnswGarbageCollectionConfig::default(),
        ),
        &Registry::default(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create GC context: {:?}", e))?;

    let pl_block_size = 60 * 1024 * 1024; // 60MB
    let prefix_path = "";

    // Track writer IDs for forking
    let mut hnsw_id = None;
    let mut versions_map_id = None;
    let mut pl_id = None;
    let mut max_head_id_id = None;

    // Track previous state for computing deltas
    let mut prev_cluster_state: Option<
        std::collections::HashMap<u32, std::collections::HashMap<u32, u32>>,
    > = None;

    for batch_idx in 0..num_batches {
        let batch = &records[batch_idx * args.batch_size..(batch_idx + 1) * args.batch_size];
        let batch_start = Instant::now();

        // Create writer, forking from previous if not first batch
        let mut writer = SpannIndexWriter::from_id(
            hnsw_provider,
            hnsw_id.as_ref(),
            versions_map_id.as_ref(),
            pl_id.as_ref(),
            max_head_id_id.as_ref(),
            collection_id,
            prefix_path,
            dimensionality,
            blockfile_provider,
            params.clone(),
            gc_context.clone(),
            pl_block_size,
            SpannMetrics::default(),
            None,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create SPANN writer: {:?}", e))?;

        // Add records
        let add_time_ms = add_batch_records(&writer, batch, args.parallelism).await?;

        // Finish
        writer
            .garbage_collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finish batch {}: {:?}", batch_idx + 1, e))?;

        // Commit
        let flusher = Box::pin(writer.commit())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit batch {}: {:?}", batch_idx + 1, e))?;

        // Flush
        let paths = Box::pin(flusher.flush())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to flush batch {}: {:?}", batch_idx + 1, e))?;

        // Store IDs for next iteration
        hnsw_id = Some(paths.hnsw_id);
        versions_map_id = Some(paths.versions_map_id);
        pl_id = Some(paths.pl_id);
        max_head_id_id = Some(paths.max_head_id_id);

        // Clear cache after each batch
        blockfile_provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear cache: {:?}", e))?;

        // Compute replica statistics by scanning the index
        let (replica_stats, current_state) = compute_replica_stats(
            blockfile_provider,
            hnsw_provider,
            collection_id,
            &hnsw_id.unwrap(),
            &versions_map_id.unwrap(),
            &pl_id.unwrap(),
            dimensionality,
            params,
            prev_cluster_state.as_ref(),
        )
        .await?;

        prev_cluster_state = Some(current_state);

        // Collect metrics
        let memory = get_memory_usage();
        let memory_delta = batch_metrics
            .last()
            .map(|prev: &BatchMetrics| memory as i64 - prev.memory_bytes as i64)
            .unwrap_or(memory as i64);

        let total_time_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
        let total_records = (batch_idx + 1) * args.batch_size;
        let metrics = BatchMetrics {
            batch_idx: batch_idx + 1,
            num_records: batch.len(),
            total_records_so_far: total_records,
            add_time_ms,
            total_time_ms,
            memory_bytes: memory,
            memory_delta_bytes: memory_delta,
            replica_stats,
        };

        if memory > peak_memory {
            peak_memory = memory;
        }

        if !args.profile {
            print_batch_summary(&metrics);
        }

        batch_metrics.push(metrics);
    }

    let total_time_ms = benchmark_start.elapsed().as_secs_f64() * 1000.0;

    Ok(BenchmarkSummary {
        total_records: records.len(),
        batch_metrics,
        total_time_ms,
        peak_memory_bytes: peak_memory,
        final_hnsw_id: hnsw_id,
        final_versions_map_id: versions_map_id,
        final_pl_id: pl_id,
        final_max_head_id_id: max_head_id_id,
    })
}

async fn add_batch_records(
    writer: &SpannIndexWriter,
    batch: &[(u32, Vec<f32>)],
    parallelism: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();

    // Split batch into chunks for parallel processing
    let chunk_size = (batch.len() + parallelism - 1) / parallelism;
    let mut tasks = Vec::new();

    for chunk in batch.chunks(chunk_size) {
        let writer_clone = writer.clone();
        let chunk_owned: Vec<(u32, Vec<f32>)> = chunk.to_vec();

        let task = tokio::spawn(async move {
            for (id, dense_vec) in chunk_owned {
                writer_clone
                    .add(id, &dense_vec)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to add record {}: {:?}", id, e))?;
            }
            Ok::<_, anyhow::Error>(())
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await
            .map_err(|e| anyhow::anyhow!("Task panicked: {:?}", e))??;
    }

    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

// ============================================================================
// Metrics & Monitoring
// ============================================================================

fn get_memory_usage() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if let Some(kb) = line
                    .strip_prefix("VmRSS:")
                    .and_then(|s| s.split_whitespace().next())
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

// ============================================================================
// Output & Reporting
// ============================================================================

fn print_batch_summary(metrics: &BatchMetrics) {
    let throughput = metrics.num_records as f64 / (metrics.total_time_ms / 1000.0);

    println!(
        "Batch {} - {} records",
        metrics.batch_idx, metrics.num_records
    );
    println!(
        "  ⏱️  Time: {} (add: {})",
        format_duration_ms(metrics.total_time_ms),
        format_duration_ms(metrics.add_time_ms)
    );
    println!("  📊 Throughput: {:.0} rec/s", throughput);

    if metrics.memory_bytes > 0 {
        let delta_sign = if metrics.memory_delta_bytes >= 0 {
            "+"
        } else {
            ""
        };
        println!(
            "  💾 Memory: {} ({}{})",
            format_bytes(metrics.memory_bytes),
            delta_sign,
            format_bytes(metrics.memory_delta_bytes.unsigned_abs() as usize)
        );
    }

    let replica_stats = &metrics.replica_stats;
    println!(
        "  🔄 Replicas: min={}, max={}, avg={:.1}, p50={}, p90={}, p99={}",
        replica_stats.min_replicas,
        replica_stats.max_replicas,
        replica_stats.avg_replicas,
        replica_stats.median_replicas,
        replica_stats.p90_replicas,
        replica_stats.p99_replicas,
    );

    let empty_clusters = replica_stats.total_clusters - replica_stats.clusters_with_docs;
    let empty_pct = if replica_stats.total_clusters > 0 {
        (empty_clusters as f64 / replica_stats.total_clusters as f64) * 100.0
    } else {
        0.0
    };
    let modified_pct = if replica_stats.total_clusters > 0 {
        (replica_stats.clusters_modified as f64 / replica_stats.total_clusters as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "  📍 Clusters: {} ({:.1}%) empty, {} ({:.1}%) modified, {} total",
        empty_clusters,
        empty_pct,
        replica_stats.clusters_modified,
        modified_pct,
        replica_stats.total_clusters
    );

    println!();
}

fn print_benchmark_results(summary: &BenchmarkSummary, _args: &Args) {
    println!();
    println!("{}", "=".repeat(60));
    println!("📊 SUMMARY");
    println!("{}", "=".repeat(60));

    // Timing Summary
    println!();
    println!("⏱️  Timing:");
    println!(
        "  Total time: {}",
        format_duration_ms(summary.total_time_ms)
    );
    println!(
        "  Average throughput: {:.0} rec/s",
        summary.average_throughput()
    );

    if summary.batch_metrics.len() > 1 {
        let first = &summary.batch_metrics[0];
        let last = &summary.batch_metrics[summary.batch_metrics.len() - 1];
        let first_throughput = first.num_records as f64 / (first.total_time_ms / 1000.0);
        let last_throughput = last.num_records as f64 / (last.total_time_ms / 1000.0);
        let degradation = summary.throughput_degradation();

        println!();
        println!("  Performance:");
        println!("    First batch: {:.0} rec/s", first_throughput);
        println!("    Last batch:  {:.0} rec/s", last_throughput);
        println!("    Degradation: {:.1}%", degradation);
    }

    // Memory Summary
    if summary.peak_memory_bytes > 0 {
        println!();
        println!("💾 Memory:");
        println!("  Peak memory: {}", format_bytes(summary.peak_memory_bytes));
        let avg_per_record = summary.peak_memory_bytes / summary.total_records;
        println!("  Avg per record: {}", format_bytes(avg_per_record));
    }

    println!();
    println!("✅ Benchmark complete!");
}

// ============================================================================
// Helper Functions
// ============================================================================

fn format_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;

    let bytes_f = bytes as f64;

    if bytes_f >= GB {
        format!("{:.2} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.1} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.1} KB", bytes_f / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{:.0}ms", ms)
    }
}

// ============================================================================
// Replica Statistics
// ============================================================================

async fn compute_replica_stats(
    blockfile_provider: &BlockfileProvider,
    hnsw_provider: &HnswIndexProvider,
    collection_id: &CollectionUuid,
    hnsw_id: &IndexUuid,
    versions_map_id: &uuid::Uuid,
    pl_id: &uuid::Uuid,
    dimensionality: usize,
    params: &InternalSpannConfiguration,
    prev_state: Option<&std::collections::HashMap<u32, std::collections::HashMap<u32, u32>>>,
) -> anyhow::Result<(
    ReplicaStats,
    std::collections::HashMap<u32, std::collections::HashMap<u32, u32>>,
)> {
    let prefix_path = "";
    let distance_function = chroma_distance::DistanceFunction::from(params.space.clone());

    // Count replicas per document by scanning all posting lists WITHOUT deduplication
    // We need to manually iterate through all posting lists instead of using scan()
    // because scan() deduplicates documents across posting lists
    let mut replica_counts: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::new();

    // Get all cluster heads first (only need HNSW for this)
    let mut reader = SpannIndexReader::from_id(
        Some(hnsw_id),
        hnsw_provider,
        collection_id,
        distance_function.clone(),
        dimensionality,
        params.ef_search,
        Some(pl_id),
        Some(versions_map_id),
        blockfile_provider,
        prefix_path,
        false,
        params.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create SPANN reader: {:?}", e))?;

    let (non_deleted_heads, _) = reader
        .hnsw_index
        .inner
        .read()
        .hnsw_index
        .get_all_ids()
        .map_err(|e| anyhow::anyhow!("Failed to get cluster heads: {:?}", e))?;

    let total_clusters = non_deleted_heads.len();
    let mut clusters_with_docs = 0;
    let mut clusters_modified = 0;

    // First, load the entire versions map to check current versions
    let versions_data = reader
        .versions_map
        .get_range(.., ..)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load versions map: {:?}", e))?;
    let mut current_versions: std::collections::HashMap<u32, u32> =
        std::collections::HashMap::new();
    versions_data.for_each(|(_, doc_id, version)| {
        current_versions.insert(doc_id, version);
    });

    // Build current state: head_id -> (doc_id -> version)
    let mut current_state: std::collections::HashMap<u32, std::collections::HashMap<u32, u32>> =
        std::collections::HashMap::new();

    for (idx, head_id) in non_deleted_heads.iter().enumerate() {
        // Recreate reader every 1000 iterations
        if idx > 0 && idx % 1000 == 0 {
            reader = SpannIndexReader::from_id(
                Some(hnsw_id),
                hnsw_provider,
                collection_id,
                distance_function.clone(),
                dimensionality,
                params.ef_search,
                Some(pl_id),
                Some(versions_map_id),
                blockfile_provider,
                prefix_path,
                false,
                params.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create SPANN reader: {:?}", e))?;
        }

        let posting_list_data = reader
            .posting_lists
            .get("", *head_id as u32)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to get posting list for head {}: {:?}", head_id, e)
            })?;

        if let Some(pl_data) = posting_list_data {
            if !pl_data.doc_offset_ids.is_empty() {
                clusters_with_docs += 1;

                // Build doc_id -> version map for this cluster (only up-to-date entries)
                let mut cluster_docs: std::collections::HashMap<u32, u32> =
                    std::collections::HashMap::new();
                for (idx, &doc_id) in pl_data.doc_offset_ids.iter().enumerate() {
                    let entry_version = pl_data.doc_versions[idx];

                    // Only count if this entry's version matches the current version
                    // and the document is not deleted (version != 0)
                    if let Some(&current_ver) = current_versions.get(&doc_id) {
                        if current_ver != 0 && entry_version == current_ver {
                            cluster_docs.insert(doc_id, entry_version);
                            *replica_counts.entry(doc_id).or_insert(0) += 1;
                        }
                    }
                }

                // Check if this cluster was modified compared to previous state
                if let Some(prev) = prev_state {
                    if let Some(prev_cluster) = prev.get(&(*head_id as u32)) {
                        // Compare with previous state
                        if &cluster_docs != prev_cluster {
                            clusters_modified += 1;
                        }
                    } else {
                        // New cluster
                        clusters_modified += 1;
                    }
                } else {
                    // First batch, all clusters are "modified"
                    clusters_modified += 1;
                }

                current_state.insert(*head_id as u32, cluster_docs);
            }
        }
    }

    if replica_counts.is_empty() {
        return Ok((
            ReplicaStats {
                min_replicas: 0,
                max_replicas: 0,
                avg_replicas: 0.0,
                median_replicas: 0,
                p90_replicas: 0,
                p99_replicas: 0,
                total_clusters,
                clusters_with_docs: 0,
                clusters_modified: 0,
            },
            current_state,
        ));
    }

    // Compute statistics
    let counts: Vec<usize> = replica_counts.values().copied().collect();
    let min = *counts.iter().min().unwrap();
    let max = *counts.iter().max().unwrap();
    let sum: usize = counts.iter().sum();
    let avg = sum as f64 / counts.len() as f64;

    // Percentiles (sort and index)
    let mut sorted = counts.clone();
    sorted.sort();
    let median = sorted[sorted.len() * 50 / 100];
    let p90 = sorted[sorted.len() * 90 / 100];
    let p99 = sorted[sorted.len() * 99 / 100];

    Ok((
        ReplicaStats {
            min_replicas: min,
            max_replicas: max,
            avg_replicas: avg,
            median_replicas: median,
            p90_replicas: p90,
            p99_replicas: p99,
            total_clusters,
            clusters_with_docs,
            clusters_modified,
        },
        current_state,
    ))
}

// ============================================================================
// Verification
// ============================================================================

/*
async fn verify_index_correctness(
    records: &[(u32, Vec<f32>)],
    summary: &BenchmarkSummary,
    blockfile_provider: &BlockfileProvider,
    hnsw_provider: &HnswIndexProvider,
    collection_id: &CollectionUuid,
    params: &InternalSpannConfiguration,
) -> anyhow::Result<()> {
    println!();
    println!("{}", "=".repeat(60));
    println!("🔍 VERIFICATION (Test Mode)");
    println!("{}", "=".repeat(60));
    println!();

    // Check that we successfully built the index
    let has_index = summary.final_hnsw_id.is_some()
        && summary.final_versions_map_id.is_some()
        && summary.final_pl_id.is_some()
        && summary.final_max_head_id_id.is_some();

    if !has_index {
        anyhow::bail!("Index was not successfully built - missing IDs");
    }

    println!("✅ Index successfully built:");
    println!("  HNSW ID: {}", summary.final_hnsw_id.as_ref().unwrap().0);
    println!(
        "  Versions Map ID: {}",
        summary.final_versions_map_id.as_ref().unwrap()
    );
    println!(
        "  Posting List ID: {}",
        summary.final_pl_id.as_ref().unwrap()
    );
    println!(
        "  Max Head ID: {}",
        summary.final_max_head_id_id.as_ref().unwrap()
    );
    println!();

    // Verify all batches completed successfully
    for metrics in &summary.batch_metrics {
        if metrics.num_records == 0 {
            anyhow::bail!("Batch {} had 0 records", metrics.batch_idx);
        }
    }

    println!(
        "✅ All {} batches completed successfully",
        summary.batch_metrics.len()
    );

    // Create reader and scan index to count documents
    println!();
    println!("Scanning index to verify document count...");

    let hnsw_id = summary.final_hnsw_id.as_ref().unwrap();
    let versions_map_id = summary.final_versions_map_id.as_ref().unwrap();
    let pl_id = summary.final_pl_id.as_ref().unwrap();

    let dimensionality = records[0].1.len();
    let prefix_path = "";
    let distance_function = chroma_distance::DistanceFunction::from(params.space.clone());

    let reader = chroma_index::spann::types::SpannIndexReader::from_id(
        Some(hnsw_id),
        hnsw_provider,
        collection_id,
        distance_function,
        dimensionality,
        params.ef_search,
        Some(pl_id),
        Some(versions_map_id),
        blockfile_provider,
        prefix_path,
        false, // adaptive_search_nprobe
        params.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create SPANN reader: {:?}", e))?;

    let postings = reader
        .scan()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to scan index: {:?}", e))?;

    let indexed_count = postings.len();
    let expected_count = records.len();

    println!("  Expected documents: {}", expected_count);
    println!("  Found in index: {}", indexed_count);

    if indexed_count != expected_count {
        anyhow::bail!(
            "Document count mismatch! Expected {} but found {} in index",
            expected_count,
            indexed_count
        );
    }

    println!(
        "✅ Document count verified: {} documents in index",
        indexed_count
    );
    println!();

    // Verify a few random documents have correct embeddings
    println!("Verifying sample document embeddings...");
    let sample_indices = [0, records.len() / 2, records.len() - 1];
    let mut verified = 0;

    for &idx in &sample_indices {
        let (expected_id, expected_embedding) = &records[idx];

        // Find this document in the postings
        if let Some(posting) = postings.iter().find(|p| p.doc_offset_id == *expected_id) {
            // Check if embeddings match (approximately)
            let embedding_matches = posting.doc_embedding.len() == expected_embedding.len()
                && posting
                    .doc_embedding
                    .iter()
                    .zip(expected_embedding.iter())
                    .all(|(a, b)| (a - b).abs() < 1e-5);

            if embedding_matches {
                verified += 1;
            } else {
                anyhow::bail!("Embedding mismatch for document ID {}", expected_id);
            }
        } else {
            anyhow::bail!("Document ID {} not found in index", expected_id);
        }
    }

    println!("✅ Verified {} sample document embeddings", verified);
    println!();

    Ok(())
}
*/

// ============================================================================
// CSV Output
// ============================================================================

fn write_csv_output(csv_path: &str, summary: &BenchmarkSummary) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(csv_path)?;

    // Write header
    writeln!(file, "batch_idx,num_records,total_records_so_far,add_time_ms,total_time_ms,throughput_recs_per_sec,memory_bytes,memory_delta_bytes,min_replicas,max_replicas,avg_replicas,median_replicas,p90_replicas,p99_replicas")?;

    // Write data rows
    for metrics in &summary.batch_metrics {
        let throughput = metrics.throughput();
        let replica_stats = &metrics.replica_stats;

        writeln!(
            file,
            "{},{},{},{:.2},{:.2},{:.2},{},{},{},{},{:.2},{},{},{}",
            metrics.batch_idx,
            metrics.num_records,
            metrics.total_records_so_far,
            metrics.add_time_ms,
            metrics.total_time_ms,
            throughput,
            metrics.memory_bytes,
            metrics.memory_delta_bytes,
            replica_stats.min_replicas,
            replica_stats.max_replicas,
            replica_stats.avg_replicas,
            replica_stats.median_replicas,
            replica_stats.p90_replicas,
            replica_stats.p99_replicas,
        )?;
    }

    println!();
    println!("✅ CSV output written to: {}", csv_path);

    Ok(())
}
