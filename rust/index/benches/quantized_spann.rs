//! Benchmark for QuantizedSpannIndexWriter (RaBitQ-quantized SPANN) add throughput.

#![recursion_limit = "256"]

mod datasets;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_blockstore::{
    arrow::provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
    provider::BlockfileProvider,
    BlockfileWriterOptions,
};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_index::{
    spann::quantized_spann::{MethodSnapshot, QuantizedSpannIds, QuantizedSpannIndexWriter},
    usearch::{USearchIndex, USearchIndexProvider},
};
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, DataRecord, Quantization, SpannIndexConfig};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;

use datasets::arxiv::Arxiv;
use datasets::dbpedia::DbPedia;
use datasets::msmarco::MsMarco;
use datasets::sec::Sec;
use datasets::synthetic::Synthetic;
use datasets::wikipedia::Wikipedia;
use datasets::{format_count, recall_at_k, Dataset, DatasetType, MetricType, Query};

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "quantized_spann_benchmark")]
#[command(about = "Benchmark for QuantizedSpannIndexWriter")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Dataset to use
    #[arg(long, default_value = "wikipedia-en")]
    dataset: DatasetType,

    /// Distance metric
    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Number of checkpoints to run
    #[arg(long)]
    checkpoint: Option<usize>,

    /// Vectors per checkpoint (default 1M)
    #[arg(long, default_value = "1000000")]
    checkpoint_size: usize,

    /// nprobe values for recall evaluation, comma-separated (e.g. "32,64,128,256")
    #[arg(long, default_value = "32,64,128,256")]
    nprobes: String,

    /// Number of threads for parallel indexing
    #[arg(long, default_value = "32")]
    threads: usize,

    /// Data vector quantization bit-width (1 or 4, default 1)
    #[arg(long, default_value = "1")]
    data_bits: u8,

    /// Quantization bit-width for centroids (1 or 4). Defaults to data_bits.
    #[arg(long)]
    centroid_bits: Option<u8>,

    /// Vector dimension (only used with --dataset synthetic)
    #[arg(long, default_value = "1024")]
    dim: usize,

    /// Number of vectors for synthetic dataset (default 1M)
    #[arg(long, default_value = "1000000")]
    synthetic_size: usize,

    /// Centroid rerank factor for navigate (single value)
    #[arg(long, default_value = "1")]
    centroid_rerank: u32,

    /// Data vector rerank factors to sweep, comma-separated (e.g. "4,8,16")
    #[arg(long, default_value = "16")]
    data_rerank_factors: String,

    /// Print the method legend after the stats tables
    #[arg(long)]
    legend: bool,

    /// Compute brute-force ground truth when precomputed GT is missing (slow at scale)
    #[arg(long)]
    brute_force_gt: bool,

    /// Directory for temporary storage (blockfiles, indexes). Defaults to system temp.
    #[arg(long)]
    tmp_dir: Option<String>,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// CONFIGURATION
// =============================================================================

const BLOCK_SIZE_BYTES: usize = 3 * 1024 * 1024; // 3MB

// =============================================================================
// Checkpoint Result
// =============================================================================

struct RerankRecall {
    rerank_factor: u32,
    recall_10: f64,
    recall_100: f64,
    /// Per-query avg wall time for full `search()` (same role as former `latency`).
    search: MethodSnapshot,
    search_scan: MethodSnapshot,
    search_load_cluster: MethodSnapshot,
    search_load_raw: MethodSnapshot,
    search_rerank: MethodSnapshot,
    avg_rerank_vecs: u64,
    rerank_data_bytes: u64,
}

struct CheckpointResult {
    checkpoint: usize,
    vectors: usize,
    num_queries: usize,
    nprobe: u32,
    rerank_results: Vec<RerankRecall>,
}

// =============================================================================
// SPANN Configuration
// =============================================================================

fn spann_config(
    data_bits: u8,
    centroid_bits: Option<u8>,
    centroid_rerank_factor: Option<u32>,
    data_rerank_factor: Option<u32>,
) -> SpannIndexConfig {
    let quantize = match data_bits {
        4 => Quantization::FourBitRabitQWithUSearch,
        _ => Quantization::OneBitRabitQWithUSearch,
    };

    SpannIndexConfig {
        // Write path parameters
        write_nprobe: Some(64),
        nreplica_count: Some(2),
        write_rng_epsilon: Some(8.0),
        write_rng_factor: Some(4.0),

        // Cluster maintenance
        split_threshold: Some(2048),
        merge_threshold: Some(512),
        reassign_neighbor_count: Some(32),

        // Commit-time parameters
        center_drift_threshold: Some(0.125),

        // HNSW parameters
        ef_construction: Some(256),
        ef_search: Some(128),
        max_neighbors: Some(24),

        quantize,
        centroid_bits,
        centroid_rerank_factor,
        data_rerank_factor,

        // Other
        ..Default::default()
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.1}m", secs / 60.0)
    }
}

/// Compute brute-force kNN ground truth for query vectors against a set of data vectors.
fn compute_ground_truth(
    query_vectors: &[Vec<f32>],
    data_vectors: &[(u32, Arc<[f32]>)],
    distance_fn: &DistanceFunction,
    k: usize,
) -> Vec<Query> {
    query_vectors
        .iter()
        .map(|qv| {
            let mut dists: Vec<(u32, f32)> = data_vectors
                .iter()
                .map(|(id, emb)| (*id, distance_fn.distance(qv, emb)))
                .collect();
            dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            let neighbors: Vec<u32> = dists.iter().take(k).map(|(id, _)| *id).collect();
            Query {
                vector: qv.clone(),
                neighbors,
                max_vector_id: data_vectors.len() as u64,
            }
        })
        .collect()
}

/// Group queries by their max_vector_id (checkpoint boundary).
fn group_queries_by_checkpoint(queries: Vec<Query>) -> BTreeMap<u64, Vec<Query>> {
    let mut map: BTreeMap<u64, Vec<Query>> = BTreeMap::new();
    for q in queries {
        map.entry(q.max_vector_id).or_default().push(q);
    }
    map
}

/// Evaluate recall for a set of queries against the index.
/// Returns (avg_recall@10, avg_recall@100, avg_search_latency).
async fn evaluate_recall(
    index: Arc<QuantizedSpannIndexWriter<USearchIndex>>,
    queries: &[&Query],
    k: usize,
    nprobe: usize,
    num_threads: usize,
    checkpoint_idx: usize,
    num_checkpoints: usize,
) -> (f64, f64, Duration) {
    if queries.is_empty() {
        return (0.0, 0.0, Duration::ZERO);
    }

    let total_recall_10 = Arc::new(AtomicUsize::new(0));
    let total_recall_100 = Arc::new(AtomicUsize::new(0));
    let total_search_nanos = Arc::new(AtomicUsize::new(0));
    let num_evaluated = Arc::new(AtomicUsize::new(0));

    let chunk_size = (queries.len() + num_threads - 1) / num_threads;
    let query_chunks: Vec<Vec<Query>> = queries
        .chunks(chunk_size)
        .map(|c| c.iter().map(|q| (*q).clone()).collect())
        .collect();

    let progress = ProgressBar::new(queries.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "[CP {}/{} Recall@{}] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                checkpoint_idx + 1,
                num_checkpoints,
                nprobe
            ))
            .unwrap(),
    );

    let handles: Vec<_> = query_chunks
        .into_iter()
        .map(|chunk| {
            let index = Arc::clone(&index);
            let total_recall_10 = Arc::clone(&total_recall_10);
            let total_recall_100 = Arc::clone(&total_recall_100);
            let total_search_nanos = Arc::clone(&total_search_nanos);
            let num_evaluated = Arc::clone(&num_evaluated);
            let progress = progress.clone();
            tokio::spawn(async move {
                let mut local_recall_10_sum: f64 = 0.0;
                let mut local_recall_100_sum: f64 = 0.0;
                let mut local_nanos: u64 = 0;
                let mut local_count: usize = 0;

                for query in chunk {
                    let t0 = Instant::now();
                    let results = index
                        .search(k, &query.vector, nprobe)
                        .await
                        .expect("Search failed");
                    local_nanos += t0.elapsed().as_nanos() as u64;
                    local_recall_10_sum += recall_at_k(&results.keys, &query.neighbors, 10);
                    local_recall_100_sum += recall_at_k(&results.keys, &query.neighbors, 100);
                    local_count += 1;
                    progress.inc(1);
                }

                total_recall_10.fetch_add(
                    (local_recall_10_sum * 1_000_000.0) as usize,
                    Ordering::Relaxed,
                );
                total_recall_100.fetch_add(
                    (local_recall_100_sum * 1_000_000.0) as usize,
                    Ordering::Relaxed,
                );
                total_search_nanos.fetch_add(local_nanos as usize, Ordering::Relaxed);
                num_evaluated.fetch_add(local_count, Ordering::Relaxed);
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Task failed");
    }
    progress.finish_and_clear();

    let total_recall_10_value = total_recall_10.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let total_recall_100_value = total_recall_100.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let num_queries = num_evaluated.load(Ordering::Relaxed);

    let avg_recall_10 = if num_queries > 0 {
        total_recall_10_value / num_queries as f64
    } else {
        0.0
    };
    let avg_recall_100 = if num_queries > 0 {
        total_recall_100_value / num_queries as f64
    } else {
        0.0
    };
    let avg_latency = if num_queries > 0 {
        Duration::from_nanos(
            (total_search_nanos.load(Ordering::Relaxed) as u64) / (num_queries as u64),
        )
    } else {
        Duration::ZERO
    };

    (avg_recall_10, avg_recall_100, avg_latency)
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn format_latency(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 10.0 {
        format!("{:.1}ms", ms)
    } else if ms < 1000.0 {
        format!("{:.0}ms", ms)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

fn format_avg_method(m: MethodSnapshot) -> String {
    if let Some(avg) = m.avg_nanos() {
        format_latency(Duration::from_nanos(avg))
    } else {
        "-".to_string()
    }
}

/// Print the recall summary table (one row per nprobe / DRR combination).
fn print_recall_summary(results: &[CheckpointResult]) {
    println!("\n=== Recall Summary ===");
    println!(
        "| {:>3} | {:>6} | {:>6} | {:>8} | {:>7} | {:>7} | {:>7} | {:>6} | {:>6} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} |",
        "CP",
        "nprobe",
        "DRR",
        "Vectors",
        "Queries",
        "RR Vecs",
        "RR Data",
        "R@10",
        "R@100",
        "search",
        "scan",
        "ld_cl",
        "ld_raw",
        "rerank"
    );
    println!(
        "|{:-^5}|{:-^8}|{:-^8}|{:-^10}|{:-^9}|{:-^9}|{:-^9}|{:-^8}|{:-^8}|{:-^9}|{:-^9}|{:-^9}|{:-^9}|{:-^9}|",
        "", "", "", "", "", "", "", "", "", "", "", "", "", ""
    );

    for r in results {
        for rr in &r.rerank_results {
            println!(
                "| {:>3} | {:>6} | {:>4}x | {:>8} | {:>7} | {:>7} | {:>7} | {:>6.2} | {:>6.2} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} |",
                r.checkpoint,
                r.nprobe,
                rr.rerank_factor,
                format_count(r.vectors),
                r.num_queries,
                rr.avg_rerank_vecs,
                format_bytes(rr.rerank_data_bytes),
                rr.recall_10,
                rr.recall_100,
                format_avg_method(rr.search),
                format_avg_method(rr.search_scan),
                format_avg_method(rr.search_load_cluster),
                format_avg_method(rr.search_load_raw),
                format_avg_method(rr.search_rerank),
            );
        }
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let distance_function = args.metric.to_distance_function();
    let num_threads = args.threads;

    // Load dataset based on CLI arg
    let dataset: Box<dyn Dataset> = match args.dataset {
        DatasetType::DbPedia => Box::new(DbPedia::load().await?),
        DatasetType::Arxiv => Box::new(Arxiv::load().await?),
        DatasetType::Sec => Box::new(Sec::load().await?),
        DatasetType::MsMarco => Box::new(MsMarco::load().await?),
        DatasetType::WikipediaEn => Box::new(Wikipedia::load().await?),
        DatasetType::Synthetic => Box::new(Synthetic::load(args.dim, args.synthetic_size)?),
    };

    let data_len = dataset.data_len();
    let dimension = dataset.dimension();
    let k = dataset.k();
    let batch_size = args.checkpoint_size;

    // Calculate number of checkpoints
    let max_checkpoints = (data_len + batch_size - 1) / batch_size;
    let num_checkpoints = args
        .checkpoint
        .unwrap_or(max_checkpoints)
        .min(max_checkpoints);

    let data_rerank_factors: Vec<u32> = args
        .data_rerank_factors
        .split(',')
        .map(|s| s.trim().parse().expect("invalid data rerank factor"))
        .collect();

    let nprobes: Vec<u32> = args
        .nprobes
        .split(',')
        .map(|s| s.trim().parse().expect("invalid nprobe value"))
        .collect();

    let centroid_rerank = args.centroid_rerank;

    let config = spann_config(
        args.data_bits,
        args.centroid_bits,
        Some(centroid_rerank),
        None,
    );

    println!("=== QuantizedSpannIndexWriter Benchmark ===");
    println!(
        "Dataset: {} ({} vectors, {} dims)",
        dataset.name(),
        format_count(data_len),
        dimension
    );
    println!(
        "Metric: {:?} | Checkpoints: {} ({} vec/CP) | Threads: {} | Data bits: {} | Centroid bits: {}",
        distance_function,
        num_checkpoints,
        format_count(batch_size),
        num_threads,
        args.data_bits,
        config.centroid_bits()
    );
    println!(
        "Centroid rerank: {}x | Data rerank factors: {:?} | nprobes: {:?}",
        centroid_rerank, data_rerank_factors, nprobes
    );
    println!(
        "Total vectors to index: {}",
        format_count((batch_size * num_checkpoints).min(data_len))
    );
    println!();

    // Load and group queries by checkpoint
    let all_queries = dataset.queries(distance_function.clone())?;
    let query_vectors: Vec<Vec<f32>> = all_queries.iter().take(100).map(|q| q.vector.clone()).collect();
    let queries_by_checkpoint = group_queries_by_checkpoint(all_queries);

    // Setup temp directory and storage
    let tmp_dir = if let Some(ref base) = args.tmp_dir {
        tempfile::tempdir_in(base)?
    } else {
        tempfile::tempdir()?
    };
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

    let collection_id = CollectionUuid::new();

    // Run checkpoints with incremental raw embedding building
    let mut total_vectors = 0usize;
    let mut all_indexed_vectors: Vec<(u32, Arc<[f32]>)> = Vec::new();
    let mut file_ids: Option<QuantizedSpannIds> = None;
    let mut raw_embedding_id: Option<Uuid> = None;
    let total_start = Instant::now();

    let mut batch_snapshots: Vec<chroma_index::spann::quantized_spann::StatsSnapshot> = Vec::new();
    let mut checkpoint_results: Vec<CheckpointResult> = Vec::new();

    for checkpoint_idx in 0..num_checkpoints {
        let offset = checkpoint_idx * batch_size;
        let limit = batch_size.min(data_len.saturating_sub(offset));

        if limit == 0 {
            println!("Checkpoint {}: No more data available", checkpoint_idx);
            break;
        }

        // === Step 1: Load checkpoint vectors ===
        let load_start = Instant::now();
        let batch_vectors = dataset.load_range(offset, limit)?;
        let load_time = load_start.elapsed();
        let actual_count = batch_vectors.len();

        if actual_count == 0 {
            println!("Checkpoint {}: No vectors loaded", checkpoint_idx);
            break;
        }

        // === Step 2: Write raw embeddings incrementally (fork -> append -> commit -> flush) ===
        let raw_write_start = Instant::now();
        {
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let arrow_blockfile_provider = ArrowBlockfileProvider::new(
                storage.clone(),
                BLOCK_SIZE_BYTES,
                block_cache,
                sparse_index_cache,
                16,
            );
            let blockfile_provider =
                BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

            // Create writer: fork from previous if exists, otherwise create new
            let mut options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
            if let Some(prev_id) = raw_embedding_id {
                options = options.fork(prev_id);
            }

            let raw_writer = blockfile_provider
                .write::<u32, &DataRecord<'_>>(options)
                .await
                .expect("Failed to create raw embedding writer");

            // Write batch embeddings
            let raw_progress = ProgressBar::new(batch_vectors.len() as u64);
            raw_progress.set_style(
                ProgressStyle::default_bar()
                    .template(&format!(
                        "[CP {}/{} Raw] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                        checkpoint_idx + 1,
                        num_checkpoints
                    ))
                    .unwrap(),
            );

            for (id, embedding) in &batch_vectors {
                let record = DataRecord {
                    id: "",
                    embedding: &embedding,
                    metadata: None,
                    document: None,
                };
                raw_writer
                    .set("", *id, &record)
                    .await
                    .expect("Failed to write embedding");
                raw_progress.inc(1);
            }
            raw_progress.finish_and_clear();

            // Commit and flush
            let flusher = raw_writer
                .commit::<u32, &DataRecord<'_>>()
                .await
                .expect("Failed to commit raw embeddings");
            raw_embedding_id = Some(flusher.id());
            flusher
                .flush::<u32, &DataRecord<'_>>()
                .await
                .expect("Failed to flush raw embeddings");
        }
        let raw_write_time = raw_write_start.elapsed();

        // === Step 3: Setup providers for indexing ===
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            16,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

        let usearch_cache = new_non_persistent_cache_for_test();
        let usearch_provider = USearchIndexProvider::new(storage.clone(), usearch_cache);

        // === Step 4: Create or open index ===
        let index = if let Some(ids) = &file_ids {
            // Open existing index with raw embedding reader
            let raw_reader = blockfile_provider
                .read::<u32, DataRecord<'static>>(BlockfileReaderOptions::new(
                    raw_embedding_id.unwrap(),
                    "".to_string(),
                ))
                .await
                .expect("Failed to open raw embedding reader");

            QuantizedSpannIndexWriter::<USearchIndex>::open(
                BLOCK_SIZE_BYTES,
                collection_id,
                config.clone(),
                dimension,
                distance_function.clone(),
                ids.clone(),
                None,
                "".to_string(),
                Some(raw_reader),
                &blockfile_provider,
                &usearch_provider,
            )
            .await
            .expect("Failed to open index")
        } else {
            // Create new index
            QuantizedSpannIndexWriter::<USearchIndex>::create(
                BLOCK_SIZE_BYTES,
                collection_id,
                config.clone(),
                dimension,
                distance_function.clone(),
                None,
                "".to_string(),
                &usearch_provider,
            )
            .await
            .expect("Failed to create index")
        };

        let index = Arc::new(index);

        // === Step 5: Index batch vectors ===
        let chunk_size = (actual_count + num_threads - 1) / num_threads;
        let chunks = batch_vectors
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect::<Vec<_>>();

        let progress = ProgressBar::new(actual_count as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "[CP {}/{} Index] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                    checkpoint_idx + 1,
                    num_checkpoints
                ))
                .unwrap(),
        );

        let index_start = Instant::now();
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let index = Arc::clone(&index);
                let progress = progress.clone();
                tokio::spawn(async move {
                    for (id, vec) in chunk {
                        index.add(id, &vec).await.expect("Failed to add vector");
                        progress.inc(1);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await?;
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed();

        // === Step 6: Commit and flush index ===
        let commit_start = Instant::now();
        let index = Arc::try_unwrap(index)
            .ok()
            .expect("Index still has references");

        // Capture stats snapshot before commit consumes the index
        let cluster_sizes = index.cluster_sizes();
        let mut snap = index.stats().snapshot(&cluster_sizes, dimension);

        let flusher = index
            .commit(&blockfile_provider, &usearch_provider)
            .await
            .expect("Failed to commit");
        file_ids = Some(flusher.flush().await.expect("Failed to flush"));
        let commit_time = commit_start.elapsed();

        let checkpoint_wall = load_time + raw_write_time + index_time + commit_time;
        snap.wall_nanos = checkpoint_wall.as_nanos() as u64;
        batch_snapshots.push(snap);

        total_vectors += actual_count;
        all_indexed_vectors.extend(batch_vectors.iter().cloned());

        // === Step 7: Evaluate recall for this checkpoint ===
        // Use pre-computed ground truth if available for this boundary.
        // Skip recall evaluation if no precomputed GT exists (brute-force is too
        // slow at scale). Use --brute-force-gt to force on-the-fly computation.
        let precomputed: Vec<&Query> = queries_by_checkpoint
            .get(&(total_vectors as u64))
            .map(|qs| qs.iter().collect())
            .unwrap_or_default();

        let computed_gt;
        let checkpoint_queries: Vec<&Query> = if !precomputed.is_empty() {
            precomputed
        } else if args.brute_force_gt {
            println!("  Computing brute-force ground truth ({} queries x {} vectors)...",
                query_vectors.len(), all_indexed_vectors.len());
            let gt_start = Instant::now();
            computed_gt = compute_ground_truth(
                &query_vectors,
                &all_indexed_vectors,
                &distance_function,
                k,
            );
            println!("  Ground truth computed in {}", format_duration(gt_start.elapsed()));
            computed_gt.iter().collect()
        } else {
            Vec::new()
        };
        let checkpoint_queries: &[&Query] = &checkpoint_queries;

        let throughput = actual_count as f64 / index_time.as_secs_f64();

        println!(
            "Checkpoint {}: {} vec | load {} | raw {} | index {} | commit {} | {:.0} vec/s",
            checkpoint_idx + 1,
            format_count(actual_count),
            format_duration(load_time),
            format_duration(raw_write_time),
            format_duration(index_time),
            format_duration(commit_time),
            throughput,
        );

        if checkpoint_queries.is_empty() {
            println!("  (no precomputed ground truth for {}M boundary, skipping recall)", total_vectors / 1_000_000);
        }

        for &nprobe in &nprobes {
            let mut rerank_results = Vec::with_capacity(data_rerank_factors.len());

            for &data_rerank_factor in &data_rerank_factors {
                let (r10, r100, avg_rerank_vecs, rerank_data_bytes, sm) =
                    if !checkpoint_queries.is_empty() {
                        let search_config = spann_config(
                            args.data_bits,
                            args.centroid_bits,
                            Some(centroid_rerank),
                            Some(data_rerank_factor),
                        );

                        let block_cache = new_cache_for_test();
                        let sparse_index_cache = new_cache_for_test();
                        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
                            storage.clone(),
                            BLOCK_SIZE_BYTES,
                            block_cache,
                            sparse_index_cache,
                            16,
                        );
                        let blockfile_provider =
                            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

                        let usearch_cache = new_non_persistent_cache_for_test();
                        let usearch_provider =
                            USearchIndexProvider::new(storage.clone(), usearch_cache);

                        let raw_reader = blockfile_provider
                            .read::<u32, DataRecord<'static>>(BlockfileReaderOptions::new(
                                raw_embedding_id.unwrap(),
                                "".to_string(),
                            ))
                            .await
                            .expect("Failed to open raw embedding reader");

                        let search_index = QuantizedSpannIndexWriter::<USearchIndex>::open(
                            BLOCK_SIZE_BYTES,
                            collection_id,
                            search_config,
                            dimension,
                            distance_function.clone(),
                            file_ids.clone().unwrap(),
                            None,
                            "".to_string(),
                            Some(raw_reader),
                            &blockfile_provider,
                            &usearch_provider,
                        )
                        .await
                        .expect("Failed to open index for search");

                        let search_index = Arc::new(search_index);

                        let (r10, r100, _lat) = evaluate_recall(
                            Arc::clone(&search_index),
                            checkpoint_queries,
                            k,
                            nprobe as usize,
                            num_threads,
                            checkpoint_idx,
                            num_checkpoints,
                        )
                        .await;

                        let search_stats_ref = search_index.stats();
                        let search_snap = search_stats_ref.snapshot(&[], dimension);

                        let per_search_vecs = if search_snap.search.calls > 0 {
                            search_snap.data_rerank_vectors / search_snap.search.calls
                        } else {
                            0
                        };
                        let per_search_bytes = per_search_vecs * dimension as u64 * 4;

                        (
                            r10,
                            r100,
                            per_search_vecs,
                            per_search_bytes,
                            (
                                search_snap.search,
                                search_snap.search_scan,
                                search_snap.search_load_cluster,
                                search_snap.search_load_raw,
                                search_snap.search_rerank,
                            ),
                        )
                    } else {
                        (
                            0.0,
                            0.0,
                            0,
                            0,
                            (
                                MethodSnapshot::default(),
                                MethodSnapshot::default(),
                                MethodSnapshot::default(),
                                MethodSnapshot::default(),
                                MethodSnapshot::default(),
                            ),
                        )
                    };

                rerank_results.push(RerankRecall {
                    rerank_factor: data_rerank_factor,
                    recall_10: r10,
                    recall_100: r100,
                    search: sm.0,
                    search_scan: sm.1,
                    search_load_cluster: sm.2,
                    search_load_raw: sm.3,
                    search_rerank: sm.4,
                    avg_rerank_vecs,
                    rerank_data_bytes,
                });
            }

            let drr_str: String = rerank_results
                .iter()
                .map(|r| {
                    format!(
                        "drr{}x={:.2}/{:.2}",
                        r.rerank_factor, r.recall_10, r.recall_100
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            let search_avgs: String = rerank_results
                .iter()
                .map(|r| format_avg_method(r.search))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  nprobe {}: {} | search_avg={}", nprobe, drr_str, search_avgs);

            checkpoint_results.push(CheckpointResult {
                checkpoint: checkpoint_idx + 1,
                vectors: total_vectors,
                num_queries: checkpoint_queries.len(),
                nprobe,
                rerank_results,
            });
        }
    }

    let total_time = total_start.elapsed();
    let overall_throughput = total_vectors as f64 / total_time.as_secs_f64();

    // Print method statistics tables
    println!(
        "{}",
        chroma_index::spann::quantized_spann::format_batch_tables(&batch_snapshots, args.legend)
    );

    println!("\n=== Indexing Summary ===");
    println!("Total vectors: {}", format_count(total_vectors));
    println!("Total time: {}", format_duration(total_time));
    println!("Overall throughput: {:.0} vec/s", overall_throughput);

    // Print recall summary table
    print_recall_summary(&checkpoint_results);

    println!("\nDone!");

    Ok(())
}
