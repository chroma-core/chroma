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
    spann::quantized_spann::{QuantizedSpannIds, QuantizedSpannIndexWriter},
    usearch::{USearchIndex, USearchIndexProvider},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, DataRecord, Quantization, SpannIndexConfig};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;

use datasets::arxiv::Arxiv;
use datasets::dbpedia::DbPedia;
use datasets::msmarco::MsMarco;
use datasets::sec::Sec;
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
    #[arg(long, default_value = "db-pedia")]
    dataset: DatasetType,

    /// Distance metric
    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Number of checkpoints to run (each checkpoint = 1M vectors)
    #[arg(long)]
    checkpoint: Option<usize>,

    /// Number of threads for parallel indexing
    #[arg(long, default_value = "16")]
    threads: usize,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// CONFIGURATION
// =============================================================================

const BLOCK_SIZE_BYTES: usize = 3 * 1024 * 1024; // 3MB
const BATCH_SIZE: usize = 1_000_000; // 1M vectors per checkpoint (matches ground truth)

// =============================================================================
// Checkpoint Result
// =============================================================================

struct CheckpointResult {
    checkpoint: usize,
    vectors: usize,
    index_time: Duration,
    commit_time: Duration,
    num_queries: usize,
    recall_10_np16: f64,
    recall_100_np16: f64,
    recall_10_np32: f64,
    recall_100_np32: f64,
    recall_10_np64: f64,
    recall_100_np64: f64,
    recall_10_np128: f64,
    recall_100_np128: f64,
    recall_10_np256: f64,
    recall_100_np256: f64,
}

// =============================================================================
// SPANN Configuration
// =============================================================================

fn spann_config() -> SpannIndexConfig {
    SpannIndexConfig {
        // Write path parameters
        write_nprobe: Some(64),
        nreplica_count: Some(2),
        write_rng_epsilon: Some(8.0),
        write_rng_factor: Some(1.0),

        // Cluster maintenance
        split_threshold: Some(512),
        merge_threshold: Some(128),
        reassign_neighbor_count: Some(32),

        // Commit-time parameters
        center_drift_threshold: Some(0.125),

        // HNSW parameters
        ef_construction: Some(256),
        ef_search: Some(128),
        max_neighbors: Some(24),

        // Flag
        quantize: Quantization::FourBitRabitQWithUSearch,

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

/// Group queries by their max_vector_id (checkpoint boundary).
fn group_queries_by_checkpoint(queries: Vec<Query>) -> BTreeMap<u64, Vec<Query>> {
    let mut map: BTreeMap<u64, Vec<Query>> = BTreeMap::new();
    for q in queries {
        map.entry(q.max_vector_id).or_default().push(q);
    }
    map
}

/// Evaluate recall for a set of queries against the index.
async fn evaluate_recall(
    index: Arc<QuantizedSpannIndexWriter<USearchIndex>>,
    queries: &[Query],
    k: usize,
    nprobe: usize,
    num_threads: usize,
    checkpoint_idx: usize,
    num_checkpoints: usize,
) -> (f64, f64) {
    if queries.is_empty() {
        return (0.0, 0.0);
    }

    let total_recall_10 = Arc::new(AtomicUsize::new(0));
    let total_recall_100 = Arc::new(AtomicUsize::new(0));
    let num_evaluated = Arc::new(AtomicUsize::new(0));

    let chunk_size = (queries.len() + num_threads - 1) / num_threads;
    let query_chunks: Vec<Vec<Query>> = queries.chunks(chunk_size).map(|c| c.to_vec()).collect();

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
            let num_evaluated = Arc::clone(&num_evaluated);
            let progress = progress.clone();
            tokio::spawn(async move {
                let mut local_recall_10_sum: f64 = 0.0;
                let mut local_recall_100_sum: f64 = 0.0;
                let mut local_count: usize = 0;

                for query in chunk {
                    let results = index
                        .search(k, &query.vector, nprobe)
                        .await
                        .expect("Search failed");
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

    (avg_recall_10, avg_recall_100)
}

/// Print the recall summary table.
fn print_recall_summary(results: &[CheckpointResult]) {
    println!("\n=== Recall Summary ===");
    println!(
        "| {:>3} | {:>8} | {:>8} | {:>8} | {:>7} | {:^13} | {:^13} | {:^13} | {:^13} | {:^13} |",
        "CP",
        "Vectors",
        "Index",
        "Commit",
        "Queries",
        "nprobe=16",
        "nprobe=32",
        "nprobe=64",
        "nprobe=128",
        "nprobe=256"
    );
    println!(
        "| {:>3} | {:>8} | {:>8} | {:>8} | {:>7} | {:>5}  {:>6} | {:>5}  {:>6} | {:>5}  {:>6} | {:>5}  {:>6} | {:>5}  {:>6} |",
        "", "", "", "", "", "R@10", "R@100", "R@10", "R@100", "R@10", "R@100", "R@10", "R@100", "R@10", "R@100"
    );
    println!(
        "|{:-^5}|{:-^10}|{:-^10}|{:-^10}|{:-^9}|{:-^15}|{:-^15}|{:-^15}|{:-^15}|{:-^15}|",
        "", "", "", "", "", "", "", "", "", ""
    );

    for r in results {
        println!(
            "| {:>3} | {:>8} | {:>8} | {:>8} | {:>7} | {:>6.4} {:>6.4} | {:>6.4} {:>6.4} | {:>6.4} {:>6.4} | {:>6.4} {:>6.4} | {:>6.4} {:>6.4} |",
            r.checkpoint,
            format_count(r.vectors),
            format_duration(r.index_time),
            format_duration(r.commit_time),
            r.num_queries,
            r.recall_10_np16,
            r.recall_100_np16,
            r.recall_10_np32,
            r.recall_100_np32,
            r.recall_10_np64,
            r.recall_100_np64,
            r.recall_10_np128,
            r.recall_100_np128,
            r.recall_10_np256,
            r.recall_100_np256
        );
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
    };

    let data_len = dataset.data_len();
    let dimension = dataset.dimension();
    let k = dataset.k();

    // Calculate number of checkpoints (each checkpoint = 1M vectors)
    let max_checkpoints = (data_len + BATCH_SIZE - 1) / BATCH_SIZE;
    let num_checkpoints = args
        .checkpoint
        .unwrap_or(max_checkpoints)
        .min(max_checkpoints);

    println!("=== QuantizedSpannIndexWriter Benchmark ===");
    println!(
        "Dataset: {} ({} vectors, {} dims)",
        dataset.name(),
        format_count(data_len),
        dimension
    );
    println!(
        "Metric: {:?} | Checkpoints: {} | Threads: {}",
        distance_function, num_checkpoints, num_threads
    );
    println!(
        "Total vectors to index: {}",
        format_count((BATCH_SIZE * num_checkpoints).min(data_len))
    );
    println!();

    // Load and group queries by checkpoint
    let all_queries = dataset.queries(distance_function.clone())?;
    let queries_by_checkpoint = group_queries_by_checkpoint(all_queries);

    // Setup temp directory and storage
    let tmp_dir = tempfile::tempdir()?;
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

    let collection_id = CollectionUuid::new();
    let config = spann_config();

    // Run checkpoints with incremental raw embedding building
    let mut total_vectors = 0usize;
    let mut file_ids: Option<QuantizedSpannIds> = None;
    let mut raw_embedding_id: Option<Uuid> = None;
    let total_start = Instant::now();

    let mut batch_snapshots: Vec<chroma_index::spann::quantized_spann::StatsSnapshot> = Vec::new();
    let mut checkpoint_results: Vec<CheckpointResult> = Vec::new();

    for checkpoint_idx in 0..num_checkpoints {
        let offset = checkpoint_idx * BATCH_SIZE;
        let limit = BATCH_SIZE.min(data_len.saturating_sub(offset));

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
        batch_snapshots.push(index.stats().snapshot(&cluster_sizes));

        let flusher = index
            .commit(&blockfile_provider, &usearch_provider)
            .await
            .expect("Failed to commit");
        file_ids = Some(flusher.flush().await.expect("Failed to flush"));
        let commit_time = commit_start.elapsed();

        total_vectors += actual_count;

        // === Step 7: Evaluate recall for this checkpoint ===
        // Use actual total_vectors count (not theoretical checkpoint boundary)
        // to match ground truth queries which are computed against actual data size
        let checkpoint_queries = queries_by_checkpoint
            .get(&(total_vectors as u64))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let (
            recall_10_np16,
            recall_100_np16,
            recall_10_np32,
            recall_100_np32,
            recall_10_np64,
            recall_100_np64,
            recall_10_np128,
            recall_100_np128,
            recall_10_np256,
            recall_100_np256,
        ) = if !checkpoint_queries.is_empty() {
            // Re-open index for search (need fresh providers)
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
                config.clone(),
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

            // Evaluate with nprobe=16
            let (r10_16, r100_16) = evaluate_recall(
                Arc::clone(&search_index),
                checkpoint_queries,
                k,
                16,
                num_threads,
                checkpoint_idx,
                num_checkpoints,
            )
            .await;

            // Evaluate with nprobe=32
            let (r10_32, r100_32) = evaluate_recall(
                Arc::clone(&search_index),
                checkpoint_queries,
                k,
                32,
                num_threads,
                checkpoint_idx,
                num_checkpoints,
            )
            .await;

            // Evaluate with nprobe=64
            let (r10_64, r100_64) = evaluate_recall(
                Arc::clone(&search_index),
                checkpoint_queries,
                k,
                64,
                num_threads,
                checkpoint_idx,
                num_checkpoints,
            )
            .await;

            // Evaluate with nprobe=128
            let (r10_128, r100_128) = evaluate_recall(
                Arc::clone(&search_index),
                checkpoint_queries,
                k,
                128,
                num_threads,
                checkpoint_idx,
                num_checkpoints,
            )
            .await;

            // Evaluate with nprobe=256
            let (r10_256, r100_256) = evaluate_recall(
                search_index,
                checkpoint_queries,
                k,
                256,
                num_threads,
                checkpoint_idx,
                num_checkpoints,
            )
            .await;

            (
                r10_16, r100_16, r10_32, r100_32, r10_64, r100_64, r10_128, r100_128, r10_256,
                r100_256,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        };

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
        println!(
            "  Recall: np16={:.2}/{:.2} np32={:.2}/{:.2} np64={:.2}/{:.2} np128={:.2}/{:.2} np256={:.2}/{:.2}",
            recall_10_np16,
            recall_100_np16,
            recall_10_np32,
            recall_100_np32,
            recall_10_np64,
            recall_100_np64,
            recall_10_np128,
            recall_100_np128,
            recall_10_np256,
            recall_100_np256,
        );

        checkpoint_results.push(CheckpointResult {
            checkpoint: checkpoint_idx + 1,
            vectors: total_vectors,
            index_time,
            commit_time,
            num_queries: checkpoint_queries.len(),
            recall_10_np16,
            recall_100_np16,
            recall_10_np32,
            recall_100_np32,
            recall_10_np64,
            recall_100_np64,
            recall_10_np128,
            recall_100_np128,
            recall_10_np256,
            recall_100_np256,
        });
    }

    let total_time = total_start.elapsed();
    let overall_throughput = total_vectors as f64 / total_time.as_secs_f64();

    // Print method statistics tables
    println!(
        "{}",
        chroma_index::spann::quantized_spann::format_batch_tables(&batch_snapshots)
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
