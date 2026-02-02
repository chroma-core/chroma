//! Benchmark for FastSpannIndexWriter add throughput.

mod datasets;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_blockstore::{
    arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_distance::DistanceFunction;
use chroma_index::{
    config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
    spann::{
        fast_writer::{FastSpannIndexWriter, SpannMetrics},
        types::{GarbageCollectionContext, SpannIndexIds, SpannIndexReader},
    },
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, InternalSpannConfiguration};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};

use datasets::dbpedia::{DbPedia, DATA_LEN, DIMENSION};
use datasets::{format_count, recall_at_k, Query};

const BLOCK_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB
const BATCH_SIZE: usize = 100_000;
const NUM_BATCHES: usize = 10;
const NUM_THREADS: usize = 16;

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

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== FastSpannIndexWriter Benchmark ===");
    println!(
        "Config: batch_size={}, num_batches={}, threads={}",
        format_count(BATCH_SIZE),
        NUM_BATCHES,
        NUM_THREADS
    );
    println!(
        "Total vectors to index: {}",
        format_count(BATCH_SIZE * NUM_BATCHES)
    );
    println!();

    // Load dataset
    let dataset = DbPedia::load().await?;
    println!(
        "Dataset: {} vectors, {} dimensions",
        format_count(dataset.data_len()),
        dataset.dimension()
    );

    // Setup temp directory and storage
    let tmp_dir = tempfile::tempdir()?;
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

    let collection_id = CollectionUuid::new();

    // Load ALL vectors upfront
    let total_vectors_to_load = (BATCH_SIZE * NUM_BATCHES).min(DATA_LEN);
    println!("Loading {} vectors...", format_count(total_vectors_to_load));
    let load_all_start = Instant::now();
    let all_vectors = dataset.load_range(0, total_vectors_to_load)?;
    let load_all_time = load_all_start.elapsed();
    println!(
        "Loaded {} vectors in {}",
        format_count(all_vectors.len()),
        format_duration(load_all_time)
    );
    println!();

    // Setup InternalSpannConfiguration
    let params = InternalSpannConfiguration::default();

    // Setup GarbageCollectionContext (disabled by default)
    let gc_context = GarbageCollectionContext::try_from_config(
        &(
            PlGarbageCollectionConfig::default(),
            HnswGarbageCollectionConfig::default(),
        ),
        &Registry::default(),
    )
    .await
    .map_err(|e| format!("Failed to create GC context: {}", e))?;

    // Track IDs between batches
    let mut spann_ids: Option<SpannIndexIds> = None;
    let mut total_vectors = 0usize;
    let total_start = Instant::now();

    for batch_idx in 0..NUM_BATCHES {
        let offset = batch_idx * BATCH_SIZE;
        let limit = BATCH_SIZE.min(DATA_LEN.saturating_sub(offset));

        if limit == 0 {
            println!("Batch {}: No more data available", batch_idx);
            break;
        }

        if offset + limit > all_vectors.len() {
            println!("Batch {}: Not enough vectors loaded", batch_idx);
            break;
        }

        // Setup providers (fresh each batch to avoid cache issues)
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

        let hnsw_cache = new_non_persistent_cache_for_test();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path()),
            hnsw_cache,
            NUM_THREADS as u32,
            false,
        );

        // Open index
        let prefix_path = spann_ids
            .as_ref()
            .map(|ids| ids.prefix_path.as_str())
            .unwrap_or("");

        let index = Arc::new(
            FastSpannIndexWriter::from_id(
                &hnsw_provider,
                spann_ids.as_ref().map(|ids| &ids.hnsw_id),
                spann_ids.as_ref().map(|ids| &ids.versions_map_id),
                spann_ids.as_ref().map(|ids| &ids.pl_id),
                spann_ids.as_ref().map(|ids| &ids.max_head_id_id),
                &collection_id,
                prefix_path,
                DIMENSION,
                &blockfile_provider,
                params.clone(),
                gc_context.clone(),
                BLOCK_SIZE_BYTES,
                SpannMetrics::default(),
                None, // cmek
            )
            .await
            .expect("Failed to open index"),
        );

        // Get batch vectors from pre-loaded data
        let batch_vectors = &all_vectors[offset..offset + limit];
        let actual_count = batch_vectors.len();

        // Chunk into partitions for parallel processing
        let chunk_size = (actual_count + NUM_THREADS - 1) / NUM_THREADS;
        let chunks = batch_vectors
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect::<Vec<_>>();

        // Progress bar
        let progress = ProgressBar::new(actual_count as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "[Batch {}/{}] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                    batch_idx + 1,
                    NUM_BATCHES
                ))
                .unwrap(),
        );

        // Spawn parallel tasks
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

        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed();

        // Commit and flush this batch
        let commit_start = Instant::now();
        let index = Arc::try_unwrap(index)
            .ok()
            .expect("Index still has references");
        let flusher = index.commit().await.expect("Failed to commit");
        let new_ids = flusher.flush().await.expect("Failed to flush");
        let commit_time = commit_start.elapsed();

        // Update IDs for next batch
        spann_ids = Some(new_ids);

        total_vectors += actual_count;
        let throughput = actual_count as f64 / index_time.as_secs_f64();

        println!(
            "Batch {}: {} vectors | index {} | commit {} | {:.0} vec/s",
            batch_idx + 1,
            format_count(actual_count),
            format_duration(index_time),
            format_duration(commit_time),
            throughput
        );
    }

    let total_time = total_start.elapsed();
    let overall_throughput = total_vectors as f64 / total_time.as_secs_f64();

    println!("\n=== Indexing Summary ===");
    println!("Total vectors: {}", format_count(total_vectors));
    println!("Total time: {}", format_duration(total_time));
    println!("Overall throughput: {:.0} vec/s", overall_throughput);

    // === Recall Evaluation ===
    println!("\n=== Recall Evaluation ===");

    // Load ground truth queries
    let queries = dataset.queries(DistanceFunction::Euclidean)?;
    let k = 100;

    // Filter queries to only those whose ground truth was computed against vectors we indexed
    let valid_queries: Vec<Query> = queries
        .into_iter()
        .filter(|q| q.max_vector_id <= total_vectors as u64)
        .collect();

    println!(
        "Evaluating {} queries (k={})...",
        format_count(valid_queries.len()),
        k
    );

    if valid_queries.is_empty() {
        println!("No valid queries found for the indexed vector count.");
        println!("\nDone!");
        return Ok(());
    }

    // Get the final IDs
    let final_ids = spann_ids.expect("No IDs after indexing");

    // Setup fresh providers for search
    let block_cache = new_cache_for_test();
    let sparse_index_cache = new_cache_for_test();
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage.clone(),
        BLOCK_SIZE_BYTES,
        block_cache,
        sparse_index_cache,
        BlockManagerConfig::default_num_concurrent_block_flushes(),
    );
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    let hnsw_cache = new_non_persistent_cache_for_test();
    let hnsw_provider = HnswIndexProvider::new(
        storage.clone(),
        PathBuf::from(tmp_dir.path()),
        hnsw_cache,
        NUM_THREADS as u32,
        false,
    );

    // Open reader for search
    let reader = SpannIndexReader::from_id(
        Some(&final_ids.hnsw_id),
        &hnsw_provider,
        &collection_id,
        DistanceFunction::Euclidean,
        DIMENSION,
        params.ef_search,
        Some(&final_ids.pl_id),
        Some(&final_ids.versions_map_id),
        &blockfile_provider,
        &final_ids.prefix_path,
        true, // adaptive_search_nprobe
        params.clone(),
    )
    .await
    .expect("Failed to open reader");

    // Run parallel recall evaluation using stream with buffer_unordered
    let distance_function = DistanceFunction::Euclidean;
    let recall_start = Instant::now();

    let progress = ProgressBar::new(valid_queries.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Recall] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
            .unwrap(),
    );

    let results: Vec<(f64, f64)> = stream::iter(valid_queries.iter())
        .map(|query| {
            let reader = &reader;
            let distance_function = &distance_function;
            let progress = &progress;
            async move {
                // 1. RNG query to find candidate heads
                let (head_ids, _, _) = reader
                    .rng_query(&query.vector, total_vectors, k)
                    .await
                    .expect("RNG query failed");

                // 2. Fetch posting lists and compute distances
                let mut candidates: Vec<(u32, f32)> = Vec::new();
                let mut seen_ids = HashSet::new();

                for head_id in head_ids {
                    if let Ok(postings) = reader.fetch_posting_list(head_id as u32).await {
                        for posting in postings {
                            // Deduplicate by id
                            if seen_ids.insert(posting.doc_offset_id) {
                                let dist = distance_function
                                    .distance(&query.vector, &posting.doc_embedding);
                                candidates.push((posting.doc_offset_id, dist));
                            }
                        }
                    }
                }

                // 3. Sort by distance and truncate to top-k
                candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
                candidates.truncate(k);

                // 4. Compute recall@10 and recall@100
                let predicted: Vec<u32> = candidates.iter().map(|(id, _)| *id).collect();
                let recall_10 = recall_at_k(&predicted, &query.neighbors, 10);
                let recall_100 = recall_at_k(&predicted, &query.neighbors, 100);

                progress.inc(1);
                (recall_10, recall_100)
            }
        })
        .buffer_unordered(NUM_THREADS)
        .collect()
        .await;

    progress.finish_and_clear();
    let recall_time = recall_start.elapsed();

    let num_evaluated = results.len();
    let (total_recall_10, total_recall_100): (f64, f64) = results
        .iter()
        .fold((0.0, 0.0), |(acc10, acc100), (r10, r100)| {
            (acc10 + r10, acc100 + r100)
        });

    let avg_recall_10 = if num_evaluated > 0 {
        total_recall_10 / num_evaluated as f64
    } else {
        0.0
    };
    let avg_recall_100 = if num_evaluated > 0 {
        total_recall_100 / num_evaluated as f64
    } else {
        0.0
    };

    println!("Queries evaluated: {}", format_count(num_evaluated));
    println!(
        "Recall@10: {:.4} | Recall@100: {:.4}",
        avg_recall_10, avg_recall_100
    );
    println!("Evaluation time: {}", format_duration(recall_time));
    println!(
        "Query throughput: {:.0} qps",
        num_evaluated as f64 / recall_time.as_secs_f64()
    );

    let mut total_recall_10: f64 = 0.0;
    let mut total_recall_100: f64 = 0.0;
    let mut num_evaluated: usize = 0;

    for query in &valid_queries {
        // 1. RNG query to find candidate heads
        let (head_ids, _, _) = reader
            .rng_query(&query.vector, total_vectors, k)
            .await
            .expect("RNG query failed");

        // 2. Fetch posting lists and compute distances
        let mut candidates: Vec<(u32, f32)> = Vec::new();
        let mut seen_ids = HashSet::new();

        for head_id in head_ids {
            if let Ok(postings) = reader.fetch_posting_list(head_id as u32).await {
                for posting in postings {
                    // Deduplicate by id
                    if seen_ids.insert(posting.doc_offset_id) {
                        let dist =
                            distance_function.distance(&query.vector, &posting.doc_embedding);
                        candidates.push((posting.doc_offset_id, dist));
                    }
                }
            }
        }

        // 3. Sort by distance and truncate to top-k
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.truncate(k);

        // 4. Compute recall@10 and recall@100
        let predicted: Vec<u32> = candidates.iter().map(|(id, _)| *id).collect();
        total_recall_10 += recall_at_k(&predicted, &query.neighbors, 10);
        total_recall_100 += recall_at_k(&predicted, &query.neighbors, 100);

        num_evaluated += 1;
        progress.inc(1);
    }

    progress.finish_and_clear();
    let recall_time = recall_start.elapsed();

    let avg_recall_10 = if num_evaluated > 0 {
        total_recall_10 / num_evaluated as f64
    } else {
        0.0
    };
    let avg_recall_100 = if num_evaluated > 0 {
        total_recall_100 / num_evaluated as f64
    } else {
        0.0
    };

    println!("Queries evaluated: {}", format_count(num_evaluated));
    println!(
        "Recall@10: {:.4} | Recall@100: {:.4}",
        avg_recall_10, avg_recall_100
    );
    println!("Evaluation time: {}", format_duration(recall_time));
    println!(
        "Query throughput: {:.0} qps",
        num_evaluated as f64 / recall_time.as_secs_f64()
    );

    println!("\nDone!");

    Ok(())
}
