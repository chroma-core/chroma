//! Benchmark for MutableQuantizedSpannIndex (RaBitQ-quantized SPANN) add throughput.
//!
//! Supports two centroid index backends:
//! - `usearch` (default): HNSW-based USearch index
//! - `binary`: Binary quantized flat index with Hamming distance
//!
//! Usage:
//!   cargo bench -p chroma-index --bench quantized_spann
//!   cargo bench -p chroma-index --bench quantized_spann -- --binary
#![recursion_limit = "8192"]

mod datasets;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_blockstore::{
    arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
    BlockfileWriterOptions,
};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_distance::DistanceFunction;
use chroma_index::{
    binary_quantized::{BinaryQuantizedIndexProvider, PersistableBinaryQuantizedIndex},
    spann::quantized_spann::{MutableQuantizedSpannIndex, QuantizedSpannConfig},
    usearch::{USearchIndex, USearchIndexConfig, USearchIndexProvider},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{CollectionUuid, DataRecord};
use indicatif::{ProgressBar, ProgressStyle};

use datasets::dbpedia::{DbPedia, DATA_LEN, DIMENSION};
use datasets::{format_count, recall_at_k, Query};

const BLOCK_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB
const BATCH_SIZE: usize = 100_000;
const NUM_BATCHES: usize = 4;
const NUM_THREADS: usize = 12;

/// Centroid index backend selection
#[derive(Clone, Copy, Debug, PartialEq)]
enum Backend {
    USearch,
    Binary,
}

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

// Uncomment to verify ASAN is working - this should trigger a heap-buffer-overflow
fn trigger_asan_test() {
    let v: Vec<u8> = vec![1, 2, 3];
    let ptr = v.as_ptr();
    println!("TRIGGERING ASAN TEST");
    unsafe {
        // Read past the end of the buffer - ASAN should catch this
        let _bad = std::ptr::read_volatile(ptr.add(100));
    }
}

#[tokio::main]
async fn main() {
    // Uncomment to verify ASAN is working:
    // trigger_asan_test();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let backend = if args.iter().any(|a| a == "--binary") {
        Backend::Binary
    } else {
        Backend::USearch
    };

    let result = match backend {
        Backend::USearch => run_usearch().await,
        Backend::Binary => run_binary().await,
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run_usearch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== MutableQuantizedSpannIndex Benchmark (USearch Backend) ===");
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

    // Load ALL vectors upfront (for raw embedding blockfile and batch processing)
    let total_vectors_to_load = (BATCH_SIZE * NUM_BATCHES).min(DATA_LEN);
    println!(
        "Loading {} vectors for raw embedding blockfile...",
        format_count(total_vectors_to_load)
    );
    let load_all_start = Instant::now();
    let all_vectors = dataset.load_range(0, total_vectors_to_load)?;
    let load_all_time = load_all_start.elapsed();
    println!(
        "Loaded {} vectors in {}",
        format_count(all_vectors.len()),
        format_duration(load_all_time)
    );

    // Create raw embedding blockfile with ALL embeddings
    println!("Writing raw embeddings to blockfile...");
    let write_start = Instant::now();

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

    let raw_embedding_writer = blockfile_provider
        .write::<u32, &DataRecord<'_>>(BlockfileWriterOptions::new("".to_string()))
        .await
        .expect("Failed to create raw embedding writer");

    for (id, embedding) in &all_vectors {
        let record = DataRecord {
            id: "",
            embedding: &embedding,
            metadata: None,
            document: None,
        };
        raw_embedding_writer
            .set("", *id, &record)
            .await
            .expect("Failed to write embedding");
    }

    let raw_flusher = raw_embedding_writer
        .commit::<u32, &DataRecord<'_>>()
        .await
        .expect("Failed to commit raw embeddings");
    let raw_embedding_id = raw_flusher.id();
    raw_flusher
        .flush::<u32, &DataRecord<'_>>()
        .await
        .expect("Failed to flush raw embeddings");

    let write_time = write_start.elapsed();
    println!(
        "Wrote {} raw embeddings in {}",
        format_count(all_vectors.len()),
        format_duration(write_time)
    );
    println!();

    // Setup configs
    let mut spann_config = QuantizedSpannConfig {
        cmek: None,
        prefix_path: "".to_string(),
        dimensions: DIMENSION,
        distance_function: DistanceFunction::Cosine,
        spann_center_drift_threshold: 0.125,
        spann_merge_threshold: 128,
        spann_nprobe: 64,
        spann_reassign_neighbor_count: 4,
        spann_replica_count: 4,
        spann_rng_epsilon: 8.0,
        spann_rng_factor: 1.0,
        spann_split_threshold: 512,
        spann_binary_oversample: 1, // Not used for USearch backend
        embedding_metadata_id: None,
        quantized_centroid_id: None,
        quantized_cluster_id: None,
        raw_centroid_id: None,
        raw_embedding_id: Some(raw_embedding_id),
        scalar_metadata_id: None,
    };

    let usearch_config = USearchIndexConfig {
        collection_id,
        cmek: None,
        prefix_path: "".to_string(),
        dimensions: DIMENSION,
        distance_function: DistanceFunction::Cosine,
        connectivity: 16,
        expansion_add: 128,
        expansion_search: 64,
        quantization_center: None,
    };

    // Run batches
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

        let usearch_cache = new_non_persistent_cache_for_test();
        let usearch_provider = USearchIndexProvider::new(storage.clone(), usearch_cache);

        // Open index
        let index = Arc::new(
            MutableQuantizedSpannIndex::<USearchIndex>::open(
                spann_config.clone(),
                usearch_config.clone(),
                &blockfile_provider,
                &usearch_provider,
            )
            .await
            .expect("Failed to open index"),
        );

        println!("Index opened");

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

        println!("Chunks created");

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

        println!("Tasks spawned");

        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed();

        println!("Tasks completed");

        // Commit and flush this batch
        let commit_start = Instant::now();
        let index = Arc::try_unwrap(index)
            .ok()
            .expect("Index still has references");
        let flusher = index
            .commit(&blockfile_provider, &usearch_provider, &usearch_config)
            .await
            .expect("Failed to commit");
        let new_config = flusher.flush().await.expect("Failed to flush");
        let commit_time = commit_start.elapsed();

        println!("Index committed and flushed");

        // Update config for next batch (preserve raw_embedding_id)
        spann_config = QuantizedSpannConfig {
            raw_embedding_id: Some(raw_embedding_id),
            ..new_config
        };

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

    // // === Recall Evaluation ===
    // println!("\n=== Recall Evaluation ===");

    // // Load ground truth queries
    // let queries = dataset.queries(DistanceFunction::Euclidean)?;
    // let k = 100;

    // // Filter queries to only those whose ground truth was computed against vectors we indexed
    // let valid_queries: Vec<Query> = queries
    //     .into_iter()
    //     .filter(|q| q.max_vector_id <= total_vectors as u64)
    //     .collect();

    // println!(
    //     "Evaluating {} queries (k={})...",
    //     format_count(valid_queries.len()),
    //     k
    // );

    // if valid_queries.is_empty() {
    //     println!("No valid queries found for the indexed vector count.");
    //     println!("\nDone!");
    //     return Ok(());
    // }

    // // Setup fresh providers for search
    // let block_cache = new_cache_for_test();
    // let sparse_index_cache = new_cache_for_test();
    // let arrow_blockfile_provider = ArrowBlockfileProvider::new(
    //     storage.clone(),
    //     BLOCK_SIZE_BYTES,
    //     block_cache,
    //     sparse_index_cache,
    //     BlockManagerConfig::default_num_concurrent_block_flushes(),
    // );
    // let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    // let usearch_cache = new_non_persistent_cache_for_test();
    // let usearch_provider = USearchIndexProvider::new(storage.clone(), usearch_cache);

    // // Open final index for search
    // let index = Arc::new(
    //     MutableQuantizedSpannIndex::<USearchIndex>::open(
    //         spann_config.clone(),
    //         usearch_config.clone(),
    //         &blockfile_provider,
    //         &usearch_provider,
    //     )
    //     .await
    //     .expect("Failed to open index for search"),
    // );

    // // Helper to run parallel recall evaluation (returns recall@10 and recall@100)
    // async fn evaluate_recall(
    //     index: &Arc<MutableQuantizedSpannIndex<USearchIndex>>,
    //     queries: &[Query],
    //     k: usize,
    //     use_quantized_centroid: bool,
    //     num_threads: usize,
    // ) -> Result<(f64, f64, usize, Duration), Box<dyn std::error::Error + Send + Sync>> {
    //     let recall_start = Instant::now();
    //     let total_recall_10 = Arc::new(AtomicUsize::new(0));
    //     let total_recall_100 = Arc::new(AtomicUsize::new(0));
    //     let num_evaluated = Arc::new(AtomicUsize::new(0));

    //     let chunk_size = (queries.len() + num_threads - 1) / num_threads;
    //     let query_chunks: Vec<Vec<Query>> =
    //         queries.chunks(chunk_size).map(|c| c.to_vec()).collect();

    //     let label = if use_quantized_centroid {
    //         "Recall (quantized)"
    //     } else {
    //         "Recall (raw)"
    //     };
    //     let progress = ProgressBar::new(queries.len() as u64);
    //     progress.set_style(
    //         ProgressStyle::default_bar()
    //             .template(&format!(
    //                 "[{}] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
    //                 label
    //             ))
    //             .unwrap(),
    //     );

    //     let handles: Vec<_> = query_chunks
    //         .into_iter()
    //         .map(|chunk| {
    //             let index = Arc::clone(index);
    //             let total_recall_10 = Arc::clone(&total_recall_10);
    //             let total_recall_100 = Arc::clone(&total_recall_100);
    //             let num_evaluated = Arc::clone(&num_evaluated);
    //             let progress = progress.clone();
    //             tokio::spawn(async move {
    //                 let mut local_recall_10_sum: f64 = 0.0;
    //                 let mut local_recall_100_sum: f64 = 0.0;
    //                 let mut local_count: usize = 0;

    //                 for query in chunk {
    //                     let results = index
    //                         .search(&query.vector, k, use_quantized_centroid)
    //                         .await
    //                         .expect("Search failed");
    //                     let predicted = results.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    //                     local_recall_10_sum += recall_at_k(&predicted, &query.neighbors, 10);
    //                     local_recall_100_sum += recall_at_k(&predicted, &query.neighbors, 100);
    //                     local_count += 1;
    //                     progress.inc(1);
    //                 }

    //                 total_recall_10.fetch_add(
    //                     (local_recall_10_sum * 1_000_000.0) as usize,
    //                     Ordering::Relaxed,
    //                 );
    //                 total_recall_100.fetch_add(
    //                     (local_recall_100_sum * 1_000_000.0) as usize,
    //                     Ordering::Relaxed,
    //                 );
    //                 num_evaluated.fetch_add(local_count, Ordering::Relaxed);
    //             })
    //         })
    //         .collect();

    //     for handle in handles {
    //         handle.await?;
    //     }
    //     progress.finish_and_clear();

    //     let recall_time = recall_start.elapsed();
    //     let total_recall_10_value = total_recall_10.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    //     let total_recall_100_value = total_recall_100.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    //     let num_queries = num_evaluated.load(Ordering::Relaxed);
    //     let avg_recall_10 = if num_queries > 0 {
    //         total_recall_10_value / num_queries as f64
    //     } else {
    //         0.0
    //     };
    //     let avg_recall_100 = if num_queries > 0 {
    //         total_recall_100_value / num_queries as f64
    //     } else {
    //         0.0
    //     };

    //     Ok((avg_recall_10, avg_recall_100, num_queries, recall_time))
    // }

    // // Test with RAW centroid navigation
    // println!("\n--- Raw centroid navigation ---");
    // let (avg_recall_10, avg_recall_100, num_queries, recall_time) =
    //     evaluate_recall(&index, &valid_queries, k, false, NUM_THREADS).await?;
    // println!("Queries evaluated: {}", format_count(num_queries));
    // println!(
    //     "Recall@10: {:.4} | Recall@100: {:.4}",
    //     avg_recall_10, avg_recall_100
    // );
    // println!("Evaluation time: {}", format_duration(recall_time));
    // println!(
    //     "Query throughput: {:.0} qps",
    //     num_queries as f64 / recall_time.as_secs_f64()
    // );

    // // Test with QUANTIZED centroid navigation
    // println!("\n--- Quantized centroid navigation ---");
    // let (avg_recall_10, avg_recall_100, num_queries, recall_time) =
    //     evaluate_recall(&index, &valid_queries, k, true, NUM_THREADS).await?;
    // println!("Queries evaluated: {}", format_count(num_queries));
    // println!(
    //     "Recall@10: {:.4} | Recall@100: {:.4}",
    //     avg_recall_10, avg_recall_100
    // );
    // println!("Evaluation time: {}", format_duration(recall_time));
    // println!(
    //     "Query throughput: {:.0} qps",
    //     num_queries as f64 / recall_time.as_secs_f64()
    // );

    println!("\nDone!");

    Ok(())
}

/// Run benchmark with binary quantized centroid index
async fn run_binary() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== MutableQuantizedSpannIndex Benchmark (Binary Quantized Backend) ===");
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

    // Load ALL vectors upfront (for raw embedding blockfile and batch processing)
    let total_vectors_to_load = (BATCH_SIZE * NUM_BATCHES).min(DATA_LEN);
    println!(
        "Loading {} vectors for raw embedding blockfile...",
        format_count(total_vectors_to_load)
    );
    let load_all_start = Instant::now();
    let all_vectors = dataset.load_range(0, total_vectors_to_load)?;
    let load_all_time = load_all_start.elapsed();
    println!(
        "Loaded {} vectors in {}",
        format_count(all_vectors.len()),
        format_duration(load_all_time)
    );

    // Create raw embedding blockfile with ALL embeddings
    println!("Writing raw embeddings to blockfile...");
    let write_start = Instant::now();

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

    let raw_embedding_writer = blockfile_provider
        .write::<u32, &DataRecord<'_>>(BlockfileWriterOptions::new("".to_string()))
        .await
        .expect("Failed to create raw embedding writer");

    for (id, embedding) in &all_vectors {
        let record = DataRecord {
            id: "",
            embedding: &embedding,
            metadata: None,
            document: None,
        };
        raw_embedding_writer
            .set("", *id, &record)
            .await
            .expect("Failed to write embedding");
    }

    let raw_flusher = raw_embedding_writer
        .commit::<u32, &DataRecord<'_>>()
        .await
        .expect("Failed to commit raw embeddings");
    let raw_embedding_id = raw_flusher.id();
    raw_flusher
        .flush::<u32, &DataRecord<'_>>()
        .await
        .expect("Failed to flush raw embeddings");

    let write_time = write_start.elapsed();
    println!(
        "Wrote {} raw embeddings in {}",
        format_count(all_vectors.len()),
        format_duration(write_time)
    );
    println!();

    // Setup configs
    // Use 10x oversample for better cluster assignment during construction
    const BINARY_OVERSAMPLE: usize = 2;
    println!("Binary quantized oversample factor: {}x", BINARY_OVERSAMPLE);

    let mut spann_config = QuantizedSpannConfig {
        cmek: None,
        prefix_path: "".to_string(),
        dimensions: DIMENSION,
        distance_function: DistanceFunction::Cosine,
        spann_center_drift_threshold: 0.125,
        spann_merge_threshold: 128,
        spann_nprobe: 64,
        spann_reassign_neighbor_count: 4,
        spann_replica_count: 4,
        spann_rng_epsilon: 8.0,
        spann_rng_factor: 1.0,
        spann_split_threshold: 512,
        spann_binary_oversample: BINARY_OVERSAMPLE,
        embedding_metadata_id: None,
        quantized_centroid_id: None,
        quantized_cluster_id: None,
        raw_centroid_id: None,
        raw_embedding_id: Some(raw_embedding_id),
        scalar_metadata_id: None,
    };

    // Run batches
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

        let binary_cache = new_non_persistent_cache_for_test();
        let binary_provider = BinaryQuantizedIndexProvider::new(storage.clone(), binary_cache);

        // Open index using binary quantized backend
        let index = Arc::new(
            MutableQuantizedSpannIndex::<PersistableBinaryQuantizedIndex>::open_binary(
                spann_config.clone(),
                &blockfile_provider,
                &binary_provider,
            )
            .await
            .expect("Failed to open index"),
        );

        println!("Index opened (binary quantized backend)");

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

        println!("Chunks created");

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

        println!("Tasks spawned");

        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed();

        println!("Tasks completed");

        // Commit and flush this batch
        let commit_start = Instant::now();
        let index = Arc::try_unwrap(index)
            .ok()
            .expect("Index still has references");
        let flusher = index
            .commit_binary(&blockfile_provider, &binary_provider)
            .await
            .expect("Failed to commit");
        let new_config = flusher.flush().await.expect("Failed to flush");
        let commit_time = commit_start.elapsed();

        println!("Index committed and flushed");

        // Update config for next batch (preserve raw_embedding_id)
        spann_config = QuantizedSpannConfig {
            raw_embedding_id: Some(raw_embedding_id),
            ..new_config
        };

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

    println!("\n=== Indexing Summary (Binary Quantized Backend) ===");
    println!("Total vectors: {}", format_count(total_vectors));
    println!("Total time: {}", format_duration(total_time));
    println!("Overall throughput: {:.0} vec/s", overall_throughput);

    // ========== RECALL EVALUATION ==========
    println!("\n=== Recall Evaluation ===");

    // Load queries with ground truth
    let queries = dataset.queries(DistanceFunction::Cosine)?;
    println!(
        "Loaded {} queries from ground truth",
        format_count(queries.len())
    );

    // Filter ground truth neighbors to only those within our indexed range
    // This allows us to measure recall even when ground truth was computed against more vectors
    let max_id = total_vectors as u32;
    let valid_queries: Vec<Query> = queries
        .into_iter()
        .map(|q| {
            let filtered_neighbors: Vec<u32> =
                q.neighbors.into_iter().filter(|&id| id < max_id).collect();
            Query {
                vector: q.vector,
                neighbors: filtered_neighbors,
                max_vector_id: q.max_vector_id,
            }
        })
        .filter(|q| !q.neighbors.is_empty()) // Only keep queries with at least one valid neighbor
        .collect();

    println!(
        "Queries with valid neighbors in index: {}",
        format_count(valid_queries.len())
    );

    if valid_queries.is_empty() {
        println!("No valid queries with neighbors in the indexed range");
        println!("\nDone!");
        return Ok(());
    }

    // Reopen index for search
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

    let binary_cache = new_non_persistent_cache_for_test();
    let binary_provider = BinaryQuantizedIndexProvider::new(storage.clone(), binary_cache);

    let index = Arc::new(
        MutableQuantizedSpannIndex::<PersistableBinaryQuantizedIndex>::open_binary(
            spann_config.clone(),
            &blockfile_provider,
            &binary_provider,
        )
        .await
        .expect("Failed to open index for search"),
    );

    let k = dataset.k(); // typically 100

    // Run recall evaluation with parallel workers
    let recall_start = Instant::now();
    let total_recall_10 = Arc::new(AtomicUsize::new(0));
    let total_recall_100 = Arc::new(AtomicUsize::new(0));
    let num_evaluated = Arc::new(AtomicUsize::new(0));
    let total_latency_ns = Arc::new(AtomicUsize::new(0));
    let min_latency_ns = Arc::new(AtomicUsize::new(usize::MAX));
    let max_latency_ns = Arc::new(AtomicUsize::new(0));

    let chunk_size = (valid_queries.len() + NUM_THREADS - 1) / NUM_THREADS;
    let query_chunks: Vec<Vec<Query>> = valid_queries
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let progress = ProgressBar::new(valid_queries.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Recall eval] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
            .unwrap(),
    );

    let handles: Vec<_> = query_chunks
        .into_iter()
        .map(|chunk| {
            let index = Arc::clone(&index);
            let total_recall_10 = Arc::clone(&total_recall_10);
            let total_recall_100 = Arc::clone(&total_recall_100);
            let num_evaluated = Arc::clone(&num_evaluated);
            let total_latency_ns = Arc::clone(&total_latency_ns);
            let min_latency_ns = Arc::clone(&min_latency_ns);
            let max_latency_ns = Arc::clone(&max_latency_ns);
            let progress = progress.clone();
            tokio::spawn(async move {
                let mut local_recall_10_sum: f64 = 0.0;
                let mut local_recall_100_sum: f64 = 0.0;
                let mut local_count: usize = 0;
                let mut local_latency_ns: u128 = 0;
                let mut local_min_ns: u128 = u128::MAX;
                let mut local_max_ns: u128 = 0;

                for query in chunk {
                    let query_start = Instant::now();
                    // Use quantized centroid navigation (true) for binary quantized backend
                    let results = index
                        .search(&query.vector, k, true)
                        .await
                        .expect("Search failed");
                    let elapsed_ns = query_start.elapsed().as_nanos();

                    local_latency_ns += elapsed_ns;
                    local_min_ns = local_min_ns.min(elapsed_ns);
                    local_max_ns = local_max_ns.max(elapsed_ns);

                    let predicted: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
                    local_recall_10_sum += recall_at_k(&predicted, &query.neighbors, 10);
                    local_recall_100_sum += recall_at_k(&predicted, &query.neighbors, 100);
                    local_count += 1;
                    progress.inc(1);
                }

                // Aggregate results (use fixed-point for recall)
                total_recall_10.fetch_add(
                    (local_recall_10_sum * 1_000_000.0) as usize,
                    Ordering::Relaxed,
                );
                total_recall_100.fetch_add(
                    (local_recall_100_sum * 1_000_000.0) as usize,
                    Ordering::Relaxed,
                );
                num_evaluated.fetch_add(local_count, Ordering::Relaxed);
                total_latency_ns.fetch_add(local_latency_ns as usize, Ordering::Relaxed);

                // Update min/max atomically
                let mut current_min = min_latency_ns.load(Ordering::Relaxed);
                while local_min_ns < current_min as u128 {
                    match min_latency_ns.compare_exchange_weak(
                        current_min,
                        local_min_ns as usize,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(x) => current_min = x,
                    }
                }

                let mut current_max = max_latency_ns.load(Ordering::Relaxed);
                while local_max_ns > current_max as u128 {
                    match max_latency_ns.compare_exchange_weak(
                        current_max,
                        local_max_ns as usize,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(x) => current_max = x,
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await?;
    }
    progress.finish_and_clear();

    let recall_time = recall_start.elapsed();
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

    // Latency stats
    let avg_latency = Duration::from_nanos(
        (total_latency_ns.load(Ordering::Relaxed) / num_queries.max(1)) as u64,
    );
    let min_latency = Duration::from_nanos(min_latency_ns.load(Ordering::Relaxed) as u64);
    let max_latency = Duration::from_nanos(max_latency_ns.load(Ordering::Relaxed) as u64);

    println!("Queries evaluated: {}", format_count(num_queries));
    println!(
        "Recall@10: {:.2}% | Recall@100: {:.2}%",
        avg_recall_10 * 100.0,
        avg_recall_100 * 100.0
    );
    println!("Evaluation time: {}", format_duration(recall_time));
    println!(
        "Query throughput: {:.0} qps",
        num_queries as f64 / recall_time.as_secs_f64()
    );
    println!(
        "Latency: min={} avg={} max={}",
        format_duration(min_latency),
        format_duration(avg_latency),
        format_duration(max_latency)
    );

    println!("\nDone!");

    Ok(())
}
