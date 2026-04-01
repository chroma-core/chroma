//! Sparse Index Block-Max WAND Benchmark
//!
//! This benchmark evaluates the performance of the Block-Max WAND algorithm
//! for sparse vector search compared to brute force baseline using the
//! Wikipedia SPLADE dataset from HuggingFace.
//!
//! ## Usage Modes
//!
//! ### Full Benchmark Mode (default)
//! Compares WAND performance against brute force ground truth:
//! ```bash
//! cargo run --release --example sparse_vector_benchmark -- \
//!   -n 65536 \  # number of documents
//!   -m 200 \    # number of queries
//!   -k 128      # top-k results
//! ```
//!
//! ### With Filtering
//! Test WAND with a filter that excludes 30% of documents:
//! ```bash
//! cargo run --release --example sparse_vector_benchmark -- \
//!   -n 65536 \
//!   -m 200 \
//!   -k 128 \
//!   -f 30       # exclude 30% of documents
//! ```
//!
//! ### WAND-Only Mode (for profiling)
//! Runs only WAND without brute force comparison, useful for flamegraph profiling:
//! ```bash
//! cargo run --release --example sparse_vector_benchmark -- \
//!   --wand-only \
//!   -i 100  # run each query 100 times for better profiling
//! ```
//!
//! ### Flamegraph Profiling Example
//! ```bash
//! # Install flamegraph tools if needed
//! cargo install flamegraph
//!
//! # Run with profiling
//! cargo flamegraph --example sparse_vector_benchmark -- \
//!   --wand-only \
//!   -n 10000 \
//!   -m 50 \
//!   -i 100
//! ```
//!
//! ## Options
//! - `--sort-by-url`: Sort documents by URL for better cache locality
//! - `--wand-only`: Skip brute force comparison for profiling
//! - `-i, --iterations`: Number of iterations per query (for profiling)
//! - `-f, --filter-percentage`: Randomly exclude a percentage of documents (0-100) to test filtering

use chroma_benchmark::datasets::wikipedia_splade::{SparseDocument, SparseQuery, WikipediaSplade};
#[allow(unused_imports)]
use sprs;
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::{
    maxscore::{
        BlockSparseReader, BlockSparseWriter, SparsePostingBlock, SPARSE_POSTING_BLOCK_SIZE_BYTES,
    },
    reader::{Score, SparseReader},
    writer::SparseWriter,
};
use chroma_types::SignedRoaringBitmap;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;
use tempfile::TempDir;
use uuid::Uuid;

// Blockfile prefix constants
const SPARSE_MAX_PREFIX: &str = "sparse_max";
const SPARSE_OFFSET_VALUE_PREFIX: &str = "sparse_offset_value";

/// Command line arguments for the benchmark
#[derive(Parser, Debug)]
#[command(name = "sparse_vector_benchmark")]
#[command(about = "Benchmark sparse index with Block-Max WAND algorithm")]
struct Args {
    /// Number of documents to load
    #[arg(short = 'n', long, default_value_t = 65536)]
    num_documents: usize,

    /// Number of queries to run
    #[arg(short = 'm', long, default_value_t = 256)]
    num_queries: usize,

    /// Top-k results to retrieve
    #[arg(short = 'k', long, default_value_t = 128)]
    top_k: usize,

    /// Block size for the sparse index
    #[arg(short = 'b', long, default_value_t = 128)]
    block_size: u32,

    /// Sort documents by URL for better cache locality
    #[arg(short = 's', long)]
    sort_by_url: bool,

    /// Skip brute force comparison (WAND only mode for profiling)
    #[arg(short = 'w', long)]
    wand_only: bool,

    /// Number of iterations to run each query (for profiling)
    #[arg(short = 'i', long, default_value_t = 1)]
    iterations: usize,

    /// Filter percentage: randomly exclude this percentage of documents (0-100)
    #[arg(short = 'f', long, default_value_t = 0)]
    filter_percentage: u32,

    /// Use BlockMaxMaxScore instead of WAND
    #[arg(long)]
    block_maxscore: bool,

    /// Max query terms: truncate each query to at most this many non-zero entries
    #[arg(long)]
    max_terms: Option<usize>,

    /// Sweep query terms: run both algorithms at 5,10,15,...,max_terms and print a table
    #[arg(long)]
    sweep_terms: bool,

    /// Batch size for commit/flush during indexing (default: 65536)
    #[arg(long, default_value_t = 65536)]
    batch_size: usize,
}

fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                total += dir_size_bytes(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

fn human_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn truncate_queries(queries: &[SparseQuery], max_terms: usize) -> Vec<SparseQuery> {
    queries
        .iter()
        .map(|q| {
            let nnz = q.sparse_vector.nnz();
            if nnz <= max_terms {
                return q.clone();
            }
            let mut pairs: Vec<(usize, f32)> = q
                .sparse_vector
                .iter()
                .map(|(idx, &val)| (idx, val))
                .collect();
            pairs.sort_by(|a, b| b.1.total_cmp(&a.1));
            pairs.truncate(max_terms);
            pairs.sort_by_key(|(idx, _)| *idx);
            let (indices, values): (Vec<usize>, Vec<f32>) = pairs.into_iter().unzip();
            let sv = sprs::CsVec::new(q.sparse_vector.dim(), indices, values);
            SparseQuery {
                query_id: q.query_id.clone(),
                text: q.text.clone(),
                sparse_vector: sv,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct SearchResult {
    query_id: String,
    top_k_offsets: Vec<u32>,
    scores: Vec<f32>,
    search_time_ms: f64,
}

fn brute_force_search(
    documents: &[SparseDocument],
    query: &SparseQuery,
    top_k: usize,
    mask: &SignedRoaringBitmap,
) -> (SearchResult, usize) {
    let start = Instant::now();

    // Use a min-heap to maintain top-k results efficiently (same as WAND implementation)
    let mut top_scores = BinaryHeap::<Score>::with_capacity(top_k);
    let mut non_trivial_count = 0;

    for (offset, doc) in documents.iter().enumerate() {
        // Skip documents that are filtered out
        if !mask.contains(offset as u32) {
            continue;
        }

        // Use sprs dot product directly
        let score = query.sparse_vector.dot(&doc.sparse_vector);
        if score > 0.0 {
            non_trivial_count += 1;

            if top_scores.len() < top_k {
                top_scores.push(Score {
                    offset: offset as u32,
                    score,
                });
            } else if let Some(min_entry) = top_scores.peek() {
                if score > min_entry.score {
                    top_scores.pop();
                    top_scores.push(Score {
                        offset: offset as u32,
                        score,
                    });
                }
            }
        }
    }

    // Extract results from heap and sort by score descending
    let mut scores: Vec<Score> = top_scores.into_sorted_vec();
    scores.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap()
            .then(a.offset.cmp(&b.offset))
    });

    let elapsed = start.elapsed();

    let result = SearchResult {
        query_id: query.query_id.clone(),
        top_k_offsets: scores.iter().map(|s| s.offset).collect(),
        scores: scores.iter().map(|s| s.score).collect(),
        search_time_ms: elapsed.as_secs_f64() * 1000.0,
    };

    (result, non_trivial_count)
}

async fn build_sparse_index(
    documents: &[SparseDocument],
    block_size: u32,
    sort_by_url: bool,
    batch_size_override: usize,
) -> anyhow::Result<(TempDir, BlockfileProvider, Uuid, Uuid)> {
    println!("🏗️ Building sparse index...");
    let start = Instant::now();

    // Sort documents by URL if requested for better cache locality
    let mut sorted_documents = documents.to_vec();
    if sort_by_url {
        println!("🔗 Sorting documents by URL for better cache locality...");
        sorted_documents.sort_by(|a, b| a.url.cmp(&b.url));
    }

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    let batch_size = batch_size_override;
    let num_chunks = sorted_documents.len().div_ceil(batch_size);

    let pb = ProgressBar::new(sorted_documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} docs ({eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb.set_message("Building index");

    let mut max_writer_id = None;
    let mut offset_value_writer_id = None;

    for (chunk_idx, chunk) in sorted_documents.chunks(batch_size).enumerate() {
        // Create writer options, forking offset_value from previous commit
        let max_writer_options =
            BlockfileWriterOptions::new(SPARSE_MAX_PREFIX.to_string()).ordered_mutations();
        let mut offset_value_writer_options =
            BlockfileWriterOptions::new(SPARSE_OFFSET_VALUE_PREFIX.to_string()).ordered_mutations();

        if let Some(id) = offset_value_writer_id {
            offset_value_writer_options = offset_value_writer_options.fork(id);
        }

        // Create writers for this chunk
        let max_writer = provider
            .write::<u32, f32>(max_writer_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create max writer: {:?}", e))?;

        let offset_value_writer = provider
            .write::<u32, f32>(offset_value_writer_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create offset value writer: {:?}", e))?;

        // Create reader for existing data (if we have previous committed data)
        let sparse_reader = if let (Some(max_id), Some(offset_value_id)) =
            (max_writer_id, offset_value_writer_id)
        {
            let max_reader = provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    max_id,
                    SPARSE_MAX_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create max reader: {:?}", e))?;

            let offset_value_reader = provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    offset_value_id,
                    SPARSE_OFFSET_VALUE_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create offset value reader: {:?}", e))?;

            Some(SparseReader::new(max_reader, offset_value_reader))
        } else {
            None
        };

        // Create sparse writer for this chunk
        let sparse_writer = SparseWriter::new(
            block_size,
            max_writer.clone(),
            offset_value_writer.clone(),
            sparse_reader,
        );

        // Write documents in this chunk (parallel across CPU cores)
        let num_partitions = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let partition_size = chunk.len().div_ceil(num_partitions);
        let handles = chunk
            .chunks(partition_size)
            .enumerate()
            .map(|(part_idx, partition)| {
                let base_offset = (chunk_idx * batch_size + part_idx * partition_size) as u32;
                let writer = sparse_writer.clone();
                let pb = pb.clone();
                let docs = partition
                    .iter()
                    .enumerate()
                    .map(|(idx, doc)| {
                        let pairs = doc
                            .sparse_vector
                            .indices()
                            .iter()
                            .zip(doc.sparse_vector.data().iter())
                            .map(|(idx, val)| (*idx as u32, *val))
                            .collect::<Vec<_>>();
                        (base_offset + idx as u32, pairs)
                    })
                    .collect::<Vec<_>>();
                tokio::spawn(async move {
                    for (offset, pairs) in docs {
                        writer.set(offset, pairs).await;
                        pb.inc(1);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.unwrap();
        }

        // Commit
        let flusher = Box::pin(sparse_writer.commit())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit sparse writer: {:?}", e))?;

        // Flush
        Box::pin(flusher.flush())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to flush sparse writer: {:?}", e))?;

        // Store the writer IDs for forking in next iteration (after commit/flush)
        max_writer_id = Some(max_writer.id());
        offset_value_writer_id = Some(offset_value_writer.id());

        // Clear cache after each chunk
        provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear cache: {:?}", e))?;
    }

    pb.finish_with_message("✅ Index built");

    let elapsed = start.elapsed();
    let storage_bytes = dir_size_bytes(temp_dir.path());
    println!("⏱️ Index build time: {:.2} s", elapsed.as_secs_f64());
    println!("  Documents indexed: {}", sorted_documents.len());
    println!("  Chunks processed: {num_chunks}");
    println!("  Documents per chunk: {batch_size}");
    println!("  Storage size: {} ({storage_bytes} bytes)", human_bytes(storage_bytes));

    Ok((
        temp_dir,
        provider,
        max_writer_id.expect("Should have created at least one max writer"),
        offset_value_writer_id.expect("Should have created at least one offset value writer"),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn search_with_wand(
    provider: &BlockfileProvider,
    max_reader_id: Uuid,
    offset_value_reader_id: Uuid,
    queries: &[SparseQuery],
    top_k: usize,
    mask: SignedRoaringBitmap,
    iterations: usize,
    show_progress: bool,
) -> anyhow::Result<Vec<SearchResult>> {
    if show_progress {
        println!("\n⚡ Searching with Block-Max WAND...");
    }

    // Open readers for the sparse index using the writer IDs
    let max_reader = provider
        .read::<u32, f32>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                max_reader_id,
                SPARSE_MAX_PREFIX.to_string(),
            ),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open max reader: {:?}", e))?;

    let offset_value_reader = provider
        .read::<u32, f32>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                offset_value_reader_id,
                SPARSE_OFFSET_VALUE_PREFIX.to_string(),
            ),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open offset value reader: {:?}", e))?;

    let sparse_reader = SparseReader::new(max_reader, offset_value_reader);

    let mut results = Vec::new();

    let pb = if show_progress {
        let pb = ProgressBar::new((queries.len() * iterations) as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message(if iterations > 1 {
            format!("WAND search ({iterations} iterations)")
        } else {
            "WAND search".to_string()
        });
        Some(pb)
    } else {
        None
    };

    for query in queries {
        let mut total_time_ms = 0.0;
        let mut last_scores = Vec::new();
        let mut last_offsets = Vec::new();

        for _ in 0..iterations {
            let start = Instant::now();

            // Convert CsVec to Vec of (dimension_id, value)
            let query_vec: Vec<(u32, f32)> = query
                .sparse_vector
                .indices()
                .iter()
                .zip(query.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val))
                .collect();

            // Run WAND search with the provided mask
            let scores = sparse_reader
                .wand(query_vec, top_k as u32, mask.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to run WAND search: {:?}", e))?;

            let elapsed = start.elapsed();
            total_time_ms += elapsed.as_secs_f64() * 1000.0;

            // Store results from last iteration
            last_offsets = scores.iter().map(|s| s.offset).collect();
            last_scores = scores.iter().map(|s| s.score).collect();

            if let Some(ref pb) = pb {
                pb.inc(1);
            }
        }

        results.push(SearchResult {
            query_id: query.query_id.clone(),
            top_k_offsets: last_offsets,
            scores: last_scores,
            search_time_ms: total_time_ms / iterations as f64, // Average time per query
        });
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ WAND search complete");
    }
    Ok(results)
}

// ── BlockMaxMaxScore index build + search ───────────────────────────

const BLOCK_MAXSCORE_PREFIX: &str = "block_maxscore";

async fn build_block_maxscore_index(
    documents: &[SparseDocument],
    sort_by_url: bool,
    batch_size_override: usize,
) -> anyhow::Result<(TempDir, BlockfileProvider, Uuid)> {
    println!("🏗️ Building BlockMaxMaxScore index...");
    let start = Instant::now();

    let mut sorted_documents = documents.to_vec();
    if sort_by_url {
        println!("🔗 Sorting documents by URL for better cache locality...");
        sorted_documents.sort_by(|a, b| a.url.cmp(&b.url));
    }

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    let batch_size = batch_size_override;
    let num_chunks = sorted_documents.len().div_ceil(batch_size);

    let pb = ProgressBar::new(sorted_documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} docs ({eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb.set_message("Building BlockMaxMaxScore index");

    let mut posting_writer_id = None;

    for (chunk_idx, chunk) in sorted_documents.chunks(batch_size).enumerate() {
        let mut posting_options = BlockfileWriterOptions::new(BLOCK_MAXSCORE_PREFIX.to_string())
            .ordered_mutations()
            .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES);
        if let Some(id) = posting_writer_id {
            posting_options = posting_options.fork(id);
        }

        let posting_writer = provider
            .write::<u32, SparsePostingBlock>(posting_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create posting writer: {:?}", e))?;

        let old_reader = if let Some(id) = posting_writer_id {
            let posting_reader = provider
                .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
                    id,
                    BLOCK_MAXSCORE_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create posting reader: {:?}", e))?;
            Some(BlockSparseReader::new(posting_reader))
        } else {
            None
        };

        let writer = BlockSparseWriter::new(posting_writer.clone(), old_reader);

        let num_partitions = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let partition_size = chunk.len().div_ceil(num_partitions);
        let handles = chunk
            .chunks(partition_size)
            .enumerate()
            .map(|(part_idx, partition)| {
                let base_offset = (chunk_idx * batch_size + part_idx * partition_size) as u32;
                let w = writer.clone();
                let pb = pb.clone();
                let docs: Vec<_> = partition
                    .iter()
                    .enumerate()
                    .map(|(idx, doc)| {
                        let pairs: Vec<(u32, f32)> = doc
                            .sparse_vector
                            .indices()
                            .iter()
                            .zip(doc.sparse_vector.data().iter())
                            .map(|(idx, val)| (*idx as u32, *val))
                            .collect();
                        (base_offset + idx as u32, pairs)
                    })
                    .collect();
                tokio::spawn(async move {
                    for (offset, pairs) in docs {
                        w.set(offset, pairs).await;
                        pb.inc(1);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.unwrap();
        }

        let flusher = writer
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit: {:?}", e))?;

        let size_before_flush = dir_size_bytes(temp_dir.path());
        flusher
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to flush: {:?}", e))?;
        let size_after_flush = dir_size_bytes(temp_dir.path());

        posting_writer_id = Some(posting_writer.id());

        provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear cache: {:?}", e))?;

        // Report the delta of the last flush as the true index size,
        // since forks write a complete new copy of all blocks.
        if chunk_idx == num_chunks - 1 {
            let storage_bytes = size_after_flush - size_before_flush;
            println!("⏱️ Index build time: {:.2} s", start.elapsed().as_secs_f64());
            println!("  Documents indexed: {}", sorted_documents.len());
            println!("  Chunks processed: {num_chunks}");
            println!(
                "  Storage size: {} ({storage_bytes} bytes)",
                human_bytes(storage_bytes)
            );
        }
    }

    pb.finish_with_message("✅ BlockMaxMaxScore index built");

    Ok((
        temp_dir,
        provider,
        posting_writer_id.expect("Should have created at least one writer"),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn search_with_block_maxscore(
    provider: &BlockfileProvider,
    posting_reader_id: Uuid,
    queries: &[SparseQuery],
    top_k: usize,
    mask: SignedRoaringBitmap,
    iterations: usize,
    show_progress: bool,
) -> anyhow::Result<Vec<SearchResult>> {
    if show_progress {
        println!("\n⚡ Searching with BlockMaxMaxScore...");
    }

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
            posting_reader_id,
            BLOCK_MAXSCORE_PREFIX.to_string(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open posting reader: {:?}", e))?;

    let reader = BlockSparseReader::new(posting_reader);
    let mut results = Vec::new();

    let pb = if show_progress {
        let pb = ProgressBar::new((queries.len() * iterations) as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message(if iterations > 1 {
            format!("MaxScore search ({iterations} iterations)")
        } else {
            "MaxScore search".to_string()
        });
        Some(pb)
    } else {
        None
    };

    for query in queries {
        let mut total_time_ms = 0.0;
        let mut last_scores = Vec::new();
        let mut last_offsets = Vec::new();

        for _ in 0..iterations {
            let start = Instant::now();

            let query_vec: Vec<(u32, f32)> = query
                .sparse_vector
                .indices()
                .iter()
                .zip(query.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val))
                .collect();

            let scores = reader
                .query(query_vec, top_k as u32, mask.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to run MaxScore search: {:?}", e))?;

            let elapsed = start.elapsed();
            total_time_ms += elapsed.as_secs_f64() * 1000.0;

            last_offsets = scores.iter().map(|s| s.offset).collect();
            last_scores = scores.iter().map(|s| s.score).collect();

            if let Some(ref pb) = pb {
                pb.inc(1);
            }
        }

        results.push(SearchResult {
            query_id: query.query_id.clone(),
            top_k_offsets: last_offsets,
            scores: last_scores,
            search_time_ms: total_time_ms / iterations as f64,
        });
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ MaxScore search complete");
    }
    Ok(results)
}

fn verify_and_compute_recall(
    documents: &[SparseDocument],
    queries: &[SparseQuery],
    reference: &[SearchResult],
    results: &[SearchResult],
) -> anyhow::Result<f64> {
    println!("\n🔍 Verifying results and computing recall...");

    if reference.is_empty() {
        return Ok(if results.is_empty() { 1.0 } else { 0.0 });
    }

    let score_tolerance = 1e-5;
    let mut total_recall = 0.0;
    let mut count = 0;
    let mut queries_with_issues = 0;
    let mut verification_passed = true;

    for ref_result in reference {
        let query = queries
            .iter()
            .find(|q| q.query_id == ref_result.query_id)
            .ok_or_else(|| anyhow::anyhow!("Query {} not found", ref_result.query_id))?;

        if let Some(wand_result) = results.iter().find(|r| r.query_id == ref_result.query_id) {
            let mut query_has_issues = false;

            // 1. Check for duplicate documents in WAND results
            let unique_offsets: HashSet<u32> = wand_result.top_k_offsets.iter().cloned().collect();
            if unique_offsets.len() != wand_result.top_k_offsets.len() {
                println!(
                    "\n  ⚠️ Query {}: WAND returned {} documents but only {} are unique",
                    query.query_id,
                    wand_result.top_k_offsets.len(),
                    unique_offsets.len()
                );
                query_has_issues = true;
                verification_passed = false;
            }

            // 2. Verify scores by recomputing with brute force
            let mut score_errors = 0;
            for (&offset, &wand_score) in wand_result
                .top_k_offsets
                .iter()
                .zip(wand_result.scores.iter())
            {
                let doc = &documents[offset as usize];
                let actual_score = query.sparse_vector.dot(&doc.sparse_vector);

                let score_diff = (actual_score - wand_score).abs();
                if score_diff > score_tolerance {
                    if score_errors == 0 {
                        println!(
                            "\n  ⚠️ Query {} has score verification errors:",
                            query.query_id
                        );
                    }
                    if score_errors < 5 {
                        println!("     Doc {offset}: WAND={wand_score:.6}, Actual={actual_score:.6}, Diff={score_diff:.2e}");
                    }
                    score_errors += 1;
                    query_has_issues = true;
                    verification_passed = false;
                }
            }
            if score_errors > 5 {
                println!("     ... and {} more score mismatches", score_errors - 5);
            }

            // 3. Check recall considering tie-breaking
            let reference_ids: HashSet<_> = ref_result.top_k_offsets.iter().collect();
            let results_ids: HashSet<_> = wand_result.top_k_offsets.iter().collect();
            let found_docs = reference_ids.intersection(&results_ids).count();

            // Get minimum scores for tie-breaking analysis
            let ref_min_score = ref_result.scores.last().copied().unwrap_or(0.0);
            let wand_min_score = wand_result.scores.last().copied().unwrap_or(0.0);
            let min_score_diff = (ref_min_score - wand_min_score).abs();

            // Check if missing documents are due to tie-breaking
            let missing_docs: Vec<_> = reference_ids.difference(&results_ids).cloned().collect();
            let extra_docs: Vec<_> = results_ids.difference(&reference_ids).cloned().collect();

            let mut missing_due_to_ties = true;
            if !missing_docs.is_empty() {
                // Check if all missing docs have scores equal to the minimum (tie-breaking)
                for &missing_offset in &missing_docs {
                    let doc = &documents[*missing_offset as usize];
                    let missing_score = query.sparse_vector.dot(&doc.sparse_vector);
                    if (missing_score - ref_min_score).abs() > score_tolerance {
                        missing_due_to_ties = false;
                        break;
                    }
                }

                // Also verify that extra docs have similar scores (tie-breaking)
                for &extra_offset in &extra_docs {
                    let doc = &documents[*extra_offset as usize];
                    let extra_score = query.sparse_vector.dot(&doc.sparse_vector);
                    if (extra_score - wand_min_score).abs() > score_tolerance {
                        missing_due_to_ties = false;
                        break;
                    }
                }
            }

            // Calculate recall
            let recall = if ref_result.top_k_offsets.is_empty() {
                1.0
            } else if missing_due_to_ties && min_score_diff < score_tolerance {
                // If differences are only due to tie-breaking, consider it 100% recall
                1.0
            } else {
                found_docs as f64 / ref_result.top_k_offsets.len() as f64
            };

            // Report issues if recall is not 100% (excluding tie-breaking)
            if recall < 1.0 {
                println!(
                    "\n  ⚠️ Query {} has recall {:.2}% (not due to tie-breaking)",
                    ref_result.query_id,
                    recall * 100.0
                );
                println!(
                    "     Reference: {} docs, min score: {:.6}",
                    ref_result.top_k_offsets.len(),
                    ref_min_score
                );
                println!(
                    "     WAND: {} docs, min score: {:.6}",
                    wand_result.top_k_offsets.len(),
                    wand_min_score
                );
                println!(
                    "     Missing {} docs: {:?}",
                    missing_docs.len(),
                    missing_docs.iter().take(10).collect::<Vec<_>>()
                );
                query_has_issues = true;
                verification_passed = false;
            } else if !missing_docs.is_empty() {
                // Recall is 100% but there were tie-breaking differences
                println!("\n  ℹ️ Query {}: 100% recall (with {} tie-breaking differences at score {:.6})",
                    query.query_id, missing_docs.len(), ref_min_score
                );
            }

            if query_has_issues {
                queries_with_issues += 1;
            }

            total_recall += recall;
            count += 1;
        }
    }

    let avg_recall = if count > 0 {
        total_recall / count as f64
    } else {
        0.0
    };

    // Summary
    if verification_passed {
        println!("\n  ✅ All verifications passed!");
        println!("     - No duplicate documents");
        println!("     - All scores match actual computation (tolerance: {score_tolerance:.2e})");
        println!(
            "     - Recall: {:.2}% (tie-breaking handled correctly)",
            avg_recall * 100.0
        );
    } else {
        println!("\n  ⚠️ Verification found issues in {queries_with_issues}/{count} queries");
        println!("     - Average recall: {:.2}%", avg_recall * 100.0);
    }

    Ok(avg_recall)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments using clap
    let args = Args::parse();

    println!("🚀 Sparse Index Block-Max WAND Benchmark");
    println!("{}", "=".repeat(60));
    println!("Configuration:");
    println!("  Dataset: Wikipedia SPLADE (from HuggingFace)");
    println!("  Queries: Downloaded from HuggingFace");
    println!("  Num documents: {}", args.num_documents);
    println!("  Num queries: {}", args.num_queries);

    // Load Wikipedia dataset
    println!("\n📥 Downloading Wikipedia dataset from HuggingFace...");
    let dataset = WikipediaSplade::init().await?;

    println!("📄 Loading documents...");
    let raw_documents: Vec<_> = dataset
        .documents()
        .await?
        .try_collect()
        .await?;

    let documents: Vec<_> = if args.num_documents <= raw_documents.len() {
        raw_documents[..args.num_documents].to_vec()
    } else {
        let base_len = raw_documents.len();
        let repeats = args.num_documents.div_ceil(base_len);
        println!(
            "  Dataset has {base_len} docs, recycling {repeats}x to reach {}",
            args.num_documents
        );
        let mut expanded = Vec::with_capacity(args.num_documents);
        for cycle in 0..repeats {
            let offset = (cycle * base_len) as u32;
            for doc in &raw_documents {
                if expanded.len() >= args.num_documents {
                    break;
                }
                let mut cloned = doc.clone();
                cloned.doc_id = format!("{}_{cycle}", cloned.doc_id);
                // Shift sparse vector indices are dimension IDs (stay the same),
                // but the doc offset in the index is expanded.len(), handled by
                // the benchmark loop, so we just need unique doc_ids.
                let _ = offset; // doc offset assigned by indexing loop
                expanded.push(cloned);
            }
        }
        expanded.truncate(args.num_documents);
        expanded
    };

    println!("✅ Loaded {} documents", documents.len());

    // Load queries from local parquet file
    println!("🔍 Loading queries from HuggingFace...");
    // Load queries from the dataset (uses already downloaded test split)
    let wiki_queries = dataset.queries().await?;

    let base_queries: Vec<_> = wiki_queries.into_iter().take(args.num_queries).collect();

    // Query term statistics
    let term_counts: Vec<usize> = base_queries.iter().map(|q| q.sparse_vector.nnz()).collect();
    let min_terms = *term_counts.iter().min().unwrap_or(&0);
    let max_terms_in_data = *term_counts.iter().max().unwrap_or(&0);
    let avg_terms = term_counts.iter().sum::<usize>() as f64 / term_counts.len().max(1) as f64;
    let median_terms = {
        let mut sorted = term_counts.clone();
        sorted.sort();
        sorted[sorted.len() / 2]
    };

    println!("✅ Loaded {} queries", base_queries.len());
    println!("  Query term stats: min={min_terms}, median={median_terms}, avg={avg_terms:.1}, max={max_terms_in_data}");

    let queries = if let Some(mt) = args.max_terms {
        println!("  Truncating queries to max {mt} terms");
        truncate_queries(&base_queries, mt)
    } else {
        base_queries.clone()
    };

    // Print rest of configuration
    println!("  Top-k: {}", args.top_k);
    println!("  Block size: {}", args.block_size);
    println!("  Sort by URL: {}", args.sort_by_url);
    if args.filter_percentage > 0 {
        println!(
            "  Filter: {}% of documents excluded",
            args.filter_percentage
        );
    }

    // Create mask based on filter percentage
    let mask = if args.filter_percentage > 0 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut excluded = roaring::RoaringBitmap::new();
        for i in 0..documents.len() as u32 {
            if rng.gen_range(0..100) < args.filter_percentage {
                excluded.insert(i);
            }
        }
        println!(
            "🎭 Filter mask created: {} documents excluded",
            excluded.len()
        );
        SignedRoaringBitmap::Exclude(excluded)
    } else {
        SignedRoaringBitmap::full()
    };

    // ── Sweep mode ──────────────────────────────────────────────────
    if args.sweep_terms {
        let term_limits = [5, 10, 15, 20, 25, 30, 35, 40];

        // Build both indices once
        let (wand_dir, wand_provider, max_id, ov_id) = Box::pin(build_sparse_index(
            &documents,
            args.block_size,
            args.sort_by_url,
            args.batch_size,
        ))
        .await?;

        let (ms_dir, ms_provider, ms_id) = Box::pin(build_block_maxscore_index(
            &documents,
            args.sort_by_url,
            args.batch_size,
        ))
        .await?;

        println!("\n📊 Sweep: query terms vs latency");
        println!("{:<12} {:<14} {:<14} {:<10}", "MaxTerms", "WAND (ms)", "MaxScore (ms)", "Speedup");
        println!("{}", "-".repeat(52));

        for &mt in &term_limits {
            let tq = truncate_queries(&base_queries, mt);
            let actual_avg_terms =
                tq.iter().map(|q| q.sparse_vector.nnz()).sum::<usize>() as f64 / tq.len() as f64;

            let wand_results = search_with_wand(
                &wand_provider,
                max_id,
                ov_id,
                &tq,
                args.top_k,
                mask.clone(),
                1,
                false,
            )
            .await?;
            let avg_wand =
                wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

            let ms_results = search_with_block_maxscore(
                &ms_provider,
                ms_id,
                &tq,
                args.top_k,
                mask.clone(),
                1,
                false,
            )
            .await?;
            let avg_ms =
                ms_results.iter().map(|r| r.search_time_ms).sum::<f64>() / ms_results.len() as f64;

            let speedup = avg_wand / avg_ms;
            println!(
                "{:<12} {:<14.2} {:<14.2} {:<10.2}x",
                format!("{mt} ({actual_avg_terms:.0})"),
                avg_wand,
                avg_ms,
                speedup,
            );
        }

        drop(wand_provider);
        drop(wand_dir);
        drop(ms_provider);
        drop(ms_dir);
        return Ok(());
    }

    let algo_name = if args.block_maxscore {
        "BlockMaxMaxScore"
    } else {
        "Block-Max WAND"
    };
    println!(
        "  Mode: {}",
        if args.wand_only {
            format!("{algo_name} only (profiling)")
        } else {
            "Full benchmark".to_string()
        }
    );
    println!("  Algorithm: {algo_name}");
    if args.iterations > 1 {
        println!("  Iterations per query: {}", args.iterations);
    }
    println!();

    if args.block_maxscore {
        // ── BlockMaxMaxScore path ───────────────────────────────────
        let (temp_dir, provider, posting_id) = Box::pin(build_block_maxscore_index(
            &documents,
            args.sort_by_url,
            args.batch_size,
        ))
        .await?;

        if args.wand_only {
            println!("\n🎯 Running MaxScore-only mode (no brute force comparison)");

            let start_total = Instant::now();
            let ms_results = search_with_block_maxscore(
                &provider,
                posting_id,
                &queries,
                args.top_k,
                mask,
                args.iterations,
                false,
            )
            .await?;
            let total_time = start_total.elapsed().as_secs_f64() * 1000.0;

            let avg_ms_time =
                ms_results.iter().map(|r| r.search_time_ms).sum::<f64>() / ms_results.len() as f64;
            let total_iterations = queries.len() * args.iterations;

            println!("\n📨 MAXSCORE-ONLY RESULTS");
            println!("{}", "=".repeat(60));
            println!("🎯 Performance:");
            println!("  Total time: {total_time:.2} ms");
            println!("  Total iterations: {total_iterations}");
            println!("  Avg time per query: {avg_ms_time:.2} ms");
            println!("  Queries per second: {:.2}", 1000.0 / avg_ms_time);
        } else {
            let (brute_force_results, brute_force_time, avg_non_trivial) =
                run_brute_force(&documents, &queries, args.top_k, &mask);

            let avg_percentage = (avg_non_trivial / documents.len() as f64) * 100.0;
            println!("Brute force total time: {brute_force_time:.2} ms");
            println!(
                "Average documents with non-zero similarity: {:.1}/{} ({:.1}%)",
                avg_non_trivial, documents.len(), avg_percentage
            );

            let ms_results = search_with_block_maxscore(
                &provider,
                posting_id,
                &queries,
                args.top_k,
                mask.clone(),
                args.iterations,
                true,
            )
            .await?;

            let avg_brute = brute_force_results
                .iter()
                .map(|r| r.search_time_ms)
                .sum::<f64>()
                / brute_force_results.len() as f64;
            let avg_ms =
                ms_results.iter().map(|r| r.search_time_ms).sum::<f64>() / ms_results.len() as f64;
            let speedup = avg_brute / avg_ms;

            let recall = verify_and_compute_recall(
                &documents,
                &queries,
                &brute_force_results,
                &ms_results,
            )?;

            println!("\n📨 BENCHMARK RESULTS (BlockMaxMaxScore)");
            println!("{}", "=".repeat(60));
            println!("🎯 Performance Comparison:");
            println!("  Method              Time (ms)    Speedup");
            println!("  {}", "-".repeat(42));
            println!("  Brute Force         {avg_brute:<12.2} 1.00x");
            println!("  BlockMaxMaxScore    {avg_ms:<12.2} {speedup:.2}x");
            println!();
            println!("🔍 Quality Metrics:");
            println!("  Recall@{}: {:.2}%", args.top_k, recall * 100.0);
            println!();
            println!("📊 Dataset Statistics:");
            println!("  Documents processed: {}", documents.len());
            println!("  Queries processed: {}", queries.len());
            println!("  Avg non-zero docs per query: {avg_non_trivial:.1} ({avg_percentage:.1}%)");
            println!("\n🎉 Benchmark completed successfully!");
        }

        drop(provider);
        drop(temp_dir);
    } else {
        // ── Original WAND path ──────────────────────────────────────
        let (temp_dir, provider, max_reader_id, offset_value_reader_id) =
            Box::pin(build_sparse_index(
                &documents,
                args.block_size,
                args.sort_by_url,
                args.batch_size,
            ))
            .await?;

        if args.wand_only {
            println!("\n🎯 Running WAND-only mode (no brute force comparison)");
            if args.iterations > 1 {
                println!(
                    "Running {} iterations per query for profiling...",
                    args.iterations
                );
            }

            let start_total = Instant::now();
            let wand_results = search_with_wand(
                &provider,
                max_reader_id,
                offset_value_reader_id,
                &queries,
                args.top_k,
                mask,
                args.iterations,
                false,
            )
            .await?;
            let total_time = start_total.elapsed().as_secs_f64() * 1000.0;

            let avg_wand_time = wand_results.iter().map(|r| r.search_time_ms).sum::<f64>()
                / wand_results.len() as f64;
            let total_iterations = queries.len() * args.iterations;

            println!("\n📨 WAND-ONLY RESULTS");
            println!("{}", "=".repeat(60));
            println!("🎯 Performance:");
            println!("  Total time: {total_time:.2} ms");
            println!("  Total iterations: {total_iterations}");
            println!("  Avg time per query: {avg_wand_time:.2} ms");
            println!("  Queries per second: {:.2}", 1000.0 / avg_wand_time);
        } else {
            let (brute_force_results, brute_force_time, avg_non_trivial) =
                run_brute_force(&documents, &queries, args.top_k, &mask);

            let avg_percentage = (avg_non_trivial / documents.len() as f64) * 100.0;
            println!("Brute force total time: {brute_force_time:.2} ms");
            println!(
                "Average documents with non-zero similarity: {:.1}/{} ({:.1}%)",
                avg_non_trivial, documents.len(), avg_percentage
            );

            let wand_results = search_with_wand(
                &provider,
                max_reader_id,
                offset_value_reader_id,
                &queries,
                args.top_k,
                mask.clone(),
                args.iterations,
                true,
            )
            .await?;

            let recall = verify_and_compute_recall(
                &documents,
                &queries,
                &brute_force_results,
                &wand_results,
            )?;

            let avg_brute = brute_force_results
                .iter()
                .map(|r| r.search_time_ms)
                .sum::<f64>()
                / brute_force_results.len() as f64;
            let avg_wand = wand_results.iter().map(|r| r.search_time_ms).sum::<f64>()
                / wand_results.len() as f64;
            let speedup = avg_brute / avg_wand;

            println!("\n📨 BENCHMARK RESULTS");
            println!("{}", "=".repeat(60));
            println!("🎯 Performance Comparison:");
            println!("  Method              Time (ms)    Speedup");
            println!("  {}", "-".repeat(42));
            println!("  Brute Force         {avg_brute:<12.2} 1.00x");
            println!("  Block-Max WAND      {avg_wand:<12.2} {speedup:.2}x");
            println!();
            println!("🔍 Quality Metrics:");
            println!("  Recall@{}: {:.2}%", args.top_k, recall * 100.0);
            println!();
            println!("📊 Dataset Statistics:");
            println!("  Documents processed: {}", documents.len());
            println!("  Queries processed: {}", queries.len());
            println!(
                "  Avg non-zero docs per query: {avg_non_trivial:.1} ({avg_percentage:.1}%)"
            );
            println!("\n🎉 Benchmark completed successfully!");
        }

        drop(provider);
        drop(temp_dir);
    }

    Ok(())
}

fn run_brute_force(
    documents: &[SparseDocument],
    queries: &[SparseQuery],
    top_k: usize,
    mask: &SignedRoaringBitmap,
) -> (Vec<SearchResult>, f64, f64) {
    println!("\n🐌 Running brute force search (ground truth)...");
    let pb_brute = ProgressBar::new(queries.len() as u64);
    pb_brute.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb_brute.set_message("Brute force search");

    let start = Instant::now();
    let mut results = Vec::new();
    let mut total_non_trivial = 0;

    for (i, query) in queries.iter().enumerate() {
        let (result, non_trivial_count) = brute_force_search(documents, query, top_k, mask);
        total_non_trivial += non_trivial_count;
        results.push(result);
        pb_brute.set_position((i + 1) as u64);
    }

    pb_brute.finish_with_message("✅ Brute force complete");
    let brute_force_time = start.elapsed().as_secs_f64() * 1000.0;
    let avg_non_trivial = total_non_trivial as f64 / queries.len() as f64;

    (results, brute_force_time, avg_non_trivial)
}
