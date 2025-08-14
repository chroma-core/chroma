//! Sparse Index Block-Max WAND Benchmark
//!
//! This benchmark evaluates the performance of the Block-Max WAND algorithm
//! for sparse vector search compared to brute force baseline.
//!
//! ## Usage Modes
//!
//! ### Full Benchmark Mode (default)
//! Compares WAND performance against brute force ground truth:
//! ```bash
//! cargo run --release --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
//!   -n 65536 \  # number of documents
//!   -m 200 \    # number of queries
//!   -k 128      # top-k results
//! ```
//!
//! ### WAND-Only Mode (for profiling)
//! Runs only WAND without brute force comparison, useful for flamegraph profiling:
//! ```bash
//! cargo run --release --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
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
//! cargo flamegraph --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
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

use anyhow::Result;
use arrow::array::{Array, Float32Array, Int32Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::{
    reader::{Score, SparseReader},
    writer::{SparseDelta, SparseWriter},
};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sprs::CsVec;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::time::Instant;
use tempfile::TempDir;
use uuid::Uuid;

// Blockfile prefix constants
const SPARSE_MAX_PREFIX: &str = "sparse_max";
const SPARSE_OFFSET_VALUE_PREFIX: &str = "sparse_offset_value";

// Sparse document and query structures using CsVec from sprs
#[derive(Debug, Clone)]
pub struct SparseDocument {
    pub doc_id: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub sparse_vector: CsVec<f32>,
}

#[derive(Debug, Clone)]
pub struct SparseQuery {
    pub query_id: String,
    pub text: String,
    pub sparse_vector: CsVec<f32>,
}

/// Load documents with sparse vectors from parquet file
pub fn load_sparse_documents(
    path: String,
    offset: usize,
    limit: usize,
) -> Result<Vec<SparseDocument>> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut documents = Vec::new();
    let mut current_offset = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let batch_size = batch.num_rows();

        // Skip if we haven't reached the offset yet
        if current_offset + batch_size <= offset {
            current_offset += batch_size;
            continue;
        }

        // Process the batch
        let start = offset.saturating_sub(current_offset);
        let end = std::cmp::min(batch_size, start + (limit - documents.len()));

        if start < end {
            let sliced_batch = batch.slice(start, end - start);
            documents.extend(process_sparse_document_batch(sliced_batch)?);
        }

        current_offset += batch_size;

        // Stop if we've collected enough documents
        if documents.len() >= limit {
            break;
        }
    }

    Ok(documents)
}

/// Load queries with sparse vectors from parquet file
pub fn load_sparse_queries(path: String, offset: usize, limit: usize) -> Result<Vec<SparseQuery>> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut queries = Vec::new();
    let mut current_offset = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let batch_size = batch.num_rows();

        // Skip if we haven't reached the offset yet
        if current_offset + batch_size <= offset {
            current_offset += batch_size;
            continue;
        }

        // Process the batch
        let start = offset.saturating_sub(current_offset);
        let end = std::cmp::min(batch_size, start + (limit - queries.len()));

        if start < end {
            let sliced_batch = batch.slice(start, end - start);
            queries.extend(process_sparse_query_batch(sliced_batch)?);
        }

        current_offset += batch_size;

        // Stop if we've collected enough queries
        if queries.len() >= limit {
            break;
        }
    }

    Ok(queries)
}

/// Process a document batch and extract sparse vectors
fn process_sparse_document_batch(batch: RecordBatch) -> Result<Vec<SparseDocument>> {
    let doc_ids = batch
        .column_by_name("doc_id")
        .ok_or_else(|| anyhow::anyhow!("doc_id column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("doc_id is not a string array"))?;

    let urls = batch
        .column_by_name("url")
        .ok_or_else(|| anyhow::anyhow!("url column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("url is not a string array"))?;

    let titles = batch
        .column_by_name("title")
        .ok_or_else(|| anyhow::anyhow!("title column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("title is not a string array"))?;

    let bodies = batch
        .column_by_name("body")
        .ok_or_else(|| anyhow::anyhow!("body column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("body is not a string array"))?;

    let sparse_indices = batch
        .column_by_name("sparse_indices")
        .ok_or_else(|| anyhow::anyhow!("sparse_indices column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_indices is not a list array"))?;

    let sparse_values = batch
        .column_by_name("sparse_values")
        .ok_or_else(|| anyhow::anyhow!("sparse_values column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_values is not a list array"))?;

    let mut documents = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let doc_id = doc_ids.value(i).to_string();
        let url = urls.value(i).to_string();
        let title = titles.value(i).to_string();
        let body = bodies.value(i).to_string();

        // Get indices and values for this document
        let indices_array = sparse_indices.value(i);
        let values_array = sparse_values.value(i);

        let indices = indices_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("indices is not Int32Array"))?;

        let values = values_array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("values is not Float32Array"))?;

        // Create sparse vector as CsVec
        let mut sparse_indices = Vec::new();
        let mut sparse_values = Vec::new();
        for j in 0..indices.len() {
            sparse_indices.push(indices.value(j) as usize);
            sparse_values.push(values.value(j));
        }
        let sparse_vector = CsVec::new(usize::MAX, sparse_indices, sparse_values);

        documents.push(SparseDocument {
            doc_id,
            url,
            title,
            body,
            sparse_vector,
        });
    }

    Ok(documents)
}

/// Process a query batch and extract sparse vectors
fn process_sparse_query_batch(batch: RecordBatch) -> Result<Vec<SparseQuery>> {
    let query_ids = batch
        .column_by_name("query_id")
        .ok_or_else(|| anyhow::anyhow!("query_id column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("query_id is not a string array"))?;

    let texts = batch
        .column_by_name("text")
        .ok_or_else(|| anyhow::anyhow!("text column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("text is not a string array"))?;

    let sparse_indices = batch
        .column_by_name("sparse_indices")
        .ok_or_else(|| anyhow::anyhow!("sparse_indices column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_indices is not a list array"))?;

    let sparse_values = batch
        .column_by_name("sparse_values")
        .ok_or_else(|| anyhow::anyhow!("sparse_values column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_values is not a list array"))?;

    let mut queries = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let query_id = query_ids.value(i).to_string();
        let text = texts.value(i).to_string();

        // Get indices and values for this query
        let indices_array = sparse_indices.value(i);
        let values_array = sparse_values.value(i);

        let indices = indices_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("indices is not Int32Array"))?;

        let values = values_array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("values is not Float32Array"))?;

        // Create sparse vector as CsVec
        let mut sparse_indices = Vec::new();
        let mut sparse_values = Vec::new();
        for j in 0..indices.len() {
            sparse_indices.push(indices.value(j) as usize);
            sparse_values.push(values.value(j));
        }
        let sparse_vector = CsVec::new(usize::MAX, sparse_indices, sparse_values);

        queries.push(SparseQuery {
            query_id,
            text,
            sparse_vector,
        });
    }

    Ok(queries)
}

/// Command line arguments for the benchmark
#[derive(Parser, Debug)]
#[command(name = "sparse_wand_benchmark")]
#[command(about = "Benchmark sparse index with Block-Max WAND algorithm")]
struct Args {
    /// Path to the dataset directory containing documents.parquet and queries.parquet
    #[arg(short = 'd', long)]
    dataset_path: String,

    /// Number of documents to load
    #[arg(short = 'n', long, default_value_t = 65536)]
    num_documents: usize,

    /// Number of queries to run
    #[arg(short = 'm', long, default_value_t = 200)]
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
    #[arg(long)]
    wand_only: bool,

    /// Number of iterations to run each query (for profiling)
    #[arg(short = 'i', long, default_value_t = 1)]
    iterations: usize,
}

#[derive(Debug, Clone)]
struct SearchResult {
    query_id: String,
    top_k_offsets: Vec<u32>,
    scores: Vec<f32>,
    search_time_ms: f64,
    full_evaluations: u32,
}

fn brute_force_search(
    documents: &[SparseDocument],
    query: &SparseQuery,
    top_k: usize,
) -> (SearchResult, usize) {
    let start = Instant::now();

    // Use a min-heap to maintain top-k results efficiently (same as WAND implementation)
    let mut top_scores = BinaryHeap::<Score>::with_capacity(top_k);
    let mut non_trivial_count = 0;

    for (offset, doc) in documents.iter().enumerate() {
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
        full_evaluations: documents.len() as u32,
    };

    (result, non_trivial_count)
}

async fn build_sparse_index(
    documents: &[SparseDocument],
    block_size: u32,
    sort_by_url: bool,
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

    // Process documents in batches with write-commit-flush loop
    let batch_size = 65536;
    let num_chunks = (sorted_documents.len() + batch_size - 1) / batch_size;

    let pb = ProgressBar::new(num_chunks as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} chunks ({eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb.set_message("Building index chunks");

    let mut max_writer_id = None;
    let mut offset_value_writer_id = None;

    for (chunk_idx, chunk) in sorted_documents.chunks(batch_size).enumerate() {
        // Create writer options, forking if not the first chunk
        let mut max_writer_options = BlockfileWriterOptions::new(SPARSE_MAX_PREFIX.to_string());
        let mut offset_value_writer_options =
            BlockfileWriterOptions::new(SPARSE_OFFSET_VALUE_PREFIX.to_string());

        if let Some(id) = max_writer_id {
            max_writer_options = max_writer_options.fork(id);
        }
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

        // Store the writer IDs for forking in next iteration
        max_writer_id = Some(max_writer.id());
        offset_value_writer_id = Some(offset_value_writer.id());

        // Create sparse writer for this chunk
        let sparse_writer = SparseWriter::new(block_size, max_writer, offset_value_writer, None);

        // Build delta for this chunk
        let mut delta = SparseDelta::default();
        for (idx, doc) in chunk.iter().enumerate() {
            let offset = (chunk_idx * batch_size + idx) as u32;

            // Convert CsVec to iterator of (dimension_id, value)
            let sparse_iter = doc
                .sparse_vector
                .indices()
                .iter()
                .zip(doc.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val));
            delta.create(offset, sparse_iter);
        }

        // Write the batch
        sparse_writer.write(delta);

        // Commit
        let flusher = sparse_writer
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit sparse writer: {:?}", e))?;

        // Flush
        flusher
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to flush sparse writer: {:?}", e))?;

        // Clear cache after each chunk
        provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear cache: {:?}", e))?;

        pb.inc(1);
    }

    pb.finish_with_message("✅ Index built");

    let elapsed = start.elapsed();
    println!("⏱️ Index build time: {:.2} s", elapsed.as_secs_f64());
    println!("  Documents indexed: {}", sorted_documents.len());
    println!("  Chunks processed: {}", num_chunks);
    println!("  Documents per chunk: {}", batch_size);

    Ok((
        temp_dir,
        provider,
        max_writer_id.expect("Should have created at least one max writer"),
        offset_value_writer_id.expect("Should have created at least one offset value writer"),
    ))
}

async fn search_with_wand(
    provider: &BlockfileProvider,
    max_reader_id: Uuid,
    offset_value_reader_id: Uuid,
    queries: &[SparseQuery],
    top_k: usize,
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
            format!("WAND search ({} iterations)", iterations)
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
        let mut last_full_evaluations = 0;

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

            // Run WAND search
            let (scores, full_evaluations) = sparse_reader
                .wand(query_vec, top_k as u32)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to run WAND search: {:?}", e))?;

            let elapsed = start.elapsed();
            total_time_ms += elapsed.as_secs_f64() * 1000.0;

            // Store results from last iteration
            last_offsets = scores.iter().map(|s| s.offset).collect();
            last_scores = scores.iter().map(|s| s.score).collect();
            last_full_evaluations = full_evaluations;

            if let Some(ref pb) = pb {
                pb.inc(1);
            }
        }

        results.push(SearchResult {
            query_id: query.query_id.clone(),
            top_k_offsets: last_offsets,
            scores: last_scores,
            search_time_ms: total_time_ms / iterations as f64, // Average time per query
            full_evaluations: last_full_evaluations,
        });
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ WAND search complete");
    }
    Ok(results)
}

fn compute_accuracy(reference: &[SearchResult], results: &[SearchResult]) -> f64 {
    if reference.is_empty() {
        return if results.is_empty() { 1.0 } else { 0.0 };
    }

    let mut total_accuracy = 0.0;
    let mut count = 0;

    for ref_result in reference {
        if let Some(result) = results.iter().find(|r| r.query_id == ref_result.query_id) {
            // For accuracy, we count how many documents in the WAND results
            // are also in the reference results (top-k from brute force)
            let reference_ids: HashSet<_> = ref_result.top_k_offsets.iter().collect();
            let results_ids: HashSet<_> = result.top_k_offsets.iter().collect();

            // Count exact matches (documents that appear in both result sets)
            let exact_matches = reference_ids.intersection(&results_ids).count();

            // Accuracy is the proportion of WAND results that are in the true top-k
            let accuracy = if result.top_k_offsets.is_empty() {
                0.0
            } else {
                exact_matches as f64 / result.top_k_offsets.len() as f64
            };

            total_accuracy += accuracy;
            count += 1;
        }
    }

    if count > 0 {
        total_accuracy / count as f64
    } else {
        0.0
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments using clap
    let args = Args::parse();

    // Construct paths to documents and queries files
    let documents_path = format!("{}/documents.parquet", args.dataset_path);
    let queries_path = format!("{}/queries.parquet", args.dataset_path);

    println!("🚀 Sparse Index Block-Max WAND Benchmark");
    println!("{}", "=".repeat(60));
    println!("Configuration:");
    println!("  Dataset: {}", args.dataset_path);
    println!("  Documents: {}", documents_path);
    println!("  Queries: {}", queries_path);
    println!("  Num documents: {}", args.num_documents);
    println!("  Num queries: {}", args.num_queries);
    println!("  Top-k: {}", args.top_k);
    println!("  Block size: {}", args.block_size);
    println!("  Sort by URL: {}", args.sort_by_url);
    println!(
        "  Mode: {}",
        if args.wand_only {
            "WAND only (profiling)"
        } else {
            "Full benchmark"
        }
    );
    if args.iterations > 1 {
        println!("  Iterations per query: {}", args.iterations);
    }
    println!();

    // Load documents
    println!("📄 Loading documents...");
    let documents = load_sparse_documents(documents_path, 0, args.num_documents)?;
    println!("✅ Loaded {} documents", documents.len());

    // Load queries
    println!("🔍 Loading queries...");
    let queries = load_sparse_queries(queries_path, 0, args.num_queries)?;
    println!("✅ Loaded {} queries", queries.len());

    // Build sparse index
    let (temp_dir, provider, max_reader_id, offset_value_reader_id) =
        build_sparse_index(&documents, args.block_size, args.sort_by_url).await?;

    if args.wand_only {
        // WAND-only mode for profiling
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
            args.iterations,
            false, // no progress bar in wand-only mode
        )
        .await?;
        let total_time = start_total.elapsed().as_secs_f64() * 1000.0;

        // Calculate metrics
        let avg_wand_time =
            wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

        let avg_wand_evaluations = wand_results
            .iter()
            .map(|r| r.full_evaluations as f64)
            .sum::<f64>()
            / wand_results.len() as f64;

        let evaluation_percentage = (avg_wand_evaluations / documents.len() as f64) * 100.0;

        let total_iterations = queries.len() * args.iterations;

        // Print results
        println!("\n📨 WAND-ONLY RESULTS");
        println!("{}", "=".repeat(60));
        println!("🎯 Performance:");
        println!("  Total time: {:.2} ms", total_time);
        println!("  Total iterations: {}", total_iterations);
        println!("  Avg time per query: {:.2} ms", avg_wand_time);
        println!("  Queries per second: {:.2}", 1000.0 / avg_wand_time);
        println!(
            "  Avg full evaluations: {:.1}/{} ({:.1}%)",
            avg_wand_evaluations,
            documents.len(),
            evaluation_percentage
        );
        println!();
        println!("📊 Statistics:");
        println!("  Documents indexed: {}", documents.len());
        println!("  Queries processed: {}", queries.len());
        println!("  Iterations per query: {}", args.iterations);

        println!("\n🔥 Ready for flamegraph profiling!");
        println!("Tip: Use with cargo flamegraph --freq 99 for lower overhead");
    } else {
        // Full benchmark mode with brute force comparison
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
        let mut brute_force_results = Vec::new();
        let mut total_non_trivial = 0;

        for (i, query) in queries.iter().enumerate() {
            let (result, non_trivial_count) = brute_force_search(&documents, query, args.top_k);
            total_non_trivial += non_trivial_count;
            brute_force_results.push(result);
            pb_brute.set_position((i + 1) as u64);
        }

        pb_brute.finish_with_message("✅ Brute force complete");
        let brute_force_time = start.elapsed().as_secs_f64() * 1000.0;

        // Calculate non-trivial document statistics
        let avg_non_trivial = total_non_trivial as f64 / queries.len() as f64;
        let avg_percentage = (avg_non_trivial / documents.len() as f64) * 100.0;

        println!("Brute force total time: {:.2} ms", brute_force_time);
        println!(
            "Average documents with non-zero similarity: {:.1}/{} ({:.1}%)",
            avg_non_trivial,
            documents.len(),
            avg_percentage
        );

        // Run WAND search
        let wand_results = search_with_wand(
            &provider,
            max_reader_id,
            offset_value_reader_id,
            &queries,
            args.top_k,
            args.iterations,
            true, // show progress in full benchmark mode
        )
        .await?;

        // Show example query and retrieved documents
        println!("\n📝 Example Query and Retrieved Documents");
        println!("{}", "=".repeat(60));
        if !queries.is_empty() && !wand_results.is_empty() {
            let example_query = &queries[0];
            let example_result = &wand_results[0];

            println!("Query: {}", example_query.text);
            println!("Query ID: {}", example_query.query_id);
            println!(
                "\nTop {} Retrieved Documents:",
                example_result.top_k_offsets.len().min(5)
            );
            println!("{}", "-".repeat(60));

            for (i, (&offset, &score)) in example_result
                .top_k_offsets
                .iter()
                .zip(example_result.scores.iter())
                .take(5)
                .enumerate()
            {
                let doc = &documents[offset as usize];
                println!("\n{}. Score: {:.4}", i + 1, score);
                println!("   URL: {}", doc.url);
                println!(
                    "   Title: {}",
                    if doc.title.len() > 80 {
                        format!("{}...", &doc.title[..77])
                    } else {
                        doc.title.clone()
                    }
                );
                println!(
                    "   Body: {}",
                    if doc.body.len() > 150 {
                        format!("{}...", &doc.body[..147])
                    } else {
                        doc.body.clone()
                    }
                );
            }
        }

        // Calculate metrics
        let accuracy = compute_accuracy(&brute_force_results, &wand_results);

        let avg_brute_force_time = brute_force_results
            .iter()
            .map(|r| r.search_time_ms)
            .sum::<f64>()
            / brute_force_results.len() as f64;

        let avg_wand_time =
            wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

        let avg_wand_evaluations = wand_results
            .iter()
            .map(|r| r.full_evaluations as f64)
            .sum::<f64>()
            / wand_results.len() as f64;

        let speedup = avg_brute_force_time / avg_wand_time;
        let evaluation_reduction = (1.0 - (avg_wand_evaluations / documents.len() as f64)) * 100.0;

        // Print results
        println!("\n📨 BENCHMARK RESULTS");
        println!("{}", "=".repeat(60));
        println!("🎯 Performance Comparison:");
        println!("  Method              Time (ms)    Evaluations    Speedup");
        println!("  {}", "-".repeat(58));
        println!(
            "  Brute Force         {:<12.2} {:<14} 1.00x",
            avg_brute_force_time,
            documents.len()
        );
        println!(
            "  Block-Max WAND      {:<12.2} {:<14.1} {:.2}x",
            avg_wand_time, avg_wand_evaluations, speedup
        );
        if args.iterations > 1 {
            println!("  (WAND averaged over {} iterations)", args.iterations);
        }
        println!();
        println!("🔍 Quality Metrics:");
        println!("  Accuracy@{}: {:.2}%", args.top_k, accuracy * 100.0);
        println!("  Evaluation reduction: {:.1}%", evaluation_reduction);
        println!();
        println!("📊 Dataset Statistics:");
        println!("  Documents processed: {}", documents.len());
        println!("  Queries processed: {}", queries.len());
        println!(
            "  Avg non-zero docs per query: {:.1} ({:.1}%)",
            avg_non_trivial, avg_percentage
        );

        println!("\n🎉 Benchmark completed successfully!");
        println!("Total queries processed: {}", queries.len());
        if args.sort_by_url {
            println!("Documents were sorted by URL for better cache locality");
        }
    }

    // Clean up
    drop(provider);
    drop(temp_dir);

    Ok(())
}
